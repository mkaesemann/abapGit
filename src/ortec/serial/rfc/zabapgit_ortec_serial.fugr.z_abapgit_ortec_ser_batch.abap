FUNCTION z_abapgit_ortec_ser_batch.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(IV_BATCH_ID) TYPE  CHAR32
*"     VALUE(IV_ATTEMPT) TYPE  I DEFAULT 1
*"     VALUE(IV_ABAP_LANGUAGE_VERS) TYPE  UCCHECK
*"     VALUE(IV_LANGUAGE) TYPE  SY-LANGU
*"     VALUE(IV_PATH) TYPE  STRING
*"     VALUE(IV_MAIN_LANGUAGE_ONLY) TYPE  CHAR1
*"     VALUE(IV_SUPPRESS_PO_COMMENTS) TYPE  CHAR1
*"     VALUE(IV_USE_LXE) TYPE  CHAR1
*"     VALUE(IT_TRANSLATION_LANGS) TYPE  TFPLAISO
*"     VALUE(IT_TADIR) TYPE  ZAOG_SER_TADIR_TT
*"     VALUE(IV_PREFETCH_BUFFER) TYPE  XSTRING OPTIONAL
*"     VALUE(IV_PREFETCH_BUFFER_EXT) TYPE  XSTRING OPTIONAL
*"     VALUE(IV_PREFETCH_BUFFER_OO) TYPE  XSTRING OPTIONAL
*"     VALUE(IV_PREFETCH_BUFFER_DD) TYPE  XSTRING OPTIONAL
*"     VALUE(IV_INPUT_ROW_COUNT) TYPE  I
*"     VALUE(IV_INPUT_VERSION) TYPE  I DEFAULT 1
*"  EXPORTING
*"     VALUE(ET_RESULT) TYPE  ZAOG_SER_BATCH_RESULT_TT
*"     VALUE(EV_OUTPUT_ROW_COUNT) TYPE  I
*"  EXCEPTIONS
*"      ERROR
*"----------------------------------------------------------------------
* SER-SLICE-2 Phase 2: worker body per
* serialization_adaptive_batch_design.md &sect;2 (decision-free
* pseudocode), mirroring the exact prefetch-injection/serialize() pattern
* of the existing single-object Z_ABAPGIT_SERIALIZE_PARALLEL
* (LZABAPGIT_PARALLELU02). One object's exception is caught INSIDE the
* loop, never aborting the batch - this is the structural guarantee for
* partial-success preservation.
*
* IV_PATH is accepted for signature symmetry with the single-object
* worker but is intentionally NOT used here: unlike that worker (one
* object, one path per call), this batch worker's IT_TADIR can carry
* several different paths in one call. Per-object path assignment is
* done by the caller (ZCL_ABAPGIT_ORTEC_SER_ORCH) during result merge,
* from its own already-known TADIR data - exactly mirroring how the
* standard path's ADD_TO_RETURN assigns EV_PATH after the fact.

  DATA: ls_result       TYPE zaog_ser_batch_result,
        ls_item         TYPE zif_abapgit_definitions=>ty_item,
        ls_i18n_params  TYPE zif_abapgit_definitions=>ty_i18n_params,
        ls_serialization TYPE zif_abapgit_objects=>ty_serialization,
        lx_error        TYPE REF TO zcx_abapgit_exception,
        lv_t0           TYPE i,
        lv_t1           TYPE i.

  IF iv_prefetch_buffer IS NOT INITIAL.
    zcl_abapgit_ortec_ser_pref=>inject_from_buffer( iv_prefetch_buffer ).
  ENDIF.
  IF iv_prefetch_buffer_ext IS NOT INITIAL.
    zcl_abapgit_ortec_ser_pref_ext=>inject_from_buffer( iv_prefetch_buffer_ext ).
  ENDIF.
  IF iv_prefetch_buffer_oo IS NOT INITIAL.
    zcl_abapgit_ortec_ser_pref_oo=>inject_from_buffer( iv_prefetch_buffer_oo ).
  ENDIF.
  " SER-SLICE-3 parity incident fix (serialization_slice_3_dtel_doma_
  " parity.md, AR-3-001): unconditional clear FIRST, on every worker
  " invocation - a pooled/reused session must never keep a PRIOR
  " dispatch's DOMA/DTEL cache when THIS dispatch's own buffer is
  " legitimately empty (e.g. no DOMA/DTEL objects in this batch at all).
  zcl_abapgit_ortec_ser_pref_ext=>clear_dd_cache( ).
  IF iv_prefetch_buffer_dd IS NOT INITIAL.
    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer( iv_prefetch_buffer_dd ).
      CATCH zcx_abapgit_exception ##NO_HANDLER.
        " Corrupt/unknown-version DOMA/DTEL batch buffer (SER-SLICE-3
        " required behavior): treat as a full prefetch MISS for this
        " buffer only - every object below still serializes via its own
        " standard per-object read, exactly like a normal prefetch miss.
        " Never propagate - the batch itself must still complete.
    ENDTRY.
  ENDIF.

  ls_i18n_params-main_language         = iv_language.
  ls_i18n_params-main_language_only    = iv_main_language_only.
  ls_i18n_params-suppress_po_comments  = iv_suppress_po_comments.
  ls_i18n_params-use_lxe               = iv_use_lxe.
  ls_i18n_params-translation_languages = it_translation_langs.

  LOOP AT it_tadir INTO DATA(ls_tadir).
    CLEAR ls_result.
    ls_result-obj_type = ls_tadir-object.
    ls_result-obj_name = ls_tadir-obj_name.

    GET RUN TIME FIELD lv_t0.

    TRY.
        CLEAR ls_item.
        ls_item-obj_type              = ls_tadir-object.
        ls_item-obj_name              = ls_tadir-obj_name.
        ls_item-devclass              = ls_tadir-devclass.
        ls_item-srcsystem             = ls_tadir-srcsystem.
        ls_item-origlang              = ls_tadir-masterlang.
        ls_item-abap_language_version = iv_abap_language_vers.

        " SER-SLICE-3 (H4 provider contract): report which case actually
        " applied for THIS object. DOMA/DTEL are the only types with a
        " batch-scoped prefetch provider today - every other type always
        " falls back to its own per-object read, with no provider consulted.
        CASE ls_tadir-object.
          WHEN 'DTEL'.
            IF zcl_abapgit_ortec_ser_pref_ext=>get_dtel_data(
                 iv_rollname = CONV #( ls_tadir-obj_name )
                 iv_language = iv_language ) = abap_true.
              ls_result-provider_hit = 1.
            ELSE.
              ls_result-provider_miss = 1.
            ENDIF.
          WHEN 'DOMA'.
            IF zcl_abapgit_ortec_ser_pref_ext=>get_doma_data(
                 iv_domname  = CONV #( ls_tadir-obj_name )
                 iv_language = iv_language ) = abap_true.
              ls_result-provider_hit = 1.
            ELSE.
              ls_result-provider_miss = 1.
            ENDIF.
          WHEN OTHERS.
            ls_result-provider_fallback = 1.
        ENDCASE.

        ls_serialization = zcl_abapgit_objects=>serialize(
          is_item        = ls_item
          io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = ls_i18n_params ) ).

        EXPORT data = ls_serialization TO DATA BUFFER ls_result-files_xstring.

        ls_result-rc                = 0.
        ls_result-output_bytes      = xstrlen( ls_result-files_xstring ).
        ls_result-output_file_count = lines( ls_serialization-files ).

      CATCH zcx_abapgit_exception INTO lx_error.
        ls_result-rc    = 4.
        ls_result-msgid = lx_error->if_t100_message~t100key-msgid.
        ls_result-msgno = lx_error->if_t100_message~t100key-msgno.
        ls_result-msgv1 = lx_error->msgv1.
        ls_result-msgv2 = lx_error->msgv2.
        ls_result-msgv3 = lx_error->msgv3.
        ls_result-msgv4 = lx_error->msgv4.
    ENDTRY.

    GET RUN TIME FIELD lv_t1.
    ls_result-elapsed_ms = ( lv_t1 - lv_t0 ) / 1000.

    APPEND ls_result TO et_result.
  ENDLOOP.

  ev_output_row_count = lines( et_result ).


ENDFUNCTION.
