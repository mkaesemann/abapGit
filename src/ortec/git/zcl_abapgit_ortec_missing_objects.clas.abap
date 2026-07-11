"! <p class="shorttext synchronized">ORTEC Bulk Missing-Object Collector</p>
"! Ensures a set of objects is present in the persistent object store, performing
"! at most one targeted negotiated remote fetch if some are missing, then retrying
"! the local check once. Never performs a network call for a repository that has
"! not opted into the ORTEC write/protocol behavior, so a read-only caller (e.g.
"! filtered Stage/Diff resolution) never pays a surprise full-fetch cost.
CLASS zcl_abapgit_ortec_missing_objects DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    "! Ensure the given objects are present in the persistent object store.
    "! Flow: local bulk check -> (if any missing) one negotiated remote fetch of
    "! iv_commit -> persist -> local bulk check retried once.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_url |
    "! Remote URL. Pass initial to skip fetching entirely (local check only).
    "! @parameter iv_commit |
    "! Commit whose tree must be fully resolvable
    "! @parameter it_sha1s |
    "! Object SHA1s the caller needs to be present in the store
    "! @raising zcx_abapgit_ortec_git |
    "! Raised if objects are still missing after the fetch+retry, if no remote
    "! fetch is possible (blank URL or write/protocol opt-in inactive), or if the
    "! negotiated fetch itself fails. Callers should treat this exactly like "no
    "! fast-path benefit available" and fall back to their existing safe path.
    CLASS-METHODS ensure_available
      IMPORTING
        iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_url      TYPE string
        iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
        it_sha1s    TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING
        zcx_abapgit_ortec_git.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.


CLASS zcl_abapgit_ortec_missing_objects IMPLEMENTATION.

  METHOD ensure_available.

    DATA lt_missing      TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects      TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_fetched_head TYPE zif_abapgit_git_definitions=>ty_sha1.

    " Step 1: local bulk resolve - one set-based DB lookup, no per-object reads.
    lt_missing = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
      iv_repo_key = iv_repo_key
      it_sha1s    = it_sha1s ).

    IF lt_missing IS INITIAL.
      RETURN. " Already fully buffered - nothing to do.
    ENDIF.

    " Never trigger a network fetch for a repo that has not opted into the ORTEC
    " write/protocol behavior. Without that opt-in, the transport layer has no
    " incremental have/want negotiation available and a fetch here could turn a
    " read-only Stage/Diff resolution into a full, non-negotiated pull.
    IF iv_url IS INITIAL OR zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_false.
      zcx_abapgit_ortec_git=>raise(
        |{ lines( lt_missing ) } object(s) not buffered and remote fetch is not available| ).
    ENDIF.

    " Step 2: ONE negotiated remote fetch of the target commit. When the
    " write/protocol opt-in is active this routes through the ORTEC fastpath
    " transport, which negotiates have/want incrementally
    " (zcl_abapgit_ortec_fetch_neg) instead of a full non-thin pack.
    TRY.
        zcl_abapgit_git_transport=>upload_pack_by_commit(
          EXPORTING
            iv_url     = iv_url
            iv_hash    = iv_commit
          IMPORTING
            et_objects = lt_objects
            ev_commit  = lv_fetched_head ).
      CATCH zcx_abapgit_exception INTO DATA(lx_fetch).
        zcx_abapgit_ortec_git=>raise( |Missing-object fetch failed: { lx_fetch->get_text( ) }| ).
    ENDTRY.

    " Step 3: persist everything the fetch returned (bulk insert).
    TRY.
        zcl_abapgit_ortec_obj_store=>store_objects(
          iv_repo_key = iv_repo_key
          it_objects  = lt_objects ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_store).
        zcx_abapgit_ortec_git=>raise( |Missing-object persist failed: { lx_store->get_text( ) }| ).
    ENDTRY.

    " Step 4: retry once - re-check what is still missing after the fetch+persist.
    lt_missing = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
      iv_repo_key = iv_repo_key
      it_sha1s    = it_sha1s ).

    IF lt_missing IS NOT INITIAL.
      zcx_abapgit_ortec_git=>raise(
        |{ lines( lt_missing ) } object(s) still missing after negotiated fetch| ).
    ENDIF.

  ENDMETHOD.

ENDCLASS.
