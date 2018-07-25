FUNCTION z_fm_abapgit_check_exists.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(IT_TADIR) TYPE  ZPP_ABAPGIT_T_TADIR
*"  EXPORTING
*"     VALUE(RT_TADIR_EXISTING) TYPE  ZPP_ABAPGIT_T_TADIR
*"     VALUE(RT_ERRORS) TYPE  ZPP_ABAPGIT_T_SERIAL_ERROR
*"  EXCEPTIONS
*"      ABAPGIT_EXCEPTION
*"----------------------------------------------------------------------

  DATA: ls_item TYPE zif_abapgit_definitions=>ty_item.

* rows from database table TADIR are not removed for
* transportable objects until the transport is released
  LOOP AT it_tadir ASSIGNING FIELD-SYMBOL(<ls_tadir>).

    ls_item-obj_type = <ls_tadir>-object.
    ls_item-obj_name = <ls_tadir>-obj_name.
    ls_item-devclass = <ls_tadir>-devclass.

    IF ls_item IS INITIAL.
      CONTINUE.
    ENDIF.

    TRY.

        IF zcl_abapgit_objects=>is_supported( ls_item ) = abap_false.
          INSERT <ls_tadir> INTO TABLE rt_tadir_existing.
          CONTINUE.
        ENDIF.

        IF zcl_abapgit_objects=>exists( ls_item ) = abap_true.
          INSERT <ls_tadir> INTO TABLE rt_tadir_existing.
        ENDIF.

      CATCH zcx_abapgit_exception INTO DATA(ox_exception).
        "Encountered a serialization error: Log and continue
        INSERT VALUE #( item = ls_item
                        type = 'E'
                        msg  = ox_exception->get_text( ) )
          INTO TABLE rt_errors.
    ENDTRY.

  ENDLOOP.

ENDFUNCTION.
