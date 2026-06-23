CLASS lcl_filter DEFINITION FINAL.
  PUBLIC SECTION.
    INTERFACES zif_abapgit_object_filter.

    METHODS constructor
      IMPORTING
        is_item TYPE zif_abapgit_definitions=>ty_item.

  PRIVATE SECTION.
    DATA mt_filter TYPE zif_abapgit_definitions=>ty_tadir_tt.
ENDCLASS.

CLASS lcl_filter IMPLEMENTATION.
  METHOD constructor.
    DATA ls_filter TYPE zif_abapgit_definitions=>ty_tadir.
    ls_filter-object   = is_item-obj_type.
    ls_filter-obj_name = is_item-obj_name.
    INSERT ls_filter INTO TABLE mt_filter.
  ENDMETHOD.

  METHOD zif_abapgit_object_filter~get_filter.
    rt_filter = mt_filter.
  ENDMETHOD.
ENDCLASS.

" Multi-object filter built from a Stage selection (ty_stage_tt).
" Used to narrow remote retrieval and local serialization in the patch/diff flow.
CLASS lcl_multi_filter DEFINITION FINAL.
  PUBLIC SECTION.
    INTERFACES zif_abapgit_object_filter.
    METHODS constructor
      IMPORTING
        it_files TYPE zif_abapgit_definitions=>ty_stage_tt.
  PRIVATE SECTION.
    DATA mt_filter TYPE zif_abapgit_definitions=>ty_tadir_tt.
ENDCLASS.

CLASS lcl_multi_filter IMPLEMENTATION.
  METHOD constructor.
    DATA ls_filter TYPE zif_abapgit_definitions=>ty_tadir.
    FIELD-SYMBOLS <ls_file> LIKE LINE OF it_files.

    LOOP AT it_files ASSIGNING <ls_file>
        WHERE status-obj_type IS NOT INITIAL.
      ls_filter-object   = <ls_file>-status-obj_type.
      ls_filter-obj_name = <ls_file>-status-obj_name.
      APPEND ls_filter TO mt_filter.
    ENDLOOP.
    SORT mt_filter BY object obj_name.
    DELETE ADJACENT DUPLICATES FROM mt_filter COMPARING object obj_name.
  ENDMETHOD.

  METHOD zif_abapgit_object_filter~get_filter.
    rt_filter = mt_filter.
  ENDMETHOD.
ENDCLASS.
