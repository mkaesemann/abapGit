CLASS zcl_abapgit_blob_buffer DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    CONSTANTS mc_type_branch_data TYPE c LENGTH 12 VALUE 'BRANCH-DATA'.
    CONSTANTS mc_type_branch_key  TYPE c LENGTH 12 VALUE 'BRANCH-KEY'.

    TYPES: BEGIN OF ty_branch_key,
             url    TYPE string,
             branch TYPE string,
           END OF ty_branch_key.

    CLASS-METHODS save_remote_branch
      IMPORTING is_branch  TYPE ty_branch_key
                i_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
                it_objects TYPE ANY TABLE
                it_files   TYPE ANY TABLE.

    CLASS-METHODS get_remote_branch_key
      IMPORTING is_branch       TYPE ty_branch_key
      RETURNING VALUE(r_commit) TYPE zif_abapgit_git_definitions=>ty_sha1 .

    CLASS-METHODS get_remote_branch
      IMPORTING is_branch  TYPE ty_branch_key
      EXPORTING e_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
                et_objects TYPE ANY TABLE
                et_files   TYPE ANY TABLE.

    CLASS-METHODS drop_remote_branch
      IMPORTING i_branch_name TYPE string.

  PRIVATE SECTION.
    CLASS-METHODS get_db_key
      IMPORTING is_branch       TYPE zcl_abapgit_blob_buffer=>ty_branch_key
      RETURNING VALUE(r_result) TYPE string.
ENDCLASS.


CLASS zcl_abapgit_blob_buffer IMPLEMENTATION.
  METHOD get_remote_branch_key.

    DATA(keyid) = get_db_key(
                      is_branch ).

    SELECT SINGLE * FROM zabapgit_blob
      INTO @DATA(ls_key)
      WHERE type  = @mc_type_branch_key
        AND keyid = @keyid.
    IF sy-subrc = 0.
      IMPORT commit = r_commit FROM DATA BUFFER ls_key-data
             ACCEPTING PADDING
             ACCEPTING TRUNCATION.
    ENDIF.

  ENDMETHOD.

  METHOD get_remote_branch.

    e_commit = get_remote_branch_key(
                   is_branch ).

    DATA(keyid) = get_db_key(
                      is_branch ).
    SELECT SINGLE * FROM zabapgit_blob
      INTO @DATA(ls_data)
      WHERE type  = @mc_type_branch_data
        AND keyid = @keyid.
    IF sy-subrc = 0.
      IMPORT object = et_objects
             files  = et_files
             FROM DATA BUFFER ls_data-data
             ACCEPTING PADDING
             ACCEPTING TRUNCATION.
    ENDIF.

  ENDMETHOD.

  METHOD save_remote_branch.

    DATA(ls_key) = VALUE zabapgit_blob( ).
    DATA(ls_data) = VALUE zabapgit_blob( ).

    ls_key-type  = mc_type_branch_key.
    ls_key-keyid = get_db_key(
                       is_branch ).

    ls_data-type  = mc_type_branch_data.
    ls_data-keyid = get_db_key(
                        is_branch ).

    TRY.
        EXPORT commit = i_commit
               TO DATA BUFFER ls_key-data
               COMPRESSION ON.

        EXPORT object = it_objects
               files  = it_files
               TO DATA BUFFER ls_data-data
               COMPRESSION ON.
      CATCH cx_sy_compression_error.
        " Buffering is an optimization; skip cache write if payload is too large
        RETURN.
    ENDTRY.

    MODIFY zabapgit_blob FROM ls_key.
    MODIFY zabapgit_blob FROM ls_data.

    COMMIT WORK.

  ENDMETHOD.

  METHOD drop_remote_branch.
    DELETE FROM zabapgit_blob WHERE type  = mc_type_branch_key
                                AND keyid = i_branch_name.
    DELETE FROM zabapgit_blob WHERE type  = mc_type_branch_data
                                AND keyid = i_branch_name.
    COMMIT WORK.
  ENDMETHOD.

  METHOD get_db_key.

    CLEAR r_result.
    CONCATENATE is_branch-url is_branch-branch
                INTO DATA(charkey) IN CHARACTER MODE
                SEPARATED BY cl_abap_char_utilities=>horizontal_tab.

    TRY.
        cl_abap_message_digest=>calculate_hash_for_char(
          EXPORTING
            if_algorithm     = |SHA512|
            if_data          = charkey
          IMPORTING
            ef_hashstring    = r_result
          " TODO: variable is assigned but never used (ABAP cleaner)
            ef_hashb64string = DATA(resultb64) ).

      CATCH cx_abap_message_digest.
        CLEAR r_result.
    ENDTRY.

  ENDMETHOD.
ENDCLASS.
