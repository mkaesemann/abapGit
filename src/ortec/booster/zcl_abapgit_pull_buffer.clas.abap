CLASS zcl_abapgit_pull_buffer DEFINITION
  PUBLIC
  FINAL
  CREATE PROTECTED .

  PUBLIC SECTION.

    "This needs to mirror ZCL_ABAPGIT_GIT_TRANSPORT=>C_SERVICE
    CONSTANTS:
      BEGIN OF c_service,
        receive TYPE string VALUE 'receive',                                    "#EC NOTEXT
        upload  TYPE string VALUE 'upload',                                     "#EC NOTEXT
      END OF c_service .

    CLASS-METHODS pull_buffered_branch
      IMPORTING
                !iv_url          TYPE string
                !iv_branch_name  TYPE string
      RETURNING
                VALUE(rs_result) TYPE zcl_abapgit_git_porcelain=>ty_pull_result
      RAISING   zcx_abapgit_exception.
    CLASS-METHODS store_branch_in_buffer
      IMPORTING
        !iv_url         TYPE string
        !iv_branch_name TYPE string
        !iv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1
        !it_objects     TYPE ANY TABLE
        !it_files       TYPE ANY TABLE .
  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS ZCL_ABAPGIT_PULL_BUFFER IMPLEMENTATION.


  METHOD pull_buffered_branch.

    zcl_abapgit_git_transport=>find_branch_ortec(
      EXPORTING
        iv_url         = iv_url
        iv_service     = c_service-upload
        iv_branch_name = iv_branch_name
      IMPORTING
        ev_branch      = DATA(remote_sha) ).

    DATA(local_sha) = zcl_abapgit_blob_buffer=>get_remote_branch_key(
                          VALUE #( url    = iv_url
                                   branch = iv_branch_name ) ).

    IF local_sha = remote_sha.
      zcl_abapgit_blob_buffer=>get_remote_branch(
        EXPORTING
          is_branch  = VALUE #( url    = iv_url
                                branch = iv_branch_name )
        IMPORTING
          e_commit   = rs_result-commit
          et_objects = rs_result-objects
          et_files   = rs_result-files ).

    ELSE.
      CLEAR: rs_result.
    ENDIF.

  ENDMETHOD.


  METHOD store_branch_in_buffer.

    zcl_abapgit_blob_buffer=>save_remote_branch(
      is_branch  = VALUE #( url    = iv_url
                            branch = iv_branch_name )
      i_commit   = iv_commit
      it_objects = it_objects
      it_files   = it_files
    ).

  ENDMETHOD.
ENDCLASS.
