class ZCL_ABAPGIT_PULL_BUFFER definition
  public
  final
  create protected .

public section.

  class-methods PULL_BUFFERED_BRANCH
    importing
      !IV_URL type STRING
      !IV_BRANCH_NAME type STRING
    returning
      value(RS_RESULT) type ZCL_ABAPGIT_GIT_PORCELAIN=>TY_PULL_RESULT .
  class-methods STORE_BRANCH_IN_BUFFER
    importing
      !IV_URL type STRING
      !IV_BRANCH_NAME type STRING
      !IV_COMMIT type ZIF_ABAPGIT_GIT_DEFINITIONS=>TY_SHA1
      !IT_OBJECTS type ANY TABLE
      !IT_FILES type ANY TABLE .
protected section.
private section.
ENDCLASS.



CLASS ZCL_ABAPGIT_PULL_BUFFER IMPLEMENTATION.


  METHOD pull_buffered_branch.

    zcl_abapgit_git_transport=>find_branch(
      EXPORTING
        iv_url         = iv_url
        iv_service     = zcl_abapgit_git_transport=>c_service-upload
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
