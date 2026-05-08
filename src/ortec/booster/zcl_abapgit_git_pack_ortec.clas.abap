CLASS zcl_abapgit_git_pack_ortec DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    CLASS-METHODS allow_simplified_decompress
      RETURNING VALUE(rv_allowed) TYPE abap_bool.

ENDCLASS.


CLASS zcl_abapgit_git_pack_ortec IMPLEMENTATION.
  METHOD allow_simplified_decompress.

    " We don't use TFS 2017 and visualstudio.com Git repos,
    " so we can always allow the simplified decompression logic that
    " relies on the zlib header for strategy decision.
    rv_allowed = abap_true.

  ENDMETHOD.
ENDCLASS.
