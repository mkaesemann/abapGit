CLASS zcx_abapgit_ortec_git DEFINITION
  PUBLIC
  INHERITING FROM cx_static_check
  CREATE PUBLIC.

  PUBLIC SECTION.

    INTERFACES if_t100_message.

    "! Exception text
    DATA mv_text TYPE string READ-ONLY.

    "! Flag indicating data corruption was detected.
    "! If set, the caller MUST NOT silently fall back to standard behavior.
    DATA mv_is_corruption TYPE abap_bool READ-ONLY.

    "! @parameter iv_text |
    "! Exception text
    "! @parameter iv_is_corruption |
    "! Flag for data corruption
    "! @parameter previous |
    "! Previous exception
    METHODS constructor
      IMPORTING
        iv_text          TYPE clike DEFAULT ''
        iv_is_corruption TYPE abap_bool DEFAULT abap_false
        previous         TYPE REF TO cx_root OPTIONAL.

    "! Raise a non-corruption ORTEC exception.
    "! Callers should catch and fall back to standard abapGit behavior.
    "! @parameter iv_text |
    "! Exception text
    "! @raising zcx_abapgit_ortec_git |
    "! Exception
    CLASS-METHODS raise
      IMPORTING
        iv_text TYPE clike
      RAISING
        zcx_abapgit_ortec_git.

    "! Raise a corruption exception as zcx_abapgit_exception.
    "! Must NOT be caught silently — data integrity is at risk.
    "! @parameter iv_text |
    "! Exception text
    "! @raising zcx_abapgit_exception |
    "! Exception
    CLASS-METHODS raise_corruption
      IMPORTING
        iv_text TYPE clike
      RAISING
        zcx_abapgit_exception.

    "! Returns mv_text - this class never populates the T100 message
    "! infrastructure (if_t100_message~t100key is deliberately left blank),
    "! so the inherited cx_root default would otherwise return a generic/
    "! empty string instead of the actual failure detail. Confirmed live:
    "! every ->get_text( ) call on a zcx_abapgit_ortec_git instance anywhere
    "! in the codebase (including cascade-combining messages such as
    "! zcl_abapgit_ortec_fastpath's "thin: X, non-thin: Y") was silently
    "! producing blank text without this override.
    METHODS get_text REDEFINITION.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcx_abapgit_ortec_git IMPLEMENTATION.

  METHOD constructor ##ADT_SUPPRESS_GENERATION.
    super->constructor( previous = previous ).
    mv_text = iv_text.
    mv_is_corruption = iv_is_corruption.
  ENDMETHOD.


  METHOD raise.
    RAISE EXCEPTION TYPE zcx_abapgit_ortec_git
      EXPORTING
        iv_text = iv_text.
  ENDMETHOD.


  METHOD raise_corruption.
    zcx_abapgit_exception=>raise( |ORTEC Git corruption: { iv_text }| ).
  ENDMETHOD.

  METHOD get_text.
    IF mv_text IS NOT INITIAL.
      result = mv_text.
    ELSE.
      result = super->get_text( ).
    ENDIF.
  ENDMETHOD.

ENDCLASS.
