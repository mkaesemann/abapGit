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

    "! Flag indicating a fresh fetch with NO haves offered (forcing the
    "! server to send a complete, self-contained pack) might succeed where
    "! this attempt failed - set when the failure means "the server said
    "! nothing new, but our own verification shows that is not actually
    "! true", as opposed to a failure that a haves-free retry cannot help
    "! with (e.g. a genuine network error).
    DATA mv_retry_without_haves TYPE abap_bool READ-ONLY.

    "! Flag indicating the request could not be built because a hard-
    "! required server capability was not advertised (e.g.
    "! INITIAL_BRANCH_BLOBLESS without `filter`, or MATERIALIZE_BLOBS
    "! without an arbitrary-blob-want capability). Callers MUST NOT
    "! silently fall back to an unfiltered/unbounded request when this
    "! flag is set - see mv_missing_capability for which capability was
    "! missing.
    DATA mv_unsupported_capability TYPE abap_bool READ-ONLY.

    "! The exact capability token that was required but not advertised
    "! by the server, set together with mv_unsupported_capability.
    DATA mv_missing_capability TYPE string READ-ONLY.

    "! Filtered call stack captured at RAISE time (this class's own
    "! constructor/raise* frames removed), so the real caller is still
    "! inspectable after the stack has unwound - see get_source_position.
    DATA mt_callstack TYPE abap_callstack READ-ONLY.

    "! Convenience snapshot of the real raise call site (program/include/
    "! line), captured at construction time from mt_callstack. By the time
    "! an exception reaches a CATCH block or a debugger breakpoint there,
    "! the original call stack that led to RAISE EXCEPTION has already
    "! unwound - RAISE EXCEPTION TYPE zcx_abapgit_ortec_git always happens
    "! inside the class's own static raise( ) method, so without this the
    "! only "source position" left to inspect points at raise( )'s own line,
    "! never the actual calling code that decided to raise. Inspect
    "! ms_src_info directly in the debugger, or call get_source_position( ).
    DATA ms_src_info TYPE zcx_abapgit_exception=>ty_scr_info READ-ONLY.

    "! @parameter iv_text |
    "! Exception text
    "! @parameter iv_is_corruption |
    "! Flag for data corruption
    "! @parameter previous |
    "! Previous exception
    METHODS constructor
      IMPORTING
        iv_text                    TYPE clike DEFAULT ''
        iv_is_corruption           TYPE abap_bool DEFAULT abap_false
        iv_retry_without_haves     TYPE abap_bool DEFAULT abap_false
        iv_unsupported_capability  TYPE abap_bool DEFAULT abap_false
        iv_missing_capability      TYPE string DEFAULT ''
        previous                   TYPE REF TO cx_root OPTIONAL.

    "! Raise a non-corruption ORTEC exception.
    "! Callers should catch and fall back to standard abapGit behavior.
    "! @parameter iv_text |
    "! Exception text
    "! @parameter iv_retry_without_haves |
    "! Set when a fresh, haves-free retry might succeed - see
    "! mv_retry_without_haves
    "! @raising zcx_abapgit_ortec_git |
    "! Exception
    CLASS-METHODS raise
      IMPORTING
        iv_text                TYPE clike
        iv_retry_without_haves TYPE abap_bool DEFAULT abap_false
      RAISING
        zcx_abapgit_ortec_git.

    "! Raise a structured unsupported-capability failure - the server did
    "! not advertise a capability a Variant B fetch mode hard-requires.
    "! Callers must decide their own fallback policy (e.g. cold-init
    "! cannot proceed); this factory only guarantees the signal is
    "! structured - it never itself falls through to an unfiltered/
    "! unbounded buffer.
    "! @parameter iv_mode |
    "! The ORTEC fetch mode that required the capability
    "! @parameter iv_capability |
    "! The missing capability token
    "! @raising zcx_abapgit_ortec_git |
    "! Always
    CLASS-METHODS raise_unsupported_capability
      IMPORTING
        iv_mode       TYPE zcl_abapgit_ortec_fetch_req=>ty_fetch_mode
        iv_capability TYPE string
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

    "! Returns the real RAISE call site (see ms_src_info) instead of the
    "! inherited cx_root default, which would otherwise point inside this
    "! class's own static raise( ) method.
    METHODS get_source_position REDEFINITION.

  PROTECTED SECTION.
  PRIVATE SECTION.
    METHODS save_callstack.
ENDCLASS.



CLASS zcx_abapgit_ortec_git IMPLEMENTATION.

  METHOD constructor ##ADT_SUPPRESS_GENERATION.
    super->constructor( previous = previous ).
    mv_text = iv_text.
    mv_is_corruption = iv_is_corruption.
    mv_retry_without_haves = iv_retry_without_haves.
    mv_unsupported_capability = iv_unsupported_capability.
    mv_missing_capability = iv_missing_capability.
    save_callstack( ).
    get_source_position(
      IMPORTING
        program_name = ms_src_info-program
        include_name = ms_src_info-include
        source_line  = ms_src_info-line ).
  ENDMETHOD.


  METHOD save_callstack.

    FIELD-SYMBOLS <ls_callstack> LIKE LINE OF mt_callstack.

    CALL FUNCTION 'SYSTEM_CALLSTACK'
      IMPORTING
        callstack = mt_callstack.

    " Remove this class's own frames (constructor, save_callstack, raise*)
    " so the highest remaining entry is the actual calling code that
    " decided to raise - mirrors zcx_abapgit_exception=>save_callstack.
    LOOP AT mt_callstack ASSIGNING <ls_callstack>.
      IF <ls_callstack>-mainprogram CP |ZCX_ABAPGIT_ORTEC_GIT*|
        OR <ls_callstack>-blockname = `SAVE_CALLSTACK`
        OR <ls_callstack>-blockname = `CONSTRUCTOR`
        OR <ls_callstack>-blockname CP `RAISE*`.
        DELETE TABLE mt_callstack FROM <ls_callstack>.
      ELSE.
        EXIT.
      ENDIF.
    ENDLOOP.

  ENDMETHOD.


  METHOD get_source_position.

    FIELD-SYMBOLS <ls_callstack> LIKE LINE OF mt_callstack.

    READ TABLE mt_callstack ASSIGNING <ls_callstack> INDEX 1.
    IF sy-subrc = 0.
      program_name = <ls_callstack>-mainprogram.
      include_name = <ls_callstack>-include.
      source_line  = <ls_callstack>-line.
    ELSE.
      super->get_source_position(
        IMPORTING
          program_name = program_name
          include_name = include_name
          source_line  = source_line ).
    ENDIF.

  ENDMETHOD.


  METHOD raise.
    RAISE EXCEPTION TYPE zcx_abapgit_ortec_git
      EXPORTING
        iv_text                = iv_text
        iv_retry_without_haves = iv_retry_without_haves.
  ENDMETHOD.


  METHOD raise_unsupported_capability.
    RAISE EXCEPTION TYPE zcx_abapgit_ortec_git
      EXPORTING
        iv_text                   = |ORTEC fetch mode { iv_mode } requires capability not advertised | &&
                                     |by server: { iv_capability }|
        iv_unsupported_capability = abap_true
        iv_missing_capability     = iv_capability.
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
