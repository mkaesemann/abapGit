"! <p class="shorttext synchronized">ORTEC Git Fetch Negotiation</p>
"! Provides known commit SHA1s (have lines) for Git want/have negotiation.
CLASS zcl_abapgit_ortec_fetch_neg DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.

    "! Get known commit SHA1s to send as 'have' lines during upload-pack.
    "! Returns empty table if no local state exists or negotiation is inactive.
    "! @parameter iv_url |
    "! Remote URL (used to resolve repository key)
    "! @parameter it_want_hashes |
    "! SHA1s being requested (want lines)
    "! @parameter rt_haves |
    "! Known commit SHA1s to send as have lines
    "! @raising zcx_abapgit_ortec_git |
    "! On error
    CLASS-METHODS get_have_commits
      IMPORTING iv_url          TYPE string
                it_want_hashes  TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rt_haves) TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING   zcx_abapgit_ortec_git.

  PROTECTED SECTION.
  PRIVATE SECTION.
    "! BFS walk over locally-stored commit ancestors.
    "! Starts from it_seeds (known-complete commits) and traverses parents
    "! up to iv_max_depth levels deep, collecting all reachable SHA1s.
    "! Capped at iv_max_results total results to bound memory and pkt-line size.
    "! @parameter iv_repo_key | Repository key
    "! @parameter it_seeds | Starting commit SHA1s (usually branch tips)
    "! @parameter iv_max_depth | Maximum BFS depth (default 50)
    "! @parameter iv_max_results | Maximum total SHA1s to return (default 200)
    "! @parameter rt_commits | All reachable ancestor SHA1s
    CLASS-METHODS collect_ancestor_haves
      IMPORTING iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                it_seeds          TYPE zif_abapgit_git_definitions=>ty_sha1_tt
                iv_max_depth      TYPE i DEFAULT 50
                iv_max_results    TYPE i DEFAULT 200
      RETURNING VALUE(rt_commits) TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
ENDCLASS.


CLASS zcl_abapgit_ortec_fetch_neg IMPLEMENTATION.

  METHOD get_have_commits.

    DATA lv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    " Resolve URL to repo key
    lv_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    IF lv_repo_key IS INITIAL.
      RETURN. " No stored state for this URL
    ENDIF.

    " Use complete-commit history (multi-branch, ordered newest first)
    rt_haves = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_repo_key ).

    " Cap to 100 entries so the pkt-line header stays within Git limits
    IF lines( rt_haves ) > 100.
      DELETE rt_haves FROM 101.
    ENDIF.

    DELETE rt_haves WHERE table_line IS INITIAL.

    " Remove any SHA1s that are also in the want list
    DATA lv_want LIKE LINE OF it_want_hashes.
    LOOP AT it_want_hashes INTO lv_want.
      DELETE rt_haves WHERE table_line = lv_want.
    ENDLOOP.

    " Extend have-list with ancestor walk (BFS up to 50 levels, 200 commits)
    DATA lt_ancestors TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    lt_ancestors = collect_ancestor_haves( iv_repo_key = lv_repo_key
                                           it_seeds    = rt_haves ).
    LOOP AT lt_ancestors INTO DATA(lv_anc).
      READ TABLE rt_haves WITH KEY table_line = lv_anc TRANSPORTING NO FIELDS.
      IF sy-subrc <> 0.
        APPEND lv_anc TO rt_haves.
      ENDIF.
    ENDLOOP.
    " Re-apply want exclusion on merged list
    LOOP AT it_want_hashes INTO lv_want.
      DELETE rt_haves WHERE table_line = lv_want.
    ENDLOOP.
    " Final cap
    IF lines( rt_haves ) > 200.
      DELETE rt_haves FROM 201.
    ENDIF.

  ENDMETHOD.

  METHOD collect_ancestor_haves.
    TYPES: BEGIN OF ty_commit_row,
             obj_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1,
             obj_data TYPE xstring,
           END OF ty_commit_row.

    DATA lt_all_commits TYPE HASHED TABLE OF ty_commit_row
                        WITH UNIQUE KEY obj_sha1.
    DATA lt_visited     TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1
                        WITH UNIQUE KEY table_line.
    DATA lt_queue       TYPE STANDARD TABLE OF zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_next        TYPE STANDARD TABLE OF zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_sha1        TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_depth       TYPE i VALUE 0.
    DATA lv_text        TYPE string.
    DATA lt_par_lines   TYPE TABLE OF string.
    DATA lv_par_line    TYPE string.
    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_all_commits.

    " Single DB read: all ready commit objects for this repo
    SELECT obj_sha1, obj_data FROM zaog_obj_store
      INTO TABLE @lt_all_commits
      WHERE repo_key = @iv_repo_key
        AND obj_type = 'commit'
        AND status   = 'R'.

    " Seed BFS queue
    lt_queue = it_seeds.

    WHILE lt_queue IS NOT INITIAL AND lv_depth < iv_max_depth
                                  AND lines( rt_commits ) < iv_max_results.
      CLEAR lt_next.
      LOOP AT lt_queue INTO lv_sha1.
        READ TABLE lt_visited WITH TABLE KEY table_line = lv_sha1 TRANSPORTING NO FIELDS.
        IF sy-subrc = 0. CONTINUE. ENDIF.
        INSERT lv_sha1 INTO TABLE lt_visited.
        APPEND lv_sha1 TO rt_commits.
        IF lines( rt_commits ) >= iv_max_results. EXIT. ENDIF.
        " Parse parents directly from pre-loaded commit data (no N+1 DB reads)
        READ TABLE lt_all_commits ASSIGNING <ls_row>
             WITH TABLE KEY obj_sha1 = lv_sha1.
        IF sy-subrc <> 0. CONTINUE. ENDIF.
        CLEAR lt_par_lines.
        TRY.
            lv_text = cl_abap_codepage=>convert_from( source   = <ls_row>-obj_data
                                                      codepage = '4110' ).
          CATCH cx_parameter_invalid_range cx_sy_conversion_codepage.
            CONTINUE. " Commit data not valid UTF-8 — skip parent walk for this entry
        ENDTRY.
        SPLIT lv_text AT cl_abap_char_utilities=>newline INTO TABLE lt_par_lines.
        LOOP AT lt_par_lines INTO lv_par_line.
          IF lv_par_line IS INITIAL. EXIT. ENDIF.
          IF strlen( lv_par_line ) >= 47 AND lv_par_line(7) = 'parent '.
            READ TABLE lt_visited WITH TABLE KEY table_line = lv_par_line+7(40)
                 TRANSPORTING NO FIELDS.
            IF sy-subrc <> 0.
              APPEND CONV zif_abapgit_git_definitions=>ty_sha1( lv_par_line+7(40) ) TO lt_next.
            ENDIF.
          ENDIF.
        ENDLOOP.
      ENDLOOP.
      lt_queue = lt_next.
      lv_depth = lv_depth + 1.
    ENDWHILE.
  ENDMETHOD.

ENDCLASS.
