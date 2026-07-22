"! <p class="shorttext synchronized">ORTEC Git Certified Have Policy</p>
"! Variant B / Package C (Slices 5+6): certificate-only operation
"! classification and certified-have selection, backed exclusively by
"! ZCL_ABAPGIT_ORTEC_MAT_STATE / ZAOG_COMMIT_HIST. Replaces
"! ZCL_ABAPGIT_ORTEC_FETCH_NEG=>GET_VERIFIED_HAVE_COMMITS (tree-walk +
"! ZAOG_REPO_STATE fetch_commit-adjacent ancestor BFS) with a single bulk,
"! certificate-scoped read. See .memory/logs/variant_b_package_c_design.md
"! for the approved design (C0, APPROVED_WITH_RESOLVED_REVISIONS).
"!
"! API boundary (design §1/§3/§3a, closes C0 correctness re-review DR-006):
"! CLASSIFY_OPERATION is a pure certificate read - no HTTP, no write, no
"! COMMIT WORK, no raw FETCH_COMMIT read, no object-graph traversal.
"! TRY_BACKFILL_TARGET is a separate, explicitly named operation that owns
"! the one bounded, local, HTTP-free certification side effect (§3a);
"! it is never called from inside CLASSIFY_OPERATION. C1 does not wire
"! either method into ZCL_ABAPGIT_ORTEC_PORCELAIN - that wiring, together
"! with branch/cold-branch routing and incremental publication, is C2
"! scope.
CLASS zcl_abapgit_ortec_have_policy DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES ty_op_class TYPE string.

    CONSTANTS:
      BEGIN OF cs_op_class,
        warm_unchanged     TYPE ty_op_class VALUE 'WARM_UNCHANGED',
        incremental_update TYPE ty_op_class VALUE 'INCREMENTAL_UPDATE',
        cold_branch        TYPE ty_op_class VALUE 'COLD_BRANCH',
      END OF cs_op_class.

    "! Pure certificate-only classification (design §3). Reads exactly one
    "! ZCL_ABAPGIT_ORTEC_MAT_STATE=>GET_STATE row (O(1) keyed) plus, only
    "! when needed, one bulk GET_CERTIFIED_HAVES call. Never performs HTTP,
    "! never writes a certificate, never issues COMMIT WORK, never reads
    "! raw ZAOG_REPO_STATE-FETCH_COMMIT, and never walks the object graph.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_target_commit |
    "! Commit SHA1 being classified (the pull/fetch target)
    "! @parameter rv_class |
    "! One of CS_OP_CLASS-WARM_UNCHANGED / INCREMENTAL_UPDATE / COLD_BRANCH
    CLASS-METHODS classify_operation
      IMPORTING iv_repo_key      TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_target_commit TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rv_class)  TYPE ty_op_class.

    "! Certified-have selection (design §5). Single bulk read against
    "! ZAOG_COMMIT_HIST, scoped by repository key (leading non-client
    "! primary-key column - a primary-key-prefix range scan, never a
    "! cross-repository table scan), HIST_LEVEL = FULL_COMPLETE only
    "! (protocol review F1: a GRAPH_COMPLETE-only commit may still have a
    "! promised, not-locally-present blob - unsafe to offer as a thin-pack
    "! have). Excludes every SHA1 in IT_WANT_HASHES, sorts deterministically
    "! (UPDATED_AT descending, COMMIT_SHA1 ascending as tie-break), and caps
    "! the result at IV_MAX_HAVES. Performs no object-store payload read, no
    "! graph walk, and no SQL per candidate - deduplication is structural
    "! (ZAOG_COMMIT_HIST's primary key is REPO_KEY + COMMIT_SHA1, so the one
    "! SELECT cannot itself return duplicate SHA1s). A genuine technical SQL
    "! failure is never caught or normalized here - it propagates as a
    "! runtime error, not a handled exception.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter it_want_hashes |
    "! SHA1s being requested (want lines) - excluded from the result
    "! @parameter iv_max_haves |
    "! Maximum number of haves to return (default 50)
    "! @parameter rt_haves |
    "! Certified (FULL_COMPLETE) have SHA1s, deterministically ordered and
    "! capped; empty (not raised) when no eligible candidate exists
    CLASS-METHODS get_certified_haves
      IMPORTING iv_repo_key      TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                it_want_hashes   TYPE zif_abapgit_git_definitions=>ty_sha1_tt OPTIONAL
                iv_max_haves     TYPE i DEFAULT 50
      RETURNING VALUE(rt_haves)  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    "! Opportunistic local (HTTP-free) certification of a single, already
    "! locally-stored-but-uncertified commit (design §3a - closes DR-003's
    "! migration-day "cold storm" for every existing repo whose commits
    "! were persisted before Package C by the raw, non-certifying
    "! ZCL_ABAPGIT_ORTEC_FASTPATH=>PERSIST_PULL_RESULT). Bounded by K (the
    "! target commit's own reachable graph/blob set), reuses only
    "! already-approved Package B bulk APIs, idempotent, and intended to be
    "! invoked by the orchestrator at most once per classification decision
    "! - never from inside CLASSIFY_OPERATION.
    "!
    "! Steps: (1) skip immediately if the commit was never stored locally
    "! at all; (2) begin/resume a materialization attempt; (3) verify tree
    "! closure - only THIS step's ZCX_ABAPGIT_ORTEC_GIT failure is treated
    "! as "locally incomplete" and swallowed (design §6 step 2 / protocol
    "! review F2); (4) mark graph complete; (5)-(6) discover the tip's blob
    "! set and bulk-check presence; (7) only when no blobs are missing, mark
    "! full complete and, when IV_BRANCH_NAME is supplied, publish the
    "! branch's snapshot pointer (ZCL_ABAPGIT_ORTEC_MAT_STATE=>
    "! PUBLISH_SNAPSHOT_COMPLETE requires a branch name; when the caller has
    "! no branch context - e.g. a commit-only pull - the have-eligibility
    "! certificate is still established, but no branch is reclassified to
    "! WARM_UNCHANGED), then issue exactly one COMMIT WORK. On incomplete
    "! local data, no HTTP is performed and no full/snapshot completeness is
    "! published - the caller should treat RV_CERTIFIED = ABAP_FALSE as
    "! "fall back to COLD_BRANCH".
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_target_commit |
    "! Commit SHA1 to attempt local certification for
    "! @parameter iv_branch_name |
    "! Branch whose snapshot pointer should be published on full success;
    "! when blank, full graph+blob certification still happens but no
    "! branch is published (no WARM_UNCHANGED reclassification for a
    "! branch-less caller)
    "! @parameter rv_certified |
    "! ABAP_TRUE iff the target commit reached FULL_COMPLETE (every
    "! reachable tree and tip blob verified present) during this call
    "! @raising zcx_abapgit_ortec_git |
    "! On a genuine technical/persistence failure (never on plain local
    "! incompleteness, which is reported via RV_CERTIFIED = ABAP_FALSE)
    CLASS-METHODS try_backfill_target
      IMPORTING iv_repo_key        TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_target_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_branch_name     TYPE string OPTIONAL
      RETURNING VALUE(rv_certified) TYPE abap_bool
      RAISING   zcx_abapgit_ortec_git.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcl_abapgit_ortec_have_policy IMPLEMENTATION.

  METHOD classify_operation.
    DATA ls_state      TYPE zcl_abapgit_ortec_mat_state=>ty_state.
    DATA lt_candidates TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    rv_class = cs_op_class-cold_branch.

    IF iv_repo_key IS INITIAL OR iv_target_commit IS INITIAL.
      RETURN.
    ENDIF.

    ls_state = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = iv_repo_key
      iv_commit   = iv_target_commit ).

    IF ls_state-snap_state = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete.
      rv_class = cs_op_class-warm_unchanged.
      RETURN.
    ENDIF.

    lt_candidates = get_certified_haves(
      iv_repo_key    = iv_repo_key
      it_want_hashes = VALUE #( ( iv_target_commit ) )
      iv_max_haves   = 1 ).

    IF lt_candidates IS NOT INITIAL.
      rv_class = cs_op_class-incremental_update.
    ENDIF.
  ENDMETHOD.

  METHOD get_certified_haves.
    TYPES: BEGIN OF ty_candidate,
             commit_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1,
             updated_at  TYPE timestampl,
           END OF ty_candidate.

    DATA lt_candidates TYPE STANDARD TABLE OF ty_candidate WITH DEFAULT KEY.
    DATA lv_max        TYPE i.

    FIELD-SYMBOLS <ls_candidate> LIKE LINE OF lt_candidates.

    IF iv_repo_key IS INITIAL.
      RETURN.
    ENDIF.

    lv_max = iv_max_haves.
    IF lv_max <= 0.
      RETURN.
    ENDIF.

    " Single bulk read, scoped by REPO_KEY (leading non-client primary-key
    " column). HIST_LEVEL = FULL_COMPLETE only (protocol review F1) - never
    " GRAPH_COMPLETE, never a ZAOG_REPO_STATE branch pointer. No object
    " payload read, no graph walk, no per-candidate SQL. Not wrapped in a
    " TRY/CATCH - a genuine technical SQL failure propagates uncaught.
    SELECT commit_sha1, updated_at
      FROM zaog_commit_hist
      INTO TABLE @lt_candidates
      WHERE repo_key   = @iv_repo_key
        AND hist_level = @zcl_abapgit_ortec_mat_state=>cs_hist_level-full_complete.

    IF lt_candidates IS INITIAL.
      RETURN.
    ENDIF.

    " ZAOG_COMMIT_HIST's primary key is REPO_KEY + COMMIT_SHA1, so the
    " SELECT above cannot itself return duplicate SHA1s - this sort is
    " purely for deterministic ordering, not deduplication.
    SORT lt_candidates BY updated_at DESCENDING commit_sha1 ASCENDING.

    LOOP AT lt_candidates ASSIGNING <ls_candidate>.
      IF line_exists( it_want_hashes[ table_line = <ls_candidate>-commit_sha1 ] ).
        CONTINUE.
      ENDIF.

      APPEND <ls_candidate>-commit_sha1 TO rt_haves.
      IF lines( rt_haves ) >= lv_max.
        EXIT.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD try_backfill_target.
    DATA lv_attempt_id TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA lt_tip_blobs  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_missing    TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    rv_certified = abap_false.

    IF iv_repo_key IS INITIAL OR iv_target_commit IS INITIAL.
      RETURN.
    ENDIF.

    " Step 1: never attempt backfill for a commit genuinely never stored -
    " only an already-present-but-uncertified commit is eligible.
    IF zcl_abapgit_ortec_obj_store=>exists(
         iv_repo_key = iv_repo_key
         iv_sha1     = iv_target_commit ) = abap_false.
      RETURN.
    ENDIF.

    " Step 2: begin/resume the attempt. Never downgrades an existing
    " certificate (ZCL_ABAPGIT_ORTEC_MAT_STATE's own contract).
    lv_attempt_id = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = iv_repo_key
      iv_commit   = iv_target_commit ).

    " Step 3: verify tree closure. Only this call's ZCX_ABAPGIT_ORTEC_GIT
    " is treated as "locally incomplete, fall back to COLD_BRANCH" (design
    " §6 step 2 / protocol review F2) - any other failure below is never
    " caught here and propagates to the caller.
    TRY.
        zcl_abapgit_ortec_obj_store=>verify_tree_closure(
          iv_repo_key = iv_repo_key
          iv_commit   = iv_target_commit ).
      CATCH zcx_abapgit_ortec_git.
        RETURN.
    ENDTRY.

    " Step 4: graph certified.
    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = iv_repo_key
      iv_commit     = iv_target_commit
      iv_attempt_id = lv_attempt_id ).

    " Steps 5-6: discover the tip's blob set and bulk-check presence.
    lt_tip_blobs = zcl_abapgit_ortec_obj_store=>get_tip_blob_sha1s(
      iv_repo_key = iv_repo_key
      iv_commit   = iv_target_commit ).
    lt_missing = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
      iv_repo_key = iv_repo_key
      it_sha1s    = lt_tip_blobs ).

    IF lt_missing IS NOT INITIAL.
      RETURN. " blobs missing - graph-only certification stands, no publish
    ENDIF.

    " Step 7: full local closure proven.
    zcl_abapgit_ortec_mat_state=>mark_full_complete(
      iv_repo_key   = iv_repo_key
      iv_commit     = iv_target_commit
      iv_attempt_id = lv_attempt_id ).

    IF iv_branch_name IS NOT INITIAL.
      zcl_abapgit_ortec_mat_state=>publish_snapshot_complete(
        iv_repo_key    = iv_repo_key
        iv_branch_name = iv_branch_name
        iv_commit      = iv_target_commit
        iv_attempt_id  = lv_attempt_id ).
    ENDIF.

    COMMIT WORK.

    rv_certified = abap_true.
  ENDMETHOD.

ENDCLASS.
