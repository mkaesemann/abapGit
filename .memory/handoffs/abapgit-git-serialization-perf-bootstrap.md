# abapGit Git/Serialization Performance Discovery — Bootstrap Handoff

```text
PACKET=COMPACT_HANDOFF_V1
TASK=ABAPGIT_GIT_SERIALIZATION_PERFORMANCE_DISCOVERY
STATUS=DISCOVERY_AND_BACKLOG_COMPLETE, REVIEW_APPROVED_WITH_MINOR_REVISIONS
  (applied)
ARTIFACTS=.memory/logs/abapgit_git_serialization_perf_discovery.md,
  .memory/logs/abapgit_git_serialization_perf_backlog.md,
  .memory/reviews/abapgit_git_serialization_perf_review.md,
  .memory/handoffs/abapgit-git-serialization-perf-bootstrap.md (this file)
```

No productive ABAP/DDIC/test code was changed. No `.memory/state.md`
change. No commit. No push. The performance-review MAJOR finding
(mis-attributed redundant-`branches()` call chain, F-2) was resolved by
correcting the discovery/backlog citations directly (not rejected) —
`.memory/reviews/abapgit_git_serialization_perf_review.md` records the
original finding as historical evidence of the review process; the fix
is reflected in the discovery/backlog files, not in the review file
itself.

## Recommended next slices (1-3, independent and measurable)

### Slice 1 — Owner-executed measurement bundle (M-1, M-3, M-4)
```text
NEXT_SLICE      Capture the three highest-value SAT traces identified in
                the backlog's Workstream E before any design work starts
WHY_NOW         Every candidate in the backlog (F-1, F-4, F-5) is
                MEASURE_FIRST specifically because no candidate's benefit
                can currently be distinguished from measurement noise;
                this is the cheapest possible next action and unblocks
                three candidates at once
AGENT           None (owner-executed on IT8; no AI agent in this
                workspace has live IT8 SAT/ST05 access — the connected
                mcp_arc-1/12 SAPDiagnose tools were previously confirmed
                NOT pointed at this repository's target IT8 system, per
                repo git-state notes)
MODEL           N/A
THINKING        N/A
CONTEXT         .memory/logs/abapgit_git_serialization_perf_backlog.md
                Workstream E (M-1, M-3, M-4 exact scenario definitions)
INITIAL_CONTEXT Owner has IT8 access and SAT/ST05 authorization; a
                40,000+-object repository (OS4-class) is available for
                M-1/M-3; a repeatable small-commit scenario is available
                for M-4
PROMPT_REQUIREMENTS N/A (owner action, not a delegated agent task)
OWNER_MEASUREMENTS M-1 (TADIR discovery SELECT cost at scale), M-3 (full
                local serialization peak memory/time at 40,000 objects,
                per-object-type time share), M-4 (does post-commit
                navigation call refresh(), and does the next
                get_files_local() re-serialize N or K objects)
STOP_CONDITIONS None of the three traces are obtainable within a
                reasonable window -> proceed to Slice 3 (F-4 code
                investigation) first, since it needs no live trace access
```

### Slice 2 — F-4 prerequisite investigation: does `refresh_local_object(s)`
already solve this?
```text
NEXT_SLICE      Read zif_abapgit_repo~refresh_local_object and
                ~refresh_local_objects (src/repo/zcl_abapgit_repo.clas.abap,
                not read in this discovery pass) and every existing caller,
                to determine whether a K-sized/incremental local-cache
                update primitive already exists and is simply unused by
                whichever path triggers refresh() most often
WHY_NOW         Cheap (single-class read, no live trace needed), fully
                independent of Slice 1/3, and could turn F-4 from a new
                DESIGN_REQUIRED mechanism into a much smaller wiring fix
                if the primitive already exists — resolves this before
                any design effort is spent
AGENT           ortec-abapgit-discovery
MODEL           MAI-Code-1-Flash
THINKING        Low — mechanical call-graph tracing, no architecture
                decision
CONTEXT         .memory/logs/abapgit_git_serialization_perf_discovery.md
                §3.3 (F-4 evidence and open question)
INITIAL_CONTEXT ALLOWED_CONTEXT=.memory/logs/
                abapgit_git_serialization_perf_discovery.md,
                .memory/logs/abapgit_git_serialization_perf_backlog.md;
                SOURCE_SCOPE=src/repo/zcl_abapgit_repo.clas.abap
                (refresh_local_object, refresh_local_objects, and every
                caller found via reference search),
                src/repo/zif_abapgit_repo.intf.abap (method contracts);
                FORBIDDEN_PATHS=.memory/archive/**, .memory/state.md,
                .memory/diagrams/**, any productive ABAP/DDIC edit;
                STATE_WRITE_ALLOWED=no; DIAGRAM_WRITE_ALLOWED=no
PROMPT_REQUIREMENTS Report: (1) exact semantics of both methods
                (what they invalidate/re-serialize, K-sized or N-sized),
                (2) every current caller and what triggers each call,
                (3) whether either method is already reachable from a
                post-commit/post-push code path, (4) a one-line
                recommendation: "F-4 is a wiring fix" or "F-4 genuinely
                needs a new mechanism, because ___"
OWNER_MEASUREMENTS None required for this slice
STOP_CONDITIONS If the read reveals the methods are already correctly
                wired and F-4's concern is theoretical (no real full
                re-serialization occurs in practice) -> close F-4 as
                ALREADY_IMPLEMENTED in the backlog instead of proceeding
                to a design
```

### Slice 3 — F-2 design: single resolved branch tip per user action
```text
NEXT_SLICE      Design a request-scoped (single top-level call only,
                never cross-request/persistent) mechanism so
                zcl_abapgit_ortec_porcelain=>pull_by_branch,
                zcl_abapgit_ortec_fastpath=>pull_by_branch/
                upload_pack_by_branch, and zcl_abapgit_ortec_filter_walk=>
                get_remote_files_for_stage share one resolved branch tip
                per user action instead of each independently calling
                zcl_abapgit_git_transport=>branches()/find_branch_ortec
WHY_NOW         Highest-confidence SOURCE_CONFIRMED Git-side finding in
                this discovery (up to 3-4 redundant HTTP round trips per
                action, corrected scope per the performance review's
                MAJOR finding); independent of local-serialization work
                (Slices 1-2) and of the postponed E1/E2 topics
AGENT           ortec-abapgit-design, then ortec-abapgit-design-review
                (balanced reviewer is sufficient; escalate to
                ortec-abapgit-adversarial-design-review only if the
                design touches branch-move/staleness detection in a way
                that risks a false WARM_UNCHANGED/stale-tip verdict)
MODEL           Claude Sonnet 5 (cross-class Git protocol decision, per
                credit-aware routing rules)
THINKING        High — must prove no staleness window is introduced;
                touches branch-move detection correctness, adjacent to
                the still-open E2_CONSUMER_COHERENCE failure class
                (cross-check required per performance-review §6, without
                reopening E2)
CONTEXT         .memory/logs/abapgit_git_serialization_perf_discovery.md
                §2.2 (corrected), .memory/logs/
                abapgit_git_serialization_perf_backlog.md F-2 (corrected
                CURRENT_PATH/SCOPE), .memory/reviews/
                abapgit_git_serialization_perf_review.md Finding 1
                (exact corrected call chain: zcl_abapgit_ortec_fastpath
                lines 637-1000, not zcl_abapgit_git_transport)
INITIAL_CONTEXT ALLOWED_CONTEXT=the three files above plus
                .github/skills/git-partial-clone/SKILL.md;
                SOURCE_SCOPE=src/ortec/git/zcl_abapgit_ortec_porcelain.
                clas.abap, src/ortec/git/zcl_abapgit_ortec_fastpath.
                clas.abap (pull_by_branch, upload_pack_by_branch),
                src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap,
                src/git/zcl_abapgit_git_transport.clas.abap (branches,
                find_branch, find_branch_ortec) for read-only reference;
                FORBIDDEN_PATHS=.memory/state.md, .memory/archive/**,
                .memory/diagrams/**; STATE_WRITE_ALLOWED=no;
                DIAGRAM_WRITE_ALLOWED=no (unless owner explicitly
                requests a call-flow diagram for this specific design)
PROMPT_REQUIREMENTS Design must state: exact sharing scope (proof it
                cannot leak across two different top-level operations),
                exact call sites updated, exact staleness-detection
                behavior preserved (WARM_UNCHANGED/COLD_BRANCH/
                INCREMENTAL_UPDATE classification must remain correct),
                before/after HTTP call count for the pull-then-Diff
                scenario, and an explicit non-goal statement that this
                does not touch ZAOG_REPO_STATE/have_policy semantics
OWNER_MEASUREMENTS None required before design; a before/after HTTP call
                count via SAT trace is the acceptance measurement once
                implemented (not part of this design slice)
STOP_CONDITIONS Design cannot prove the shared tip is scoped to one
                top-level call without touching persisted repo-state
                semantics -> stop and report OWNER_DECISION_REQUIRED
                rather than widening scope into ZAOG_REPO_STATE
```

## Final response packet

See orchestrator's chat response for the compact
`TASK=ABAPGIT_GIT_SERIALIZATION_PERFORMANCE_DISCOVERY` packet required by
the task brief.
