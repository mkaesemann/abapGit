# abapGit — Git Handling and Object Serialization Performance Backlog

```text
PACKET=COMPACT_HANDOFF_V1
TASK=ABAPGIT_GIT_SERIALIZATION_PERFORMANCE_DISCOVERY
PHASE=BACKLOG (workstreams E-G)
SOURCE=.memory/logs/abapgit_git_serialization_perf_discovery.md
```

## Workstream E — measurement plan

Existing evidence (AUDIT-M-1, OS4 D1 log, Package E checkpoint 1 perf scan)
already covers Git-side pull/decode and OS4 correctness; it does not cover
the candidates below. No new instrumentation is proposed — all measurements
use existing SAT/ST05 tooling on IT8.

### M-1 — TADIR full-tree discovery cost at scale (supports F-1)
```text
SCENARIO       Stage-by-Transport with a transport containing 5-10 objects,
               against a package tree with 40,000+ total TADIR entries
SYSTEM         IT8
REPOSITORY_SIZE 40,000+ objects (OS4-class repository)
OPERATION      Open Stage page, select "Stage by Transport", pick one small
               transport
WARM/COLD      Warm (commit already indexed in ZAOG_OBJ_INDEX)
FILTER/TRANSPORT One small transport (5-10 objects)
TRACE_BOUNDARY  SAT trace bounded to zcl_abapgit_tadir=>read through
               zcl_abapgit_serialize=>add_objects return
REQUIRED_COLUMNS Gross time, DB time, and row count for the
               select_objects TADIR SELECT specifically
EXPECTED_DISCRIMINATING_RESULT TADIR SELECT elapsed time and row count
               confirm whether the O(N) discovery read is a measurable
               fraction of total Stage-by-Transport time, or negligible
               (TADIR is small-row/well-indexed) — this determines whether
               F-1 is worth implementing
MAX_OUTPUT_SIZE Compact SAT hit list (top 20 statements by time), no full
               export
```

### M-2 — External delta-base and pack-decode shape at 100k+-object scale
```text
SCENARIO       Cold branch fetch (first-ever clone) of a 100,000+ persisted-
               object repository, then an incremental pull adding ~1,000
               changed objects
SYSTEM         IT8
REPOSITORY_SIZE 100,000+ persisted Git objects
OPERATION      Branch pull (cold), then branch pull (incremental)
WARM/COLD      Both (one cold, one warm-incremental)
FILTER/TRANSPORT N/A
TRACE_BOUNDARY  SAT trace bounded to zcl_abapgit_ortec_pack_dec=>
               decode_streaming/resolve_all
REQUIRED_COLUMNS DB call count and time for the external-delta-base
               SELECT, HTTP request/byte count, total elapsed
EXPECTED_DISCRIMINATING_RESULT Confirms §2.3's static "already bulk"
               conclusion holds at production scale (no per-delta
               singleton reads appear under load); a negative result would
               be a real regression finding requiring escalation, not a
               new backlog candidate
MAX_OUTPUT_SIZE Compact SAT hit list, no full export
```

### M-3 — Local serialization peak memory/time at 40,000 objects (full,
non-filtered)
```text
SCENARIO       Full (non-filtered) local serialization of a 40,000-object
               package tree — first repository overview render after
               refresh()
SYSTEM         IT8
REPOSITORY_SIZE 40,000 objects, mixed types (CLAS/INTF/FUGR/TABL/DTEL/DDLS/
               MSAG at minimum)
OPERATION      Repository overview open (cold, mt_local empty)
WARM/COLD      Cold (mt_local empty / just refreshed)
FILTER/TRANSPORT None (full package)
TRACE_BOUNDARY  SAT trace + memory snapshot bounded to
               zcl_abapgit_serialize=>serialize (parallel and sequential
               runs, separately)
REQUIRED_COLUMNS Peak private/session memory during serialize(), gross
               elapsed time, per-object-type time share (CLAS/INTF vs.
               FUGR vs. TABL/DTEL vs. MSAG vs. remainder)
EXPECTED_DISCRIMINATING_RESULT Identifies which object-type family
               dominates cost at this scale and whether the existing
               ser_pref/_ext/_oo prefetch coverage matches the actual
               cost distribution (if a high-cost type has no prefetch
               class, that is a new, evidence-backed SERIALIZER-PREFETCH
               candidate — not proposed here without this data)
MAX_OUTPUT_SIZE Compact SAT hit list (top 30 statements/types by time),
               single memory snapshot value, no full trace export
```

### M-4 — Does post-commit navigation call `refresh()`?
```text
SCENARIO       Commit a small change set (5-10 objects) and observe the
               very next repository-overview render
SYSTEM         IT8
REPOSITORY_SIZE Any (40,000-object repository preferred to make the effect
               visible if present)
OPERATION      Push/commit from Stage page, then automatic return to
               repository overview
WARM/COLD      Warm (mt_local/mt_remote already populated from the Stage
               page that preceded the commit)
FILTER/TRANSPORT N/A
TRACE_BOUNDARY  SAT trace bounded to the commit-success event handler
               through the next zcl_abapgit_repo_status=>calculate call
REQUIRED_COLUMNS Whether zif_abapgit_repo~refresh is called in this window;
               if yes, whether the subsequent get_files_local() call count
               of serialized objects equals the FULL package (N) or only
               the just-committed subset (K)
EXPECTED_DISCRIMINATING_RESULT Directly confirms or refutes the mandatory
               challenge question "does post-commit navigation trigger a
               redundant full refresh?" — this is a pure fact-finding
               measurement, not yet backed by a located call site (see
               discovery §1)
MAX_OUTPUT_SIZE Compact SAT hit list plus a single call-count fact, no full
               export
```

## Workstream F — candidates

### F-1: Filter transport/package/object-name entries into the TADIR
discovery SELECT
```text
ID                  F-1
CATEGORY            LOCAL-OBJECT-DISCOVERY
TITLE               Scope the TADIR discovery SELECT to the filter's own
                    object set for filtered callers, instead of the whole
                    package tree
CURRENT_PATH        zif_abapgit_tadir~read -> build -> select_objects
                    (src/objects/core/zcl_abapgit_tadir.clas.abap:271-330);
                    it_filter applied only AFTER this SELECT (lines
                    499-513)
EVIDENCE            SOURCE_CONFIRMED (discovery §3.1) — no measurement yet
ROOT_CAUSE          Filter parameter exists on the method signature but is
                    applied as a post-SELECT in-memory DELETE, not as a
                    WHERE-clause restriction or a set-based semi-join
SCALE_VARIABLE      N (whole package/subpackage tree), should be K
                    (filter's own object set) for filtered callers
CURRENT_COMPLEXITY  O(N) SELECT + O(N log N) in-memory filter delete
TARGET_COMPLEXITY   O(K) SELECT (e.g. one additional FOR ALL ENTRIES
                    restricting object/obj_name to the filter set) when
                    it_filter is supplied and non-trivial in size;
                    unchanged O(N) behavior when it_filter is empty (full
                    serialization callers)
EXPECTED_BENEFIT    Reduces TADIR SQL row volume for Stage-by-Transport/
                    single-object Diff/patch on very large package trees;
                    magnitude UNKNOWN pending M-1 (TADIR is well-indexed,
                    benefit may be small in absolute terms)
CORRECTNESS_RISK    LOW-MEDIUM — must preserve exact existing semantics
                    (subpackage inclusion, delflag/srcsystem ranges,
                    exclude-list) for the filtered subset; must not change
                    behavior for the unfiltered (full) caller at all
MEMORY_RISK         LOW — TADIR rows are metadata-only regardless
PERSISTENCE_OR_PROTOCOL_IMPACT NONE
SCOPE               zcl_abapgit_tadir.clas.abap only (select_objects/read),
                    no DDIC change
PREREQUISITES       M-1 measurement to confirm the SELECT is a measurable
                    cost share before spending implementation/review effort
EXACT_MEASUREMENT   M-1
ACCEPTANCE          Existing zcl_abapgit_tadir unit tests unchanged/still
                    pass; a new test confirms identical result set (order
                    and content) for a filtered call before/after; M-1
                    re-run shows reduced TADIR SELECT row count/time for
                    the filtered scenario
REJECT_IF           M-1 shows the existing SELECT is already sub-second/
                    negligible relative to total Stage-by-Transport time
                    at 40,000+ objects
OWNER_PACKAGE       Package F (validated legacy-code/perf cleanup) or a
                    new small standalone slice — NOT part of Package
                    E (E1/E2 are postponed and unrelated to this file)
```

### F-2: Reuse a single resolved branch tip within one user action
```text
ID                  F-2
CATEGORY            GIT-PROTOCOL
TITLE               Avoid a second/third independent info/refs GET for the
                    same repository within one user action
CURRENT_PATH        zcl_abapgit_ortec_porcelain=>pull_by_branch (own
                    branches() call, "disclosed-but-accepted redundant"
                    per its own comment, line 191) + zcl_abapgit_ortec_
                    fastpath's OWN pull_by_branch (line 637, its own
                    branches() call at line 673) and upload_pack_by_branch
                    (line 886, find_branch_ortec calls for thin-pack
                    attempt + self-contained retry) + zcl_abapgit_ortec_
                    filter_walk=>get_remote_files_for_stage's independent
                    branches() call. CORRECTED per performance-review
                    Finding 1: the standard zcl_abapgit_git_transport=>
                    upload_pack_by_branch's own find_branch fallthrough is
                    UNREACHABLE for ORTEC-active repos (its TRY around
                    zcl_abapgit_ortec_fastpath=>upload_pack_by_branch
                    always RETURNs or RAISEs, never falls through,
                    zcl_abapgit_git_transport.clas.abap:412-446) — the
                    original citation of that class was wrong; the real
                    mechanism is entirely inside zcl_abapgit_ortec_fastpath
EVIDENCE            SOURCE_CONFIRMED (discovery §2.2)
ROOT_CAUSE          No request-scoped (single user action) cache for
                    resolved branch-list/tip data; each consumer resolves
                    independently
SCALE_VARIABLE      Not N/K-scoped — a fixed per-action HTTP round-trip
                    count (1 -> up to 3), independent of repository size,
                    but each round trip's own payload (info/refs
                    advertisement) DOES grow with ref count
CURRENT_COMPLEXITY  Up to 3 HTTP round trips per user action for the same
                    repository/branch
TARGET_COMPLEXITY   1 HTTP round trip per user action, shared via an
                    explicit request-scoped parameter or a very
                    short-lived (single top-level call), explicitly
                    invalidated cache — NOT a persistent/cross-request
                    cache
EXPECTED_BENEFIT    One fewer HTTP round trip per Diff/Stage-by-Transport
                    action following a pull; magnitude scales with network
                    latency and ref-advertisement size, not repository
                    object count — UNKNOWN absolute value, no trace yet
CORRECTNESS_RISK    MEDIUM — must not reintroduce a stale-tip risk (the
                    entire point of the independent re-checks being
                    removed is to detect a moved branch); any shared value
                    must be scoped to a single top-level operation, never
                    cached across operations/requests
MEMORY_RISK         LOW
PERSISTENCE_OR_PROTOCOL_IMPACT NONE if scoped to one call stack (no new
                    persisted state); must not be confused with or
                    implemented via ZAOG_REPO_STATE (that already has its
                    own certified-have semantics, see have_policy)
SCOPE               zcl_abapgit_ortec_porcelain, zcl_abapgit_ortec_
                    fastpath (both its own pull_by_branch and
                    upload_pack_by_branch methods — corrected per
                    performance-review Finding 1, NOT the standard
                    zcl_abapgit_git_transport=>upload_pack_by_branch,
                    whose find_branch fallthrough is unreachable for
                    ORTEC-active repos), and zcl_abapgit_ortec_filter_walk
                    — a genuine cross-class design, not a mechanical edit
PREREQUISITES       A design that explicitly defines the sharing scope
                    (single top-level call only) and proves no staleness
                    window is introduced; correctness review required
                    before implementation given the branch-move detection
                    role of these calls. Since this candidate sits
                    adjacent to the still-open E2_CONSUMER_COHERENCE class
                    of stale-remote-state bugs, its design must explicitly
                    cross-check against that failure mode once E2 resumes
                    (performance-review observation, §6) \u2014 this does not
                    reopen E2, it is a forward-looking design constraint
EXACT_MEASUREMENT   None new required beyond confirming HTTP call count via
                    existing SAT trace during any Diff-after-pull scenario
ACCEPTANCE          HTTP call count for one Diff-after-pull user action
                    drops from up to 3 to 1; existing branch-move/cold-
                    branch/warm-unchanged classification tests still pass
                    unchanged
REJECT_IF           Design review finds no safe way to share the tip
                    within one call stack without weakening branch-move
                    detection
OWNER_PACKAGE       DESIGN_REQUIRED — new slice, cross-references but does
                    not reopen E2_CONSUMER_COHERENCE
```

### F-3: Cross-commit reuse of `ZAOG_OBJ_INDEX` rows (already tracked,
cited not proposed as new)
```text
ID                  F-3
CATEGORY            TREE-WALK-INDEX
TITLE               Reuse ZAOG_OBJ_INDEX rows across sibling/parent commits
                    with a shared/near-identical tree
CURRENT_PATH        zcl_abapgit_ortec_obj_index=>rebuild_index (discovery
                    §2.4)
EVIDENCE            SOURCE_CONFIRMED (discovery §2.4); design constraint
                    already established in .memory/state.md
                    (E1-TREE-REUSE: "PROVEN UNSAFE as a bare tree-SHA1 key
                    — any future attempt needs a composite key of at
                    minimum (tree_sha1, hash-of-.abapgit-content,
                    devclass)")
ROOT_CAUSE          Every new commit SHA1 rebuilds its full filtered index
                    from scratch even when its tree is 95%+ identical to
                    an already-indexed sibling/parent commit
SCALE_VARIABLE      N per new commit (should approach K = changed-file
                    count for incremental commits)
CURRENT_COMPLEXITY  O(N) full tree walk + classify + hash, once per new
                    commit SHA1
TARGET_COMPLEXITY   O(K) incremental diff-based index update — NOT
                    designed in this discovery
EXPECTED_BENEFIT    Would materially reduce first-touch latency for
                    Diff/Stage-by-Transport on a freshly pulled commit in
                    an actively-developed large repository; magnitude
                    UNKNOWN, no measurement performed (design does not
                    exist yet)
CORRECTNESS_RISK    HIGH per existing owner-approved design constraint — a
                    bare tree-SHA1 key was already proven unsafe
PERSISTENCE_OR_PROTOCOL_IMPACT DDIC change required (composite key) per
                    existing design constraint
SCOPE               Not scoped — DESIGN_REQUIRED, full design + adversarial
                    review needed before any implementation slice exists
PREREQUISITES       Owner approves a dedicated design + performance
                    DESIGN_GATE per the existing entry condition already
                    recorded in .memory/state.md
EXACT_MEASUREMENT   A repeat-commit-frequency measurement on a real large
                    repository (how often are near-identical trees
                    actually indexed from scratch) would justify priority,
                    not performed this pass
ACCEPTANCE          N/A — no implementation authorized
REJECT_IF           Owner does not prioritize E1-TREE-REUSE
OWNER_PACKAGE       E1-TREE-REUSE (existing deferred topic, unchanged
                    entry condition) — listed here ONLY for backlog
                    completeness and ranking; NOT a new candidate
```

### F-4: Incremental (K-sized) local re-serialization after `refresh()`
```text
ID                  F-4
CATEGORY            LOCAL-SERIALIZATION
TITLE               Avoid forcing a full (N-sized) local re-serialization
                    on every refresh() call when only a known, small
                    object set changed
CURRENT_PATH        zif_abapgit_repo~refresh (src/repo/zcl_abapgit_repo.
                    clas.abap:796-810) unconditionally sets
                    mv_request_local_refresh = abap_true; zif_abapgit_
                    repo~get_files_local's cache short-circuit is bypassed
                    whenever this flag is set
EVIDENCE            SOURCE_CONFIRMED (discovery §3.3); the specific
                    triggering UI path (post-commit navigation) is UNKNOWN
                    pending M-4
ROOT_CAUSE          refresh() has one binary mode (full invalidate); there
                    is no "invalidate just these K objects" local-cache
                    update path
SCALE_VARIABLE      N (whole package), should be K (objects changed by the
                    operation that called refresh()) for the common case
                    of a small commit/pull
CURRENT_COMPLEXITY  O(N) full re-serialization on next get_files_local()
TARGET_COMPLEXITY   O(K) targeted re-serialization + in-place merge into
                    mt_local for the changed object set only, when the
                    caller can supply that set (e.g. post-commit: exactly
                    the objects just staged/committed)
EXPECTED_BENEFIT    UNKNOWN magnitude pending M-3/M-4; potentially the
                    highest-value LOCAL-SERIALIZATION candidate in this
                    backlog if M-4 confirms this fires on routine
                    post-commit navigation of large repositories, since
                    §3.3 already proves the full-N behavior exists
CORRECTNESS_RISK    MEDIUM-HIGH — mt_local is consumed by status
                    calculation, checksums, and multiple UI pages; a
                    partial-merge cache update must not leave stale rows
                    for objects that were deleted, renamed, or moved
                    between packages/generated-include boundaries
MEMORY_RISK         LOW (reduces memory versus current full-N behavior)
PERSISTENCE_OR_PROTOCOL_IMPACT NONE (in-memory repository instance cache
                    only, zif_abapgit_repo~refresh_local_object/
                    refresh_local_objects already exist as a narrower
                    primitive and were not investigated as a possible
                    existing solution this pass — see prerequisite)
SCOPE               zcl_abapgit_repo (refresh/get_files_local family);
                    cross-references zif_abapgit_repo~refresh_local_object/
                    refresh_local_objects which may already partially
                    solve this and were NOT read this pass
PREREQUISITES       M-4 to confirm the trigger path and object-count
                    delta; a read of the EXISTING
                    refresh_local_object(s) methods (not investigated this
                    pass) to check whether this capability already exists
                    and is simply unused by the post-commit path — this
                    could turn F-4 into a wiring fix rather than a new
                    mechanism
EXACT_MEASUREMENT   M-3, M-4
ACCEPTANCE          Post-commit navigation re-serializes only the
                    committed object set, not the whole package;
                    zcl_abapgit_repo_status=>calculate results are
                    identical to the full-refresh baseline for an
                    A/B-compared scenario
REJECT_IF           M-4 shows refresh() is not actually invoked on the
                    post-commit path (i.e. the concern is theoretical, not
                    real, for that specific trigger) AND no other frequent
                    trigger is found
OWNER_PACKAGE       DESIGN_REQUIRED — new slice; investigate existing
                    refresh_local_object(s) methods FIRST before any new
                    design work
```

### F-5: `TR_CHECK_TYPE` per-item loop in bulk transport determination
```text
ID                  F-5
CATEGORY            LOCAL-SERIALIZATION
TITLE               Replace the per-repository-item TR_CHECK_TYPE function
                    call with a bulk/cached lockable-type classification
CURRENT_PATH        zcl_abapgit_ortec_cts_buffer=>determine_transports_bulk
                    (src/ortec/zcl_abapgit_ortec_cts_buffer.clas.abap,
                    STEP 1/STEP 2 loops)
EVIDENCE            SOURCE_CONFIRMED (discovery §3.4)
ROOT_CAUSE          TR_CHECK_TYPE is called once per repository item to
                    classify lockable/transportable status; the result is
                    a pure function of object TYPE, not of the specific
                    object instance, so it is called far more often than
                    necessary (once per DISTINCT object type would
                    suffice)
SCALE_VARIABLE      N (all repository items passed to this method)
CURRENT_COMPLEXITY  O(N) function-module calls (no DB/HTTP cost per call)
TARGET_COMPLEXITY   O(distinct object types) — typically under 50 for any
                    real repository — with a local lookup table keyed by
                    object type
EXPECTED_BENEFIT    LOW-MEDIUM — function-module call overhead is local
                    (no DB/HTTP round trip), so absolute benefit is likely
                    small even at N=40,000; included for completeness, not
                    because it is expected to be high-value
CORRECTNESS_RISK    LOW — TR_CHECK_TYPE's classification result for a
                    given object TYPE is stable within one call; caching
                    by type only (not by type+name) is safe
MEMORY_RISK         NONE
PERSISTENCE_OR_PROTOCOL_IMPACT NONE
SCOPE               zcl_abapgit_ortec_cts_buffer.clas.abap only
PREREQUISITES       None
EXACT_MEASUREMENT   None dedicated; would show up as a small time
                    reduction in M-3's repository-overview trace if
                    re-run before/after
ACCEPTANCE          Identical lock-key/transport results for a
                    representative repository, function-module call count
                    reduced to distinct-object-type count
REJECT_IF           M-3 (or any future trace) shows this call is not a
                    measurable time contributor at any tested scale
OWNER_PACKAGE       Package F (validated legacy-code/perf cleanup) —
                    lowest priority in this backlog
```

## Workstream G — ranking

```text
DIMENSION                F-1        F-2        F-3        F-4        F-5
Measured cost share       UNKNOWN    UNKNOWN    UNKNOWN    UNKNOWN    UNKNOWN
                          (M-1)      (n/a, HTTP (n/a)      (M-3/M-4)  (n/a)
                                     count is
                                     source-
                                     confirmed,
                                     not timed)
Frequency of operation    HIGH       HIGH       MEDIUM     HIGH (IF   MEDIUM
                          (every     (every     (only new  M-4        (every
                          filtered   Diff/Stage commits,   confirms)  serializ-
                          call)      after pull) not warm             ation)
                          repeat)
Affected repo sizes       Large      All        Large,     Large      Large
                          package                actively
                          trees                  developed
Expected absolute time    LOW-MED    LOW-MED    MEDIUM-    UNKNOWN,   LOW
  reduction                (UNKNOWN)  (1 fewer   HIGH       potentially
                                      HTTP RT)   (UNKNOWN)  HIGH
Expected memory reduction NONE       NONE       NONE       MEDIUM     NONE
Implementation effort     LOW        MEDIUM     HIGH       MEDIUM-    LOW
                                     (cross-               HIGH
                                     class)
Correctness risk          LOW-MED    MEDIUM     HIGH       MEDIUM-    LOW
                                                            HIGH
Regression surface        SMALL      MEDIUM     LARGE      LARGE      SMALL
Need for DDIC/migration   NO         NO         YES        NO         NO
Observability/validation  GOOD       GOOD       NEEDS      NEEDS      GOOD
  quality                 (M-1)      (HTTP      NEW        M-3/M-4
                                     count is   DESIGN
                                     directly
                                     observable)

DECISION                  MEASURE_   DESIGN_    DEFER      MEASURE_   MEASURE_
                           FIRST      REQUIRED   (existing  FIRST      FIRST
                                                 topic,
                                                 unchanged)
```

### 1. Low-risk immediate improvements
- None are ready for `IMPLEMENT_NEXT` without the measurements below —
  every candidate in this backlog has at least one open cardinality/
  cost-share question. This is a deliberate outcome of the evidence
  discipline (no candidate whose benefit cannot be distinguished from
  measurement noise), not an oversight.
- Closest to low-risk-and-immediate once measured: **F-5** (mechanical,
  small, no correctness risk) and **F-1** (small, well-isolated, no DDIC).

### 2. High-value designs requiring gates
- **F-2** (redundant branch-tip resolution) — cross-class, touches
  branch-move detection correctness; requires design + correctness
  review before any implementation, per its own risk profile.
- **F-4** (incremental local re-serialization after refresh()) —
  potentially the highest-value single item in this backlog IF M-4
  confirms the post-commit trigger, but requires investigating the
  existing (unread this pass) `refresh_local_object(s)` methods first,
  then a full design + correctness review given `mt_local`'s broad
  consumer surface.

### 3. Ideas rejected or deferred, with reasons
- **F-3** (cross-commit `ZAOG_OBJ_INDEX` reuse) — **DEFER**, not because
  it lacks value but because it duplicates the already-tracked
  `E1-TREE-REUSE` deferred topic with an owner-set entry condition
  (dedicated design + performance DESIGN_GATE) that has not been met.
  Not re-opened by this discovery.
- Any candidate touching `E1_OBJINDEX_PERFORMANCE` (5000-vs-30000 chunk
  discrepancy) or `E2_CONSUMER_COHERENCE` (OS4 false-MODIFIED root
  cause/parallel-worker stale cache) — **REJECT for this backlog**, both
  are explicit owner-postponed topics as of 2026-07-31; this discovery
  cites them as evidence only and proposes no action on them.
- A persistent local-serialization snapshot (the eventual, out-of-scope
  goal named in the task brief) — **DEFER**, per the discovery's answer
  to the final mandatory challenge question: existing, cheaper reductions
  (F-1, F-2, F-4) have not yet been measured or exhausted, so a new
  persistent-snapshot layer cannot yet be justified as necessary.
