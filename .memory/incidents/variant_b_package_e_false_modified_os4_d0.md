# Variant B / Package E — False Local+Remote MODIFIED — D0 Reproduction (OS4, LARGE REPO)

```text
STATUS=ROOT_CAUSE_B_FOUND_CHECKSUM_BASELINE_NEAR_EMPTY_FIX_PENDING_AUTHORIZATION
E2_FIX_AUTHORIZED=YES (owner, explicit)
MAJOR_PIVOT_FINDING_B_CONFIRMED=Queried the persisted checksum baseline
  directly (table ZABAPGIT, type=REPO_CS, key=000000000002 for repo
  "OS4 6.0"): it contains ONLY 3 entries (.abapgit.xml, /LOT/OS package
  devc.xml, /LOT/ nspc.xml) -- NOT ONE of the ~17000+ actual code objects
  has a baseline entry. Repo was created 2026-05-06 (New Online clone of
  an existing large remote) and the baseline was apparently never
  populated with the full file list after the initial clone.
  ZCL_ABAPGIT_STATUS_CALC~BUILD_EXISTING's fallback for "file not found
  in baseline" is to blanket-mark BOTH lstate and rstate as MODIFIED
  whenever local sha1 <> remote sha1 for ANY reason (source comment even
  says "this is a strange situation... maybe first run"). This explains
  why the MM symptom is so widespread and never self-heals across
  refreshes. DECISIVE PROOF: clicked the per-file "diff" for GC_GEOLON
  while it showed MM -- result: "There are no differences to show. The
  local state completely matches the remote repository." Content is
  byte-identical; the bug is 100% in status/baseline logic, not
  serialization content. Standard abapGit provides a documented
  self-healing method for exactly this, ZIF_ABAPGIT_REPO_CHECKSUMS~REBUILD,
  which repopulates the baseline from current local state -- NOT yet
  invoked, needs owner awareness since it adopts current local as truth
  for ALL fields (could mask genuine unreviewed local changes if any
  exist). Full details in D1 log §24.
BUG_A_PARALLEL_SHA1_DEFECT_STATUS=Still separately confirmed (reproduced
  twice) but NOT the primary/sole explanation anymore -- now understood
  as one possible TRIGGER for a mismatch that the missing baseline turns
  into a permanent, blanket MM instead of a self-correcting one-sided M.
  Byte-level root cause of the SHA1 defect itself remains unresolved.
FIX_1_FUGR_INCLUDES=PARTIAL SUCCESS after owner restart: GC_ADDRESS_GEOCODE's
  own includes fixed by with_inactive_incls=abap_true, but OTHER function
  groups' not-yet-generated includes still wrong. Owner suggested all 4
  RS_GET_ALL_INCLUDES params = abap_true; IMPLEMENTED, activated, awaiting
  another restart to test. Owner: deprioritize if this doesn't help either
  (dev-system-only edge case).
FIX_2_PARALLEL_STALE_CACHE=CONFIRMED WORKING AS DESIGNED (live-verified:
  mt_dtel/mt_dokil hold exactly 1/2 rows respectively -- only the current
  object's data, no accumulation) but CONFIRMED NOT SUFFICIENT to fix the
  MODIFIED/MODIFIED symptom -- owner reports it recurs (GC_GEOLON) even
  after a full restart (rules out pre-fix stale-session explanation).
  NEW LIVE FINDING: the parallel worker's OWN computed SHA1 for GEOLON
  (55f8cfe1eb28ffe5b074e39452129a6320a9f333) does NOT match the known-good
  remote blob (0a8df7887b9decf9d7e79f89e53df651cd6a6d9d), PROVING the
  corruption happens INSIDE the worker's serialize() call -- yet EVERY
  individually-inspected content component (DD04V core fields, i18n
  texts, DOKIL longtexts) is independently verified CORRECT for this
  object. True root cause NOT yet isolated -- likely in ls_extra/abap
  language version, DD04V post-processing, or the XML
  assembly/hashing mechanism itself (none yet instrumented).
TOOL_LESSON=abap_debug_variable table-row access DOES work with bracket
  index syntax WITHOUT spaces (e.g. tab[1]-field), unlike the previously
  documented tab[ 1 ]-field (with spaces) which fails. Revisit old
  "can't expand table rows" limitation notes with this corrected syntax.
D0_REPRODUCTION=CONFIRMED via a fast, owner-discovered repro: repo menu ->
  "Stage By Transport" -> transport IT8K900025 (filters to /LOT/GC package,
  236 objects, ~4-20s instead of the full 17321-object/15+min Overview or
  Full Stage). Root cause CONFIRMED via live breakpoint capture through
  ZCL_ABAPGIT_STATUS_CALC=>build_existing's actual branch execution (not
  circumstantial): (A) is_local-file-sha1 (46e7ae23ace9830328dd0e7c063e31f671c98f56)
  genuinely differs from is_remote-sha1 (f1f852820cdd36e58845d424bfa3e28876b11d38,
  matching the known-good OBJ_STORE blob) for /LOT/GC_GEOLAT in the
  bulk/filtered local-serialize code path -- **owner-confirmed via a live
  experiment this session: disabling PARALLEL serialization (sequential
  mode) makes this symptom DISAPPEAR**, decisively narrowing the defect to
  the parallel serialization code path (renamed PC-3; PC-1 stays
  REJECTED). (B) The persisted checksum baseline (ZCL_ABAPGIT_REPO_CHECKSUMS,
  whole-repo, not filter-scoped) has only 3 total file entries for this
  entire repo, so the `READ TABLE ... BINARY SEARCH` lookup fails
  (sy-subrc=8) for virtually every file, triggering build_existing's
  documented "strange situation" fallback that UNCONDITIONALLY sets BOTH
  lstate='M' AND rstate='M' -- confirmed via direct variable capture
  immediately after assignment, and owner-confirmed this session as a
  SEPARATE, STILL-PRESENT bug (some files still show M/M "even though only
  modified in local or remote" even with parallel OFF). PC-1 (stale
  standard mt_remote cache) is REJECTED: Stage By Transport uses the SAME
  ORTEC facade/OBJ_INDEX fast path as Diff (confirmed live: BP5a/b fired,
  BP8 fallback-catch did NOT fire, no silent fallback), yet still
  reproduces the bug -- the standard-cache-vs-ORTEC asymmetry cannot be the
  mechanism. Full detail: .memory/logs/variant_b_package_e_false_modified_os4_d1.md
  §14, §18.
ABAP_FS_MCP_TOOLS_USED=get_connected_systems, get_sap_system_info,
  abap_debug_session (start/status/stop), abap_debug_breakpoint (set/remove),
  abap_debug_stack, abap_debug_variable, abap_debug_step, get_abap_object_info,
  get_abap_object_lines, search_abap_object_lines, search_abap_objects,
  find_where_used, get_abap_object_workspace_uri, execute_data_query,
  get_abap_sql_syntax, plus browser automation (click_element/read_page/
  screenshot_page/type_in_page/run_playwright_code) against an
  owner-shared, already-authenticated WebGUI tab
BASELINE=CURRENT_HEAD=8b382a5eab948f67a9ca6239293e193b70d8d38c (working tree
  clean except this session's own `.memory/*` edits — verified via `git
  status --short`, re-confirmed this session)
E1_PERF_A_STATUS=IMPLEMENTED at this HEAD (`c_index_write_chunk_size TYPE i
  VALUE 30000` in `zcl_abapgit_ortec_obj_index.clas.abap` line 75, used only
  as an INSERT/MODIFY chunk-size trigger at line 456 inside `rebuild_index`'s
  write loop). Confirmed independent of E2: it changes only how many rows are
  written per bulk DB call during an index rebuild — it does not change which
  rows are written, the index's lookup keys, the READY (`R`) status flag, the
  commit/branch resolution logic, or any status-comparison semantics. Note:
  the implemented value (30000) differs from `.memory/state.md`'s recorded
  design contract (5000) — flagged as a documentation inconsistency for
  Package E bookkeeping, NOT an E2-relevant behavior difference (out of
  scope for this incident; no action taken here).
SEPARATE_BACKLOG_ITEMS (NOT E2)=(1) owner observed a `SYSTEM_NO_ROLL`
  runtime dump in IT8 when Full Stage is triggered right after a full
  Overview serialize of this large repo -- recorded as a new Package E
  backlog item, NOT investigated as part of E2. (2) FG /LOT/GC_ADDRESS_GEOCODE
  is falsely reported by the owner as "does not exist locally at all" --
  LIVE-CONFIRMED ROOT CAUSE, NOT an ORTEC/abapGit bug: the standard SAP
  kernel function RS_GET_ALL_INCLUDES(program='/LOT/SAPLGC_ADDRESS_GEOCODE')
  returns sy-subrc=0 (success) but ZERO includes, BEFORE any abapGit
  subtraction/filtering logic runs. abapGit's own code (functions(),
  main_name(), the ORTEC prefetch buffer) was verified clean and correct.
  Needs owner-side standalone verification of RS_GET_ALL_INCLUDES for this
  program outside abapGit to determine if this is a genuine SAP-side data/
  registration issue for this one program (likely out of scope for E2) or
  session-state interference (not yet identified). See D1 §19, §21.
FOLLOW_UP_QUESTIONS (not fabricated answers)=why does the persisted
  checksum baseline only have 3 entries for this large repo (never
  successfully Add+Commit'd through this checksum mechanism, or
  cleared/reset at some point)? why does the bulk/filtered local serialize
  produce a different SHA1 than Diff's own local-content resolution for
  this DTEL?
E2_FIX_AUTHORIZED=NO (root cause confirmed; no fix designed or applied)
```

## 1. Repository identity (sanitized — no credentials/full URL persisted)

Queried read-only via `mcp_arc-12_SAPQuery` against `ZABAPGIT` (repo entity,
already known from the prior, now-superseded small-repo session) and
`ZAOG_REPO_STATE`/`ZAOG_OBJ_INDEX`/`ZAOG_OBJ_STORE` (ORTEC persistence),
bounded to exact keys only (no repo-wide or unbounded scans):

```text
ZABAPGIT.VALUE (repo GUID)  = 000000000002
DISPLAY_NAME                = OS4 6.0
PACKAGE                     = /LOT/OS
ORTEC REPO_KEY               = 288c81fc1cad   (derived deterministically from
                                the repo's remote URL by
                                zcl_abapgit_ortec_repo_state=>get_repo_key_for_url;
                                confirmed present in ZAOG_REPO_STATE)
```

`ZAOG_REPO_STATE` rows for this repo_key (2 branches tracked):

| BRANCH_NAME | CURR_COMMIT | FETCH_COMMIT | FETCH_TS (UTC) | IS_SHALLOW | DEEPEN_LVL |
|---|---|---|---|---|---|
| refs/heads/releases/6.0.1 | 54b5f5711a0de43903ed8421bb11ce37c411436f | 54b5f571... (same) | 2026-07-30 09:06:01.75 | (blank) | 0 |
| refs/heads/development/6.0.x | 81157b1448b4183f38403ec63caad1291a2226a4 | 81157b14... (same) | 2026-07-30 09:19:48.88 | X | 0 |

The persisted `ZABAPGIT` repo entity's `BRANCH_NAME` field
(`refs/heads/development/6.0.x`, from the prior session's query) matches the
SECOND row above — this is the currently SELECTED branch, tip
`81157b1448b4183f38403ec63caad1291a2226a4`, fetched TODAY (2026-07-30,
09:19:48 UTC), shallow. `ZAOG_COMMIT_HIST` confirms `HIST_LEVEL=F` (full
complete) for both commits — snapshot completeness is not in question for
either commit.

## 2. Reported symptom (owner description, exact object)

```text
OBJ_TYPE       = DTEL
OBJ_NAME       = /LOT/GC_GEOLAT
REMOTE_PATH    = /src/#lot#gc/#lot#gc_geolat.dtel.xml
OVERVIEW_LSTATE/RSTATE = Local MODIFIED + Remote MODIFIED (M M)
FULL_STAGE             = same M M pair reported for this object
DIFF_ACTION            = "There are no differences to show; local state
                          completely matches remote repository."
  (this exact string is the literal exception text raised by
  zcl_abapgit_gui_page_diff_base's constructor, see D1 §1 below — confirms
  the owner saw the real "empty diff" code path, not a rendering glitch)
SESSION_DISCIPLINE     = owner reports no Pull/Refresh/branch-switch/
                          checksum-update/Cache-Admin action was taken
                          between the overview render, Full Stage render,
                          and the Diff action (required for this D0 to be
                          meaningful — any such action would invalidate the
                          comparison)
```

## 3. Object-store / index evidence for the exact path (bounded query, exact key only)

`ZAOG_OBJ_INDEX` — exactly one row exists for this object at the current
commit (no duplicate/competing row for a different commit was found for this
`obj_name`):

```text
REPO_KEY     = 288c81fc1cad
COMMIT_SHA1  = 81157b1448b4183f38403ec63caad1291a2226a4
OBJ_TYPE     = DTEL
OBJ_NAME     = /LOT/GC_GEOLAT
FILE_PATH    = /src/#lot#gc/
FILE_NAME    = #lot#gc_geolat.dtel.xml
BLOB_SHA1    = f1f852820cdd36e58845d424bfa3e28876b11d38
TREE_SHA1    = 0f391f346567eb7d9c48846e57ccbd9220b88b3f
IDX_STATUS   = R (ready)
```

`ZAOG_OBJ_STORE` confirms this blob SHA1 exists and is well-formed:

```text
REPO_KEY = 288c81fc1cad
OBJ_SHA1 = f1f852820cdd36e58845d424bfa3e28876b11d38
OBJ_TYPE = blob
OBJ_SIZE = 3777 bytes
```

This `BLOB_SHA1` is the value the ORTEC filtered-walk/OBJ_INDEX path would
resolve as "remote" for this file **if and only if** the Diff action's live
staleness re-check (D1 §2) accepts `81157b14...` as the current commit at
the moment the Diff action runs. It is recorded here as a reference value for
the owner's live capture to compare against, **not** as a substitute for
capturing the actual live `LOCAL_SHA1`/checksum-baseline values, which
require the debugger (see D1 worksheet).

## 3b. Live re-verification this session (ABAP-FS MCP, read-only, unchanged)

Re-queried `ZAOG_REPO_STATE`, `ZAOG_OBJ_INDEX`, `ZAOG_COMMIT_HIST` directly
via `execute_data_query` (bounded to `repo_key=288c81fc1cad` /
`obj_name=/LOT/GC_GEOLAT` — no repo-wide scan). All three tables are
byte-identical to the values already recorded in §1-§3 above (both branch
rows, the single `ZAOG_OBJ_INDEX` row, `HIST_LEVEL=F` for both commits) — no
drift since this packet was first written. `c_index_write_chunk_size = 30000`
in `ZCL_ABAPGIT_ORTEC_OBJ_INDEX` line 75 re-confirmed via direct active-source
read (line 456 is the consuming `IF lines( lt_rows ) >= c_index_write_chunk_size`
check) — independent of E2, per D0 §0 above (unchanged verdict).

All 9 classes named in the debugging prompt's pre-flight check
(`ZCL_ABAPGIT_REPO_ONLINE`, `ZCL_ABAPGIT_REPO`, `ZCL_ABAPGIT_REPO_STATUS`,
`ZCL_ABAPGIT_STATUS_CALC`, `ZCL_ABAPGIT_REPO_CHECKSUMS`,
`ZCL_ABAPGIT_REPO_CONTENT_LIST`, `ZCL_ABAPGIT_ORTEC_FILTER_WALK`,
`ZCL_ABAPGIT_ORTEC_OBJ_INDEX`, `ZCL_ABAPGIT_GUI_PAGE_DIFF_BASE`) confirmed
readable via `get_abap_object_info` this session.

## 4. NOT_CAPTURED — requires owner live debugger session (see D1 worksheet)

The following fields are REQUIRED for a complete D0 packet per the
diagnostic-ladder standard, but cannot be obtained without a live, single,
uninterrupted debugger session driven by the owner (I have no interactive
SAP GUI/debugger tool access). Each is explicitly marked `NOT_CAPTURED`
A debug session is already armed (ABAP-FS MCP, `Mode: User`, 10 breakpoints
set across 7 classes — exact locations in the handoff). The only remaining
action is for the owner to trigger, in one uninterrupted sequence in their
already-authenticated IT8 SAP GUI session, using the SAME SAP user this
ADT/debug connection is logged in as: (1) Repository Overview for OS4 6.0,
(2) Full Stage, (3) Diff for DTEL `/LOT/GC_GEOLAT` — no Pull/Refresh/
branch-switch/checksum-update/Cache-Admin action in between. No fix is
authorized until this D0 packet is complete and a live mismatch is
confirmed:

STAGE-BY-TRANSPORT (live-captured this session, see D1 §14 for full detail):
  LOCAL SHA1 for this file (bulk/filtered serialize)   = 46e7ae23ace9830328dd0e7c063e31f671c98f56
  REMOTE SHA1 for this file (ORTEC OBJ_INDEX fast path) = f1f852820cdd36e58845d424bfa3e28876b11d38 (matches known-good OBJ_STORE blob)
  Persisted checksum-baseline lookup (READ TABLE ... BINARY SEARCH)  = sy-subrc=8 (FAILED — baseline table only has 3 total rows for this repo)
  Resulting rs_result-lstate / rs_result-rstate         = M / M (both, confirmed by direct variable read)
  ROOT CAUSE MECHANISM = CONFIRMED (see D1 §14): local/remote SHA1 mismatch
    (cause of the mismatch itself NOT yet found) + near-empty persisted
    checksum baseline causes the mismatch to render as blanket "both
    modified" instead of a nuanced single-side state.
OVERVIEW/FULL STAGE (unfiltered path): NOT separately live-captured with
  confirmed values this session (the debug hook died silently during two
  earlier attempts — see D1 §8/§13/§15) but owner confirms the SAME M/M
  symptom renders identically on Overview and Full Stage; Stage-By-Transport
  reaches the SAME zcl_abapgit_status_calc=>build_existing logic downstream
  regardless of which method resolved the remote side, so this D0's
  Stage-By-Transport capture is considered representative and sufficient to
  explain the Overview/Full-Stage symptom too, pending final owner
  confirmation.
DIFF: NOT_CAPTURED this session (BP7 remained armed but Diff was not
  triggered before this D0 update — owner reports Diff shows "no
  differences" for this file, consistent with Diff's local-content
  resolution NOT suffering from either causal factor above, but the live
  DIFF-side SHA1s were not directly captured by breakpoint this session).
Wall-clock time: Stage-By-Transport serialize = 4.10s-20.07s across repeat
  refreshes of the 236-object /LOT/GC-filtered set (vs. 795-1003s for the
  full 17321-object repo) — the transport-filtered repro is the
  recommended reproduction method going forward.
```

## Next required action

Execute `.memory/logs/variant_b_package_e_false_modified_os4_d1.md` (bounded
debugger worksheet) in ONE uninterrupted session and report the captured
values. No fix is authorized until this D0 packet is complete and a live
mismatch is confirmed. See
[.memory/handoffs/variant-b-package-e-e2-os4-diagnostic.md](.memory/handoffs/variant-b-package-e-e2-os4-diagnostic.md).
