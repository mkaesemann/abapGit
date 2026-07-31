# Variant B / Package E — D1 Diagnostic Log — False Local+Remote MODIFIED (OS4, LARGE REPO)

```text
STATUS=E2_FIXES_IMPLEMENTED_AND_ACTIVATED_AWAITING_SCALE_VALIDATION
E2_FIX_AUTHORIZED=YES (owner, explicit: "I Need a real fix... find the
  root cause and fix it" + "it might be worth trying to call the function
  module with the parameters set")
FIX_1=ZCL_ABAPGIT_OBJECT_FUGR=>get_includes(): added
  with_inactive_incls = abap_true to the RS_GET_ALL_INCLUDES call.
  Root cause (owner's own empirical finding, confirmed by this agent via
  live SQL against ENLFDIR): freshly-transported/not-yet-regenerated
  includes (e.g. FORM-routine pools like #lot#lgc_address_geocodef01) are
  silently omitted by this standard SAP kernel function unless this flag
  is set, even though the source is fully active. Activated.
FIX_2=THE PARALLEL-SERIALIZATION ROOT CAUSE, FOUND AND FIXED (see §22):
  all three ORTEC per-object prefetch classes
  (ZCL_ABAPGIT_ORTEC_SER_PREF/_EXT/_OO)'s `inject_from_buffer` methods
  merged freshly-received per-object data into session-local UNIQUE-keyed
  HASHED TABLEs via a bare `INSERT ls_x INTO TABLE mt_x` with NO clearing
  and NO sy-subrc check. Since parallel RFC worker sessions (`STARTING NEW
  TASK ... DESTINATION IN GROUP`) are POOLED/REUSED across many separate,
  unrelated serialize dispatches over their lifetime, a worker that had
  EVER cached an entry for a given key (e.g. DTEL rollname
  /LOT/GC_GEOLAT) would have every SUBSEQUENT re-injection for that same
  key SILENTLY FAIL (duplicate-unique-key INSERT is a no-op, uncaught) --
  causing that worker to serve WHATEVER STALE VALUE IT FIRST CACHED,
  forever, regardless of the object's true current content, and
  regardless of what the main process freshly re-extracted and sent.
  This fully explains: (a) parallel-only (run_sequential never uses this
  worker-session-scoped caching mechanism), (b) apparent
  non-determinism/inconsistent reproduction (depends entirely on WHICH of
  the ~14 pooled workers happens to be assigned a given object this run,
  and whether THAT SPECIFIC worker previously cached different content
  for it in some earlier, unrelated dispatch). Fix: CLEAR the relevant
  session-local cache table(s) at the top of each `inject_from_buffer`
  before the merge loop, in all three classes. Activated.
D1_LIVE_CAPTURE=CONFIRMED (see §14 — full live breakpoint capture through
  ZCL_ABAPGIT_STATUS_CALC=>build_existing's ELSE branch, exact mechanism
  proven, not just circumstantial)
ROOT_CAUSE_ID=CONFIRMED (see §14, §22): (A) is_local-file-sha1 computed via
  the bulk/filtered serialize path (zcl_abapgit_serialize=>files_local,
  reached via get_files_local_filtered/get_files_local) genuinely DIFFERS
  from the correctly-resolved is_remote-sha1 for this file in this code
  path (46e7ae23... vs f1f852820c..., the latter matching the known-good
  OBJ_STORE blob) — ROOT CAUSE NOW FOUND: stale-worker-cache bug in
  ZCL_ABAPGIT_ORTEC_SER_PREF*'s inject_from_buffer, see FIX_2 above. (B)
  The persisted checksum baseline (ZCL_ABAPGIT_REPO_CHECKSUMS, sourced
  from zcl_abapgit_persist_factory=>get_repo_cs()->read()) has only 3
  total file entries for the entire OS4 6.0 repo — a `READ TABLE ...
  BINARY SEARCH` against this near-empty baseline fails (sy-subrc=8) for
  virtually every file, so `build_existing`'s explicit "strange
  situation... mark both changed" ELSE branch fires UNCONDITIONALLY,
  setting BOTH lstate=M AND rstate=M regardless of true content -- STILL
  UNFIXED, a separate, independent defect from (A)/FIX_2, confirmed by
  the owner as still present even with parallel serialization off. (A) is
  the spurious trigger, (B) is why the result renders as symmetric "M M"
  (not a nuanced single-side change) for nearly every file in the repo.
  PC-1 (stale `mt_remote` cache) is REJECTED as the mechanism — Stage-by-
  Transport doesn't even use `mt_remote`/`get_files_remote` (confirmed via
  source + live trace, it uses the SAME ORTEC facade/OBJ_INDEX fast path
  as Diff) yet still reproduces the bug, proving the standard-cache-vs-
  ORTEC-facade asymmetry is NOT the root mechanism.
```

Scope: D0+D1 only (read-only, static source tracing + bounded read-only DB
queries). No fix, no D2/D3 instrumentation, no productive/DDIC/runtime-flag
change, no `.memory/state.md` rewrite beyond the compact pointer described in
the handoff. Repository: OS4 (`repo_key=288c81fc1cad`, branch
`refs/heads/development/6.0.x`, tip `81157b1448b4183f38403ec63caad1291a2226a4`,
shallow, `HIST_LEVEL=F`). Target object: DTEL `/LOT/GC_GEOLAT`
(`/src/#lot#gc/#lot#gc_geolat.dtel.xml`).

## 1. Verified call graph (current source, all three access modes)

**Repository overview** (`zcl_abapgit_gui_page_repo_view` →
`zcl_abapgit_repo_content_list=>list` → `build_repo_items`,
[zcl_abapgit_repo_content_list.clas.abap](src/repo/zcl_abapgit_repo_content_list.clas.abap#L125)):
```
zcl_abapgit_repo_status=>calculate( ii_repo = mi_repo  ii_log = mi_log )   " NO it_remote supplied, NO ii_obj_filter
```

**Full Stage** (non-"Stage by Transport";
[zcl_abapgit_stage_logic.clas.abap](src/repo/stage/zcl_abapgit_stage_logic.clas.abap#L114)
`zif_abapgit_stage_logic~get`, reached when `ii_obj_filter IS INITIAL`):
```
ii_repo_online->get_files_remote( ii_obj_filter )   " ii_obj_filter is INITIAL for Full Stage
zcl_abapgit_repo_status=>calculate( ii_repo = ii_repo_online  ii_obj_filter = ii_obj_filter  it_local = rs_files-local )
```

Both of the above resolve remote content the SAME way, because
`zcl_abapgit_repo_status=>calculate`
([zcl_abapgit_repo_status.clas.abap](src/repo/zcl_abapgit_repo_status.clas.abap#L64))
only bypasses `get_files_remote` when the CALLER already supplied `it_remote`
— which neither overview nor Full Stage does:
```
IF it_remote IS SUPPLIED.
  lt_remote = it_remote.
ELSE.
  lt_remote = ii_repo->get_files_remote( ii_obj_filter = ii_obj_filter  iv_ignore_files = abap_true ).   " <-- both overview and Full Stage land here
ENDIF.
```
`ii_repo->get_files_remote()` resolves to
[zcl_abapgit_repo_online.clas.abap](src/repo/zcl_abapgit_repo_online.clas.abap#L449)
`zif_abapgit_repo~get_files_remote`:
```
METHOD zif_abapgit_repo~get_files_remote.
  fetch_remote( ).                              " line 78
  rt_files = super->get_files_remote( ... ).     " zcl_abapgit_repo, returns cached mt_remote (line 716)
ENDMETHOD.
```
`fetch_remote()` (same file, line 78):
```
IF mv_request_remote_refresh = abap_false.
  RETURN.                                        " <-- no-op: mt_remote keeps whatever was cached earlier in this session/instance
ENDIF.
... pull_by_branch / pull_by_commit ... set_files_remote( ls_pull-files ) ...
```
`mv_request_remote_refresh` (defined in
[zcl_abapgit_repo.clas.abap](src/repo/zcl_abapgit_repo.clas.abap#L51)) is set
`abap_true` only at construction (line 206) and inside `refresh()` (line
376), and set `abap_false` once consumed, inside `set_files_remote` (line
868). `zcl_abapgit_repo_srv=>get_instance()->get(key)`
([zcl_abapgit_repo_srv.clas.abap](src/repo/zcl_abapgit_repo_srv.clas.abap#L142))
caches repo instances in `mt_list` for reuse across page navigations within
the same session — so, absent an explicit Pull/Refresh, **overview and Full
Stage compare local content against whatever `mt_remote` snapshot was cached
the FIRST time this repo instance fetched remote content in the current
session**, not necessarily the branch's current tip.

**Diff for one object** (always builds a single-object filter — see
[zcl_abapgit_gui_page_diff_base.clas.abap](src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap#L634)
`get_files_and_status`; `is_object`/`is_file` is always populated for a
per-object Diff invocation, so `li_obj_filter IS NOT INITIAL`):
```
CALL METHOD ('ZCL_ABAPGIT_ORTEC_GIT_FACADE')=>('RESOLVE_FILTERED_REMOTE')
  EXPORTING ii_repo_online = mi_repo  ii_obj_filter = li_obj_filter
  RECEIVING rt_files = et_remote.
et_status = zcl_abapgit_repo_status=>calculate( ii_repo = mi_repo  ii_obj_filter = li_obj_filter  it_local = et_local  it_remote = et_remote ).  " it_remote IS supplied -> bypasses get_files_remote entirely
```
`zcl_abapgit_ortec_git_facade=>resolve_filtered_remote`
([zcl_abapgit_ortec_git_facade.clas.abap](src/ortec/git/zcl_abapgit_ortec_git_facade.clas.abap#L38))
delegates unconditionally to
`zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`
([zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L90)),
which:
```
lv_commit = li_repo_online->get_selected_commit( ).       " normally initial (branch mode)
IF lv_commit IS INITIAL.
  lv_branch = li_repo_online->get_selected_branch( ).
  ls_state  = zcl_abapgit_ortec_repo_state=>get_state( repo_key, lv_branch ).   " reads ZAOG_REPO_STATE directly (persisted, NOT the mi_repo in-memory cache)
  lv_commit = ls_state-fetch_commit.
  li_branches = zcl_abapgit_git_transport=>branches( lv_url ).                 " LIVE, uncached ref lookup against the remote
  ls_branch   = li_branches->find_by_name( lv_branch ).
  IF lv_commit IS INITIAL OR ls_branch-sha1 IS INITIAL OR ls_branch-sha1 <> lv_commit.
    " stale/never-fetched: try a filtered cold-fetch to ls_branch-sha1, or fall back to get_files_remote
  ENDIF.
ENDIF.
rt_files = zcl_abapgit_ortec_obj_index=>get_files_for_filter( iv_repo_key = lv_repo_key  iv_commit = lv_commit  ii_obj_filter = ii_obj_filter  ... ).
```
This path independently re-validates the branch tip against the LIVE remote
and resolves the requested file directly from `ZAOG_OBJ_INDEX`/`ZAOG_OBJ_STORE`
at that (possibly newer) commit — **entirely bypassing `mi_repo`'s
`mt_remote`/`mv_request_remote_refresh` cache.**

`zcl_abapgit_stage_logic~get`'s `ELSE` branch (Stage-by-Transport,
`ii_obj_filter` bound) uses the identical ORTEC-facade call — confirming
Stage-by-Transport would NOT reproduce this symptom (only Full/unfiltered
Stage would), consistent with the owner's report of "Full Stage" specifically.

## 2. Diagnostic-relevant note on E1-PERF-A

`c_index_write_chunk_size` (write-batch size for `rebuild_index`'s bulk
INSERT/MODIFY loop, `zcl_abapgit_ortec_obj_index.clas.abap` line 456) has no
bearing on §1 above: `get_files_for_filter`'s READS of `ZAOG_OBJ_INDEX` are
unaffected by how many rows were written per batch during the last rebuild.
Confirmed independent, per D0 §0.

## 3. Candidate root cause (source-confirmed asymmetry, NOT yet live-confirmed)

**PC-1 — stale cached `mt_remote` in the shared `zcl_abapgit_repo_online`
instance, bypassed by the ORTEC filtered-walk's independent live-tip
revalidation.**

Mechanism: if the OS4 repository's `zcl_abapgit_repo_online` instance was
already loaded (and its `mt_remote` cache populated, `mv_request_remote_refresh`
consumed to `abap_false`) in the CURRENT session BEFORE the branch tip
advanced from an older commit to `81157b1448b4183f38403ec63caad1291a2226a4`
(e.g. via a background/independent ORTEC fastpath re-fetch that updates
`ZAOG_REPO_STATE` without ever touching the standard repo instance's own
cache), and the owner did not click Pull/Refresh since, then:
- Overview and Full Stage compare LOCAL against the OLD cached `mt_remote`
  entry for this file → if this file's content differs between the OLD and
  NEW commit, and LOCAL already matches the NEW commit, this reproduces a
  MODIFIED verdict that is **stale, not a real difference**.
- Diff, via the ORTEC facade, independently re-validates the branch tip live
  and resolves the file from `ZAOG_OBJ_INDEX` at the CURRENT commit
  (`81157b14...`, confirmed present and `IDX_STATUS=R` in D0 §3) → correctly
  finds LOCAL matches REMOTE → "no differences."

This is fully consistent with the observed pattern (overview+Full Stage=MM,
Diff=no differences) and with `ZAOG_REPO_STATE.FETCH_TS=2026-07-30 09:19:48`
being very recent (today), suggesting the branch tip may indeed have moved
during the owner's current session. **This has NOT been live-confirmed** —
no debugger capture of `mv_request_remote_refresh`, the cached `mt_remote`
entry's SHA1, or the exact wall-clock ordering has been performed. Elevate
via the D1 worksheet below (breakpoints 1-2 target this directly).

## 3b. New source finding this session — `build_existing`'s match-first
   short-circuit (ABAP-FS MCP direct read, `ZCL_ABAPGIT_STATUS_CALC`)

Direct read of `ZCL_ABAPGIT_STATUS_CALC`, method `build_existing`
(lines 111-155, called from `process_local` at line ~398 for every local
file matched against a remote file of the same path/filename):

```abap
rs_result-match    = boolc( is_local-file-sha1 = is_remote-sha1 ).   " line 128
IF rs_result-match = abap_true.
  RETURN.                                                            " lstate/rstate left at default (unchanged) — baseline is NEVER consulted here
ENDIF.

" only reached when is_local-file-sha1 <> is_remote-sha1:
READ TABLE it_state INTO ls_file_sig WITH KEY path/filename BINARY SEARCH.
IF sy-subrc = 0.
  IF ls_file_sig-sha1 <> is_local-file-sha1.  rs_result-lstate = modified. ENDIF.
  IF ls_file_sig-sha1 <> is_remote-sha1.      rs_result-rstate = modified. ENDIF.
ELSE.
  rs_result-lstate = modified.  rs_result-rstate = modified.          " both, unconditionally
ENDIF.
```

This is a definitive, line-level confirmation (not previously cited by exact
method/line in this OS4 log) of what the discovery doc's prose only
characterized generally: **`match` is decided FIRST, purely from
`is_local-file-sha1 = is_remote-sha1`, and short-circuits with an immediate
`RETURN` when true — `lstate`/`rstate` are never touched in that case,
regardless of how stale or wrong the checksum baseline (`it_state`/
`ls_file_sig`) is.** Consequently:

- **H3 (checksum baseline stale/wrong) is CONTRADICTED as a sole cause** —
  reclassified from `NOT_VERIFIED` to `CONTRADICTED_AS_SOLE_CAUSE` below. A
  stale baseline cannot, by itself, produce `lstate=modified AND
  rstate=modified` for a file whose local and remote SHA1 genuinely agree at
  the moment `build_existing` runs.
- Reaching the observed `lstate=modified AND rstate=modified` outcome
  **requires** `is_local-file-sha1 <> is_remote-sha1` to be true as computed
  for that specific `build_existing` call (whichever of the two branches
  below it, both still need that initial inequality).
- Given the owner's session discipline (no edit/Pull/Refresh/checksum-update
  between the three views) and that the SAP-side object content is
  deterministically re-serialized each time, the **local** SHA1 side of this
  inequality should be stable across Overview/Full-Stage/Diff. That leaves
  the **remote** SHA1 input as the only plausible source of divergence
  between Overview/Full-Stage (`lstate=rstate=modified`) and Diff (`match`
  presumably `true`, or at least zero net diff lines) — which is exactly
  what PC-1 (stale cached `mt_remote` vs. the ORTEC facade's independent
  live-tip/OBJ_INDEX resolution, §3) already predicts structurally. This
  finding narrows the remaining hypothesis space rather than replacing PC-1;
  it still requires live SHA1 capture (below) to move from CANDIDATE to
  CONFIRMED.
- This also fixes the exact breakpoint target for BP3: `ZCL_ABAPGIT_STATUS_CALC`
  line 128 (`rs_result-match = boolc( is_local-file-sha1 = is_remote-sha1 )`),
  conditioned on `is_local-file-filename = '#lot#gc_geolat.dtel.xml'` — this
  single line, hit once per Overview/Full-Stage/Diff action, is sufficient to
  capture both `is_local-file-sha1` and `is_remote-sha1` as actually used by
  the status calculation for this file, for all three flows.

## 4. Hypothesis matrix (neutral evaluation; H-numbering reconstructed for this
   log since the owner's original H1-H12 wording was not preserved verbatim
   in this session's context after a mid-session compaction — treat the
   numbering here as a restatement of the same hypothesis SPACE, not a
   verbatim quote of the owner's text; the owner should correct any mismatch)

| # | Hypothesis | Status | Basis |
|---|---|---|---|
| H1 | Wrong/mismatched repo_key used by one of the three flows | NOT_LIKELY | All three flows resolve `repo_key` the same way (`get_repo_key_for_url` from `mi_repo`'s own URL); no per-flow repo_key divergence found in source. |
| H2 | Overview/Stage and Diff use different remote-file snapshots (different commit or different caching layer) | **LEADING — PC-1 above, source-confirmed structurally, pending live SHA1/commit capture** | §1 above: confirmed structurally different code paths (`mt_remote` cache vs. ORTEC live-tip + OBJ_INDEX). |
| H3 | Checksum baseline (`zcl_abapgit_repo_checksums`) stale/wrong for this file | CONTRADICTED_AS_SOLE_CAUSE (this session, §3b) | `build_existing` decides `match` from `is_local-file-sha1 = is_remote-sha1` FIRST and returns immediately when true, never consulting the baseline — a stale baseline alone cannot produce `lstate=rstate=modified` when local truly equals remote. Baseline value itself still worth capturing (BP4) as corroborating, non-causal evidence. |
| H4 | Path/filename encoding mismatch (`#` namespace escaping) between index and local | NOT_LIKELY | `ZAOG_OBJ_INDEX.FILE_NAME = #lot#gc_geolat.dtel.xml` matches the owner-reported `REMOTE_PATH` exactly, byte-for-byte in the visible text. |
| H5 | OBJ_INDEX row stale/wrong for this commit (duplicate or wrong-commit row) | NOT_LIKELY | Exactly one `ZAOG_OBJ_INDEX` row exists for `obj_name=/LOT/GC_GEOLAT` at `repo_key=288c81fc1cad`; its `commit_sha1` matches the branch's current `FETCH_COMMIT`/`CURR_COMMIT`. |
| H6 | Missing/incomplete blob in `ZAOG_OBJ_STORE` for the indexed SHA1 | NOT_LIKELY | Blob exists, type=`blob`, size=3777 bytes — well-formed. |
| H7 | `HIST_LEVEL` not `F` for the relevant commit (incomplete snapshot) | NOT_LIKELY | `ZAOG_COMMIT_HIST.HIST_LEVEL='F'` for `81157b14...`. |
| H8 | Branch-switch/selection timing (stale `get_selected_branch()`/`get_selected_commit()`) | NOT_VERIFIED | Requires live capture of `mi_repo`'s state at each of the three actions; not distinguishable from H2 without capture. |
| H9 | Non-UTF8/binary content mismatch causing a false byte-level diff | NOT_LIKELY | DTEL XML is plain text; no binary-detection code path applies (`is_binary` only affects rendering, not the status MODIFIED verdict). |
| H10 | Object type/name case-sensitivity mismatch (`/LOT/GC_GEOLAT` vs `/lot/gc_geolat`) | NOT_LIKELY | Both `ZAOG_OBJ_INDEX.OBJ_NAME` and the owner-reported name are uppercase and match exactly. |
| H11 | Pending E1-PERF-A batch-size change silently altering index content | ELIMINATED | See D0 §0 / §2 above — chunk size only affects write-batch grouping, not content. |
| H12 | Filter-walk fallback path silently reverting to standard `get_files_remote` for Diff too (making Diff use the SAME stale cache as overview) | NOT_LIKELY as sole explanation, but **relevant boundary condition** | The fallback (`CATCH zcx_abapgit_exception` in `get_remote_files_for_stage`) only triggers on an actual exception (e.g. branch lookup failure); if it silently fell back for THIS Diff invocation, Diff would show the SAME MODIFIED verdict as overview — but the owner reports Diff shows NO differences, so the ORTEC fast path (not the fallback) is what actually ran for this reproduction. Worth confirming via breakpoint 3 below (was the fallback ever hit?). |
| H13 | Parallel-RFC-start-failure forces sequential fallback mid-serialize, changing the relative ORDER/TIMING of `mt_remote` population vs. local scan vs. checksum-baseline read for at least this file, producing a transient mismatch | **REJECTED (owner-confirmed, §10)** | Owner confirmed the "RFC task 1-1 already open" banner is a known WebGUI(SAP GUI for HTML)-only artifact, absent in classic SAP GUI for Windows, present in BOTH the first and later attempts, and unrelated to the MODIFIED/MODIFIED symptom. Do not pursue further as part of E2. |

## 5. Debugger worksheet (owner-executed, ONE uninterrupted session, ONE target
   file: `/src/#lot#gc/#lot#gc_geolat.dtel.xml` / DTEL `/LOT/GC_GEOLAT`)

Execution order matters: (a) open the OS4 repository overview first and let
it render, (b) WITHOUT Pull/Refresh, open Full Stage, (c) WITHOUT
Pull/Refresh, open Diff for this exact object. Set breakpoints BEFORE step
(a). Use conditional breakpoints filtered to this file/object wherever noted,
to avoid stopping on unrelated files in this large repository.

1. **`zcl_abapgit_repo_online=>fetch_remote`**, line 78 (`IF
   mv_request_remote_refresh = abap_false. RETURN.`) — no condition possible
   here (repo-level, not file-level); just note whether the breakpoint is HIT
   or SKIPPED (return) at step (a) and again at step (b). Capture:
   `mv_request_remote_refresh` value on entry, and (if it proceeds)
   `ls_pull-commit` after the `pull_by_branch`/`pull_by_commit` call.
2. **`zcl_abapgit_repo.clas.abap`, method behind `get_files_remote`
   (`zif_abapgit_repo~get_files_remote`, base implementation, ~line 716,
   `rt_files = mt_remote.`)** — conditional breakpoint: only break when a
   loop/inspection shows an `mt_remote` entry with `path =
   '/src/#lot#gc/' AND filename = '#lot#gc_geolat.dtel.xml'`. Capture that
   entry's `sha1` at step (a) (overview) and step (b) (Full Stage). Expect
   identical values if both truly share the cache (confirms/refutes the
   "same cache" half of PC-1).
3. **`zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`**, entry and
   the `IF lv_commit IS INITIAL OR ls_branch-sha1 IS INITIAL OR
   ls_branch-sha1 <> lv_commit.` line (~line 148) — break at step (c) only
   (Diff). Capture: `ls_state-fetch_commit`, `ls_branch-sha1` (live remote
   tip), and whether the `IF` was TRUE (stale, triggers cold-fetch/fallback)
   or FALSE (fast path taken directly with the cached commit). Also capture
   whether the `CATCH zcx_abapgit_exception` at the bottom of the method was
   ever entered (confirms/refutes H12).
4. **`zcl_abapgit_ortec_obj_index=>get_files_for_filter`** (entry) — break at
   step (c) only. Capture `iv_commit` (should equal `81157b1448b4183f38403ec63caad1291a2226a4`
   if the fast path was taken) and, from its return value, the resolved
   `sha1` for `/src/#lot#gc/#lot#gc_geolat.dtel.xml`. Compare against the
   `BLOB_SHA1` already recorded in D0 §3
   (`f1f852820cdd36e58845d424bfa3e28876b11d38`) — a mismatch here would mean
   OBJ_INDEX itself resolved something unexpected, contradicting §1's D0
   evidence and requiring re-investigation.
5. **`zcl_abapgit_repo_checksums=>get_checksums_per_file`** (or its
   equivalent baseline-read call inside `zcl_abapgit_status_calc=>calculate_status`)
   — conditional breakpoint on this file's path/filename, hit once per action
   (a)/(b)/(c). Capture the baseline SHA1 recorded for this file each time —
   confirms/refutes H3 (should be identical across all three if the baseline
   truly isn't touched between actions, since no Pull/checksum-update
   happened).

Record all captured values directly into
`.memory/incidents/variant_b_package_e_false_modified_os4_d0.md` §4's
`NOT_CAPTURED` placeholders, replacing each with the observed value (or
`N/A` with a one-line reason if a breakpoint never fires), then report back
for D2 authorization decision.

## 6. Breakpoints actually armed this session (ABAP-FS MCP, supersedes the
   manual worksheet above as the operative plan — the agent captures
   automatically once each is hit, no owner debugger interaction needed)

Debug session: `abap_debug_session` (`connectionId=it8`, `action=start`) →
`Mode: User, Status: Ready for debugging`. 10 session-scoped breakpoints set
via `abap_debug_breakpoint`:

| # | Class | Line | Condition | Purpose |
|---|---|---|---|---|
| BP1 | `ZCL_ABAPGIT_REPO_ONLINE` | 83 | none | `fetch_remote`'s `IF mv_request_remote_refresh = abap_false. RETURN.` gate |
| BP2 | `ZCL_ABAPGIT_REPO` | 716 | none | `get_files_remote`'s `rt_files = mt_remote.` — inspect `mt_remote` for the target path/filename before return |
| BP3 | `ZCL_ABAPGIT_STATUS_CALC` | 128 | `is_local-file-filename = '#lot#gc_geolat.dtel.xml'` | `build_existing`'s match decision — primary breakpoint, see §3b |
| BP4 | `ZCL_ABAPGIT_REPO_CHECKSUMS` | 214 | none | `zif_abapgit_repo_checksums~get`'s cache-valid gate (baseline read) |
| BP5a | `ZCL_ABAPGIT_ORTEC_FILTER_WALK` | 101 | none | `get_remote_files_for_stage` entry (first executable line; `METHOD` line 90 itself is not a valid breakpoint location) |
| BP5b | `ZCL_ABAPGIT_ORTEC_FILTER_WALK` | 138 | none | persisted-vs-live-tip staleness check (`ls_branch-sha1 <> lv_commit`) |
| BP5c | `ZCL_ABAPGIT_ORTEC_FILTER_WALK` | 171 | none | fallback `CATCH zcx_abapgit_exception` → reverts to standard `get_files_remote` (tests H12) |
| BP6a | `ZCL_ABAPGIT_ORTEC_OBJ_INDEX` | 154 | `iv_repo_key = '288c81fc1cad'` | `get_files_for_filter` entry (line 144 `METHOD` itself is not executable) |
| BP6b | `ZCL_ABAPGIT_ORTEC_OBJ_INDEX` | 197 | `iv_repo_key = '288c81fc1cad'` | first `rt_files = build_files_from_rows( ... )` call — resolved SHA1 for the target file |
| BP7 | `ZCL_ABAPGIT_GUI_PAGE_DIFF_BASE` | 626 | none | `constructor`'s `IF lines( mt_diff_files ) = 0` — the exact "no differences" decision point |

All 10 verified `Set successfully` by ABAP-FS. Two originally-requested
locations (`ZCL_ABAPGIT_REPO_CHECKSUMS` line 210, `ZCL_ABAPGIT_ORTEC_FILTER_WALK`
line 90, `ZCL_ABAPGIT_ORTEC_OBJ_INDEX` line 144) were rejected by the ADT
breakpoint API as non-executable (`METHOD ...` header / `DATA` declaration
lines); each was moved to that method's actual first executable statement,
noted in the table above. Nothing else in this plan differs from the
worksheet in §5.

**Blocking gap**: no available tool in this profile can drive the abapGit
web UI (Overview → Full Stage → Diff) itself — ABAP-FS MCP provides ADT
debugging primitives only, not SAP GUI/browser automation for this internal
application. Per the task's own authorized stop condition, the owner must
trigger the three-screen sequence in their already-authenticated IT8 SAP GUI
session (same SAP user as this debug session) while these breakpoints remain
armed; capture and stepping then proceed autonomously.

**Update — resolved**: the owner shared their WebGUI (SAP GUI for HTML)
browser tab directly, which this agent then drove itself via browser
automation tools (`click_element`/`read_page`/`screenshot_page`). WebGUI
(no classic frontend attached) correctly routes `Mode: User` breakpoint
hits to the external ADT/VS Code listener; a classic SAP GUI (WinGUI)
window on the SAME user does NOT — it intercepts the breakpoint locally in
its own modal debugger dialog, invisible to `abap_debug_stack`
(`No service for threadid 1`). Use WebGUI, not WinGUI, for this style of
externally-driven reproduction.

## 7. Live capture — RUN A (first Overview→Full Stage attempt, this session,
   pre-timeout)

Breakpoints 1-4 fired correctly via WebGUI. Overview rendered to completion
(17321 objects, 795.29s serialize). Captured at BP3
(`ZCL_ABAPGIT_STATUS_CALC=>build_existing`, condition matched):

```text
is_local-file-sha1  = f1f852820cdd36e58845d424bfa3e28876b11d38
is_remote-sha1      = f1f852820cdd36e58845d424bfa3e28876b11d38   (EQUAL)
is_local-file-filename = #lot#gc_geolat.dtel.xml
is_local-file-path     = /src/#lot#gc/
=> rs_result-match = abap_true -> immediate RETURN (lstate/rstate left
   unchanged, i.e. NOT flagged modified, for this file in THIS render)
```

Rendered Overview page (full accessibility-tree capture, case-insensitive
search for "geolat" across the entire ~3343-line snapshot): **zero
matches** — `/LOT/GC_GEOLAT` does not appear in the changed-items table at
all, i.e. Overview showed it as UNCHANGED. This is the OPPOSITE of the
owner-reported symptom for this specific run.

Full Stage was then opened (no Pull/Refresh) and reached BP1/BP2 again with
`mv_request_remote_refresh = 'X'` on the first call (live refresh) — but
progress stalled at ~5% because an UNRELATED, pre-existing breakpoint in
standard function `DD_TBFD_SET_GET` (function group `SDTB`, line 216,
`distribute_tables dd03e_tab dd03p_tab tabname.`) kept re-triggering inside
the DDIC table-metadata read path used by `ZCL_ABAPGIT_OBJECT_TABL`'s TABL
serialization (`DDIF_TABL_GET` → `DD_TABL_GET` → `DD_TABD_GET` →
`DD_TBFD_GET` → `DD_TBFD_SET_GET`) — a `Mode: User` debug session intercepts
ALL breakpoints hit by that user's requests, not just the 10 we set, so a
stray/pre-existing breakpoint on a heavily-looped standard FM can stall an
unrelated investigation. Before Full Stage could complete, the WebGUI
session itself timed out from inactivity (unrelated infra issue, separate
from the ABAP debug layer) and RUN A's Full Stage result was never obtained.

**Security note (process, no secret persisted here)**: after the timeout,
a stale "Login: ORTEC-SAP@dev.azure.com" dialog was found (behind a new
progress popup) with a live Azure DevOps PAT visible in plaintext in the
page's accessibility tree. The agent did not interact with that dialog or
use the credential; the owner rotated the token and cleared the dialog
themselves. No token value is recorded in any memory file. Lesson for this
class of task: reading a full accessibility-tree snapshot of an
authenticated enterprise web app can expose in-flight credential fields
verbatim — treat any such capture as sensitive and never echo it back
in full.

## 8. Live capture — RUN B (Overview re-triggered after session reset) —
   NEW, SIGNIFICANT: non-deterministic reproduction observed

After the owner retriggered Overview fresh (new WebGUI session), the debug
session's own bookkeeping was found stale: `abap_debug_session action=stop`
returned HTTP 400, while `status`/`start` still reported "Active" and
`abap_debug_status` kept reporting the SAME frozen pre-timeout state
(paused at `DD_TBFD_SET_GET` line 216, 3 threads) even though the real
HTTP request had already completed and the page had fully rendered — i.e.
**none of the 10 armed breakpoints were actually live/hooked into this new
request; the debug hook had silently died with the old web session.**
Removing the stray `DD_TBFD_SET_GET` breakpoint (`abap_debug_breakpoint`,
action=remove) caused the status to refresh to a genuine idle state (0
threads, "Completed - Execution finished"), confirming the staleness. All
10 original breakpoints were then re-armed fresh (all `Set successfully`)
before proceeding — **no live SHA1/commit values were captured for RUN B's
Overview render** (the breakpoints were re-armed only after this render had
already completed).

Despite the missing breakpoint capture, the RENDERED OUTPUT itself is
directly observed DOM/accessibility-tree text, not inferred, and is a
genuine, important finding:

```text
RUN B rendered Overview row: "DTEL /LOT/GC_GEOLAT   SAP diff (1) M M"
  -> Local: Modified, Remote: Modified, diff(1) link present
  -> MATCHES the owner-reported false-MODIFIED symptom for this exact file
```

This directly CONTRADICTS RUN A's Overview render (same file, same repo,
same commit `81157b14...`, no Pull/Refresh/branch-switch in between,
within the same overall investigation session) which showed the file as
UNCHANGED (absent from the table). **The false-MODIFIED status for this
file is non-deterministic across separate Overview computations**, not a
stable, always-reproducible verdict — this rules out any theory requiring
a permanently-stale value and instead points to something timing/ordering
dependent that varies per-run.

**New candidate — PC-2 (parallel-RFC-start-failure / sequential-fallback
timing)**: RUN B's rendered page displayed this warning banner (not
recorded as present during RUN A, though RUN A's banner state was not
explicitly checked either way, so this is a NEW observation, not a
confirmed absence in RUN A):

```text
RFC task 1-1 already open.
Running current object sequentially due to parallel start failure.
Changed package assignment for object DCLS /O4H/R_TPL_RESOURCETYPESETTP
Changed package assignment for object DCLS /O4H/R_TPL_RESOURCETYPETP
```

Hypothesis: when the parallel-RFC serialization/status-calc infrastructure
fails to start a parallel task and falls back to running an object
sequentially, the ORDERING/TIMING of `mt_remote` population vs. the local
scan vs. the checksum-baseline read for at least this file may change
relative to the normal all-parallel path, producing a transient/spurious
mismatch. **NOT yet source- or live-confirmed** — this is a new hypothesis
added to the matrix in §4 (call it H13), to be tested by re-arming
breakpoints BEFORE the render (in progress, see STATUS block at file top)
and checking whether the RFC-fallback banner and a live BP3 mismatch
(`is_local-file-sha1 <> is_remote-sha1`) co-occur.

## 10. CORRECTION (owner-verified, this session) — retracts parts of §7-§9

The owner reviewed this session's claims directly and corrected two of them.
Recording both corrections verbatim-in-substance, per the no-fabrication/
no-silent-drift rule — **do not treat the retracted claims as evidence of
anything going forward**:

1. **RUN A's "file absent / rendered UNCHANGED" claim (§7) is FAULTY,
   RETRACTED.** The owner confirmed `/LOT/GC_GEOLAT` WAS visible with
   Local:Modified/Remote:Modified in that same Overview render too. The
   symptom is **deterministic** — it reproduces every time in both the full
   Overview and Full Stage, not intermittently. The "RUN A vs RUN B
   non-determinism" framing in §8 is WRONG and must not be relied on.
   Root-cause analysis: the agent's own text search for "geolat" (case-
   insensitive grep across a `read_page` accessibility-tree snapshot) most
   likely produced a FALSE NEGATIVE, not a true absence — this large table
   (changed-items list, thousands of rows; Stage's own list is 5489 objects
   over 37 pages) is almost certainly virtualized/paginated in the
   UI5-on-WebGUI rendering, so a full-snapshot text search can easily miss
   an off-screen/not-yet-rendered row. **Lesson: a zero-match text search
   across a page snapshot of a large/paginated table is NOT valid evidence
   of absence — only of non-presence-in-the-currently-rendered-subset.**
   Must use the page's own filter/search UI control (bringing the exact row
   into the rendered DOM) before concluding a row is absent, never a blind
   grep alone. The single BP3 hit that WAS captured in RUN A
   (`is_local-file-sha1 = is_remote-sha1`, both
   `f1f852820cdd36e58845d424bfa3e28876b11d38`) is not itself retracted as a
   raw data point (the debugger did capture equal SHA1s at that specific
   hit), but its INTERPRETATION ("this proves Overview rendered the file as
   unchanged") is retracted — `build_existing`/the status calculation may
   run more than once per file per action, or this hit may not have been
   the one governing the final rendered verdict. This discrepancy is NOT
   yet explained and remains an open question for the next live capture.
2. **The "RFC task 1-1 already open... parallel start failure" banner (§8,
   candidate H13/PC-2) is a KNOWN, UNRELATED WebGUI-only artifact — RETRACT
   H13/PC-2 entirely.** Owner confirmed this banner is a SAP GUI for HTML
   (WebGUI)-specific quirk that does NOT occur in classic SAP GUI for
   Windows, and it was ALSO present during the very first (RUN A) attempt,
   not something new/differentiating in RUN B as originally guessed. It is
   NOT related to the MODIFIED/MODIFIED symptom. Do not investigate this
   further as part of E2; H13 is removed from the live hypothesis space
   (§4's table entry is left in place for history but should be read as
   REJECTED, not "new/unconfirmed").

## 11. NEW — separate issue for Package E backlog (NOT part of E2, do not
    investigate now): `SYSTEM_NO_ROLL` dump during Stage-after-Overview

The owner reported that triggering Full Stage right after a full Overview
serialize/retrieve of this large repo (OS4 6.0, 17321 objects) sometimes
causes a `SYSTEM_NO_ROLL` runtime dump in IT8 (most recent dump in the
system at the time of this session). Owner's own working theory: possibly
related to bad cache/memory handling when the same large repo's data is
held/re-processed across both the Overview and Stage computations back to
back. **This is explicitly a SEPARATE issue from E2** (the false-MODIFIED
status) — do not fold it into the E2 root-cause investigation. Action:
record as a new backlog item for Package E (a dedicated `SYSTEM_NO_ROLL`
investigation, likely memory/roll-area sizing or a missing
FREE/CLEAR of a large internal table across the two back-to-back
computations) — no further action taken on it in this session.

## 12. Current status (superseding §9)

Per the owner's direction, Overview is no longer required as a
prerequisite step — the MODIFIED/MODIFIED symptom is confirmed reproducible
in Full Stage alone, deterministically, every time. This session's live
capture continues directly against Full Stage (abapGit was restarted by the
owner and navigated straight to Stage). Important operational note from the
owner: **the Stage page is paginated (37 pages for 5489 changed objects)
and the target file `/src/#lot#gc/#lot#gc_geolat.dtel.xml` may not be on
the initially-displayed page** — it must be located via the page's own
"Filter Objects" search box (or trusted only from breakpoint-captured
values / internal table dumps), never assumed absent from an unfiltered
page/snapshot view. All 10 breakpoints remain armed. Full Stage's own
serialize completed cleanly this run (17321 objects, 1003.62s, no dump).
No fix authorized; no code modified; no DB write performed.

## 13. Blocker — Stage page stuck on "Loading stage data..." (this session)

After the RUN B/C Full Stage serialize completed (17321 objects, 1003.62s,
no dump), the Stage object table itself never rendered — stuck on "Loading
stage data..." across multiple checks, filter-box input was silently
cleared, and no further server-side breakpoint fired (0 threads). The page
displays a standing banner: "Attention: You use Edge browser control. There
are several known malfunctions... If this does not disappear soon, then
there is a JS init error." Owner suspects this is a SAP GUI for HTML
(WebGUI) + Edge browser-control client-side rendering issue, unrelated to
the server-side E2 investigation, and is retrying while checking browser
dev-tools console errors.

**Owner's explicit fallback instruction (binding constraint for this
incident) if the WebGUI Stage retry does not work**: restart abapGit and
use the **Overview page only** — do **NOT** use Full Stage in that case,
specifically to avoid re-triggering the `SYSTEM_NO_ROLL` dump (§11) that
can occur when Stage runs right after a full Overview serialize of this
large repo. Owner confirms the wrong-state determination (MODIFIED shown
with no actual diff) happens identically on the Overview page alone, so
the E2 root cause is fully diagnosable from Overview-only captures if
needed. **STATUS=PAUSED_AWAITING_OWNER_GO** — no further browser/debugger
actions until the owner reports back whether the WebGUI retry worked.

## 14. ROOT CAUSE CONFIRMED — live capture via "Stage By Transport"
    (owner-provided fast repro, this session)

**Resolved (owner)**: fixed the WebGUI/Edge rendering issue from §13. Owner
also found a much faster, targeted reproduction: **repo menu → "Stage By
Transport" → transport `IT8K900025`** (filters to `/LOT/GC` package objects
only, 236 objects instead of 17321). This reproduces the M/M status for
`/LOT/GC_GEOLAT` in ~4-20 seconds instead of ~15+ minutes, AND additionally
shows function group `/LOT/GC_ADDRESS_GEOCODE` as claimed "does not exist
locally at all" even though it definitely exists in IT8 — a second,
more severe symptom, NOT YET INVESTIGATED this session (follow-up needed).

Owner also corrected: the `git()`-source-confirmed fact that
`zif_abapgit_stage_logic~get` routes "Stage By Transport" (any call with
`ii_obj_filter` supplied) through the SAME ORTEC facade
(`CALL METHOD ('ZCL_ABAPGIT_ORTEC_GIT_FACADE')=>('RESOLVE_FILTERED_REMOTE')`
→ `ZCL_ABAPGIT_ORTEC_FILTER_WALK`) that single-object Diff uses — NOT the
standard `mt_remote`/`get_files_remote` cache path used by unfiltered
Overview/Full Stage. Confirmed live via breakpoints this session (see
below): BP5a/b fired, BP8 (the `CATCH cx_root` fallback at
`ZCL_ABAPGIT_STAGE_LOGIC` line 134) did NOT fire, proving the ORTEC facade
call succeeded without exception — no silent fallback occurred. **This
means PC-1/H2 (the standard-cache-vs-ORTEC-facade asymmetry) CANNOT be the
root cause**, since this reproduction never touches the standard cache
path at all, yet the bug still occurs.

### Live breakpoint sequence (fresh debug session — the previous one had
    silently gone dead again; `abap_debug_session action=stop` succeeded
    this time, followed by a clean `start` and re-arming all 11
    breakpoints; this is the THIRD time in this incident the debug hook
    has silently died without visible symptoms other than 0 threads never
    changing — see §15 for a consolidated lesson)

1. **BP5a** (`ZCL_ABAPGIT_ORTEC_FILTER_WALK` line 101, `get_remote_files_for_stage`
   entry) — fired, confirming the ORTEC facade path is taken for Stage By
   Transport.
2. **BP5b** (line 138, staleness check) — fired. Captured:
   `lv_commit = 81157b1448b4183f38403ec63caad1291a2226a4`,
   `ls_branch-sha1 = 81157b1448b4183f38403ec63caad1291a2226a4` (EQUAL) — the
   `IF` is FALSE, so the FAST PATH (warm OBJ_INDEX) is taken, not a cold
   fetch. BP5c (fallback catch) correctly did NOT fire.
3. **BP6b** (`ZCL_ABAPGIT_ORTEC_OBJ_INDEX` line 197, `build_files_from_rows`)
   — fired. `iv_repo_key = 288c81fc1cad` confirmed. (BP6a, the method
   entry at line 154, was not observed to pause separately — not
   investigated further, non-blocking.)
4. **BP4** (`ZCL_ABAPGIT_REPO_CHECKSUMS` line 214, cache-valid gate) —
   fired. `mv_cache_valid` = blank/false (cache rebuilt fresh this
   request, consistent with prior findings).
5. **BP3** (`ZCL_ABAPGIT_STATUS_CALC=>build_existing` line 128, condition
   matched on `#lot#gc_geolat.dtel.xml`) — fired. Captured:
   ```text
   is_local-file-sha1  = 46e7ae23ace9830328dd0e7c063e31f671c98f56
   is_remote-sha1      = f1f852820cdd36e58845d424bfa3e28876b11d38
   is_local-file-path  = /src/#lot#gc/
   is_remote-path      = /src/#lot#gc/
   is_remote-filename  = #lot#gc_geolat.dtel.xml
   ```
   Path and filename match exactly on both sides. The two SHA1 values are
   DIFFERENT. `is_remote-sha1` matches the known-good OBJ_STORE blob
   recorded in D0 §3 — the REMOTE side is correctly resolved. Verified via
   a bounded `ZAOG_OBJ_INDEX` query (`repo_key=288c81fc1cad`) that
   `46e7ae23ace9830328dd0e7c063e31f671c98f56` does NOT match ANY recorded
   remote blob SHA1 for this repo (checked against all `geolat`/`geolon`/
   `geoalt`-family DTELs specifically, given how similar these names are —
   a possible name-collision/cache-key bug was considered and RULED OUT:
   zero matches for this SHA1 anywhere in `ZAOG_OBJ_INDEX` for this repo).
   So this is not a case of the wrong sibling object's REMOTE content
   being substituted — the LOCAL value itself is anomalous relative to
   both the remote index and (per the owner's Diff result) the object's
   true current content.
6. Stepped through line-by-line from here (not just `continue`), to avoid
   missing the exact branch taken:
   - Line 128: `rs_result-match = boolc(...)` evaluates to `abap_false`
     (confirmed: the two SHA1s differ).
   - Line 129-130: `IF rs_result-match = abap_true. RETURN. ENDIF.` — NOT
     taken (match is false), execution falls through.
   - Line 134-138: `READ TABLE it_state INTO ls_file_sig WITH KEY path =
     ... filename = ... BINARY SEARCH.` — captured **`sy-subrc = 8`**
     (NOT 0, NOT the "clean not-found" 4 — `8` means the binary-search
     precondition itself was not satisfiable) and **`lines( it_state ) =
     3`** — the persisted checksum-baseline table passed into this method
     has only **3 total rows** for this request.
   - Line 140 `IF sy-subrc = 0.` — FALSE, so the ELSE branch (lines
     148-153) is taken.
   - Lines 152-153 executed: **`rs_result-lstate = 'M'`**, **`rs_result-rstate
     = 'M'`** — confirmed via direct variable evaluation immediately
     after each assignment. This is the exact, direct, proven mechanism
     producing the rendered "M M" status.
7. Execution then continued to completion with no further breakpoint hits
   for this same file (processed once). Page re-rendered (Serialize: 236
   objects, 4.10 seconds) — was still finishing client-side render at last
   check.

### Why `it_state` only has 3 rows (traced, source-confirmed)

`ZCL_ABAPGIT_STATUS_CALC~calculate_status`'s local variable
`lt_state_by_file` (passed to `build_existing` as `it_state`) is built by
`ensure_state( it_cur_state = it_cur_state  it_local = it_local )`:
```abap
IF lines( it_cur_state ) = 0.
  " Empty state is usually not expected. Maybe for new repos.
  " In this case suppose the local state is unchanged
  LOOP AT it_local ... " builds one entry PER LOCAL FILE from local's own sha1
ELSE.
  rt_state = it_cur_state.   " <-- this branch was taken (3 lines, not 236)
ENDIF.
```
Since the result has only 3 lines (not one-per-local-file, which would be
236 for this transport), `it_cur_state` (the parameter, NOT the empty-state
fallback) was itself already only 3 lines. `it_cur_state` traces back to
`ZCL_ABAPGIT_REPO_CHECKSUMS~get()`, which reads the repo's PERSISTED
checksum baseline via `zcl_abapgit_persist_factory=>get_repo_cs()->read(
mv_repo_key )` — a whole-repo persisted record, NOT filtered by
`ii_obj_filter`/transport at all (`get()` takes no filter parameter). **This
means the persisted checksum baseline for the ENTIRE OS4 6.0 repo has only
3 file entries recorded, total** — this baseline is only populated/updated
by `zif_abapgit_repo_checksums~rebuild()` (called after a successful
Add+Commit through abapGit). A near-empty persisted baseline for a
repo this size strongly suggests this repo has rarely or never had a
successful Commit recorded through this checksum mechanism (or it was
cleared/reset at some point) — **NOT yet confirmed which; this is a
separate, useful follow-up question for the owner, not fabricated.**

### Two distinct, now-separated causal factors

- **(A) Trigger (NOT yet root-caused further — follow-up needed)**: the
  bulk/filtered local serialization (`zcl_abapgit_serialize=>files_local()`,
  reached via `get_files_local_filtered`/`get_files_local`) produces a
  local SHA1 for `/LOT/GC_GEOLAT` that does not match what the owner's Diff
  action computes for the same live object (Diff reports no differences,
  implying Diff's local-content resolution finds local=remote). WHY these
  two resolution paths diverge for this specific object is NOT yet
  determined — candidate angles for a future session: language/translation
  variant handling, active-vs-inactive version selection, a stale
  serialize-result cache keyed incorrectly, or a genuine DDIC-metadata
  read inconsistency (recall this object's serialization chain passes
  through `DDIF_TABL_GET`/`DD_TBFD_SET_GET`-style DDIC field-catalog
  reads, per earlier full-Overview call-stack observations — worth
  checking first).
- **(B) Amplifier (fully confirmed, source + live)**: the persisted
  checksum baseline is nearly empty (3 rows) for this repo, so ANY
  `match=false` outcome (whether from cause (A) or a genuine real change)
  ALWAYS renders as a blanket, undifferentiated "M M" rather than a
  correctly-attributed single-side state, because `build_existing`'s
  documented "strange situation" fallback cannot distinguish "local
  changed" from "remote changed" from "both changed" without a baseline
  entry to compare against. This explains why nearly every row in the
  236-object Stage-By-Transport view (and the 5489-object full Stage view)
  renders as symmetric "M M".

### Not yet investigated (follow-ups for a future session, NOT done here)

- Why `zcl_abapgit_serialize=>files_local()`'s result for this DTEL
  diverges from Diff's own local-content resolution (cause A above).
- The `/LOT/GC_ADDRESS_GEOCODE` "does not exist locally" symptom — a
  DIFFERENT failure mode, not yet traced; likely a similar "lookup miss ->
  wrong-default" pattern but in different code (possibly
  `build_new_remote`/`process_remote`'s item-index lookup, or a
  function-group-specific serialization/item-resolution issue), not
  confirmed.
- Why the persisted checksum baseline has only 3 entries for this large,
  presumably actively-used repo (owner follow-up question, not
  technical-only).
- `SYSTEM_NO_ROLL` dump (§11) — separate Package E backlog item, still not
  investigated.

## 15. Lesson — debug session silently dies with NO visible symptom other
    than "0 threads" never changing across a genuinely running, breakpoint
    -laden request

This is the THIRD occurrence in this single incident (after the two
documented in §8/§13) of the ABAP-FS debug session's hook silently going
dead — `abap_debug_status` kept reporting `Active Threads: 0` /
`"Completed - Execution finished"` throughout an entire multi-second-to-
multi-minute server computation that DEFINITELY executes code covered by
multiple unconditioned breakpoints (e.g. BP4, always reached). The only
reliable tell was the ABSENCE of any expected breakpoint hit despite the
computation visibly completing (serialize success banner rendered). Fix
each time: `abap_debug_session action=stop` (may 400 if already dead, or
may succeed — inconsistent) → `action=start` (fresh) → re-arm ALL
breakpoints (`abap_debug_breakpoint action=set`, idempotent) → retry the
triggering action. **Lesson for future sessions of this kind: after ANY
gap in activity (a pause for owner input, a page reload, a WebGUI
timeout/fix), always do a full stop+start+re-arm cycle BEFORE trusting
that previously-set breakpoints are still live, rather than assuming
"Set successfully" from hours/many-actions ago still holds.**

## 16. Diff-side confirmation — LOCAL side proven wrong (owner-requested
    follow-up, this session)

Per the owner's request, triggered Diff for `/LOT/GC_GEOLAT` directly from
the Stage-By-Transport row (clicking the filename link opens its diff).
Confirmed via call stack that Diff ALSO resolves remote through the SAME
ORTEC facade (`ZCL_ABAPGIT_GUI_PAGE_DIFF_BASE` line 669 →
`ZCL_ABAPGIT_ORTEC_GIT_FACADE` line 39 → `ZCL_ABAPGIT_ORTEC_FILTER_WALK`)
AND internally calls the SAME `zcl_abapgit_repo_status=>calculate` →
`ZCL_ABAPGIT_STATUS_CALC=>build_existing` (BP3 fired again, same file).
Critically, Diff resolves local content via `get_files_local_filtered`
with a **single-item filter** (`CREATE OBJECT lo_filter EXPORTING is_item
= ls_item`, one object only) rather than the transport's 236-item filter.

Captured at BP3 in the Diff context:
```text
is_local-file-sha1  = f1f852820cdd36e58845d424bfa3e28876b11d38
is_remote-sha1      = f1f852820cdd36e58845d424bfa3e28876b11d38   (EQUAL)
```
**This is the decisive confirmation the owner asked for**: in Diff's
context, local and remote are EQUAL and both correctly equal the
known-good blob. In Stage's context (same file, same commit, same
session, moments apart), local came out as `46e7ae23...` — WRONG. Since
Diff is trusted as ground truth (owner: "no differences"), **the LOCAL
side is conclusively the broken one**, and the divergence is narrowed
specifically to: **single-object-filtered local serialize (correct) vs.
multi-object/bulk-filtered local serialize (wrong)** — both go through
the exact same `zcl_abapgit_serialize=>files_local()` entry point and the
exact same `get_files_local_filtered()` wrapper, so the bug must be
inside `files_local()`/`add_objects()`'s handling of a filter with MANY
items vs. ONE item (e.g. parallel-RFC dispatch, a per-object prefetch/
buffer mechanism, or object-count-dependent branching).

### Investigated candidate: ORTEC per-object prefetch buffer (checked,
    NOT confirmed as the mechanism for this object)

`zcl_abapgit_serialize=>run_parallel` (the parallel-RFC dispatch used only
when serializing many objects) calls three ORTEC-specific helpers before
each `CALL FUNCTION 'Z_ABAPGIT_SERIALIZE_PARALLEL' STARTING NEW TASK`:
`zcl_abapgit_ortec_ser_pref=>extract_for_object`,
`..._ser_pref_ext=>extract_for_object`, `..._ser_pref_oo=>extract_for_object`
— custom performance optimizations that slice a prefetched buffer (DOKIL
documentation entries, MSAG message entries, etc.) per-object using a
manually-coded PREFIX-RANGE `LOOP AT ... WHERE object >= lv_object AND
object < lv_object_high` technique (`ZCL_ABAPGIT_ORTEC_SER_PREF=>
extract_for_object`). This looked like a strong candidate for a
name-collision bug given how similar the GEOLAT/GEOLON/GEOALT names are.
**Checked directly against live `DOKIL` table data**
(`ID='DE' AND OBJECT LIKE '/LOT/GC_GEO%'`): `/LOT/GC_GEOALT`, `/LOT/GC_GEOLAT`,
`/LOT/GC_GEOLON`, `/LOT/GC_GEO_ALIGN_REQUIRED` all diverge from each other
well BEFORE the position used as the prefix-range boundary (e.g. GEOLAT
vs. GEOLON differ at the 13th character, `A` vs `O`, which alone
determines lexicographic ordering) — so this specific prefix-range
extraction is NOT mis-scoped for these particular sibling objects. **This
candidate is not eliminated in general** (only checked for this one
object family) but is NOT confirmed as the mechanism for `/LOT/GC_GEOLAT`
specifically. Not investigated further this session due to time; the
parallel RFC worker process itself was not reachable for direct breakpoint
tracing (runs as a separate async work process under `STARTING NEW TASK`).

## 17. `/LOT/GC_ADDRESS_GEOCODE` "does not exist locally" — attempted
    reproduction this session, RESULT DIFFERS FROM OWNER REPORT (flagged,
    not resolved)

Owner reported that during the Stage-By-Transport repro, function group
`/LOT/GC_ADDRESS_GEOCODE`'s function-module include file
(`/src/#lot#gc/#lot#gc_address_geocode.fugr.#lot#lgc_address_geocodef01.abap`)
shows **"Local: Not exists" / "Remote: Added new"** despite genuinely
existing locally in IT8.

Attempted live capture: set conditional breakpoints on
`ZCL_ABAPGIT_STATUS_CALC=>build_existing` (`is_local-file-filename` match)
and `=>build_new_remote` (`is_remote-filename` match) for this exact
filename, confirmed the debug hook was genuinely alive (an unconditioned
`ZCL_ABAPGIT_REPO_CHECKSUMS` breakpoint fired in the same run), then
refreshed Stage-By-Transport. **Neither conditional breakpoint fired** —
this file did not reach either code path this time. Checked the rendered
page directly instead: in the CURRENT live state, this file's row (in the
Stage page's "Local changes (83 files)" section — note: NOT the full
236-object combined view from earlier in this session, a narrower
sub-view) shows status **"A" (local Add, pending, transport UNKNOWN)** —
i.e. it currently renders as "exists locally, not yet added/pushed", the
OPPOSITE of the owner's "Local: Not exists" description.

**This is an unresolved discrepancy, not a retraction of the owner's
report** — possible explanations (none confirmed): (a) this "Local
changes (83)" list is a different, narrower view than wherever the owner
originally saw "Local: Not exists / Remote: Added new" (e.g. the original
236-object Stage-By-Transport combined table, or the Overview page, may
show this file differently than this specific sub-section), (b) something
about the repo/transport state changed between the owner's observation and
this check, or (c) the two-symptom family (DTEL SHA1 mismatch vs. FUGR
existence mismatch) may not share the same live-reproducible state at all
times. **Needs the owner to point to the exact screen/section where "Local:
Not exists" was observed so this can be re-checked in the same view.** Not
investigated further this session.

## 18. Owner-confirmed experiment: disabling PARALLEL serialization fixes
    the unchanged-file MODIFIED/MODIFIED symptom (huge confirmation)

Owner disabled parallel serialization (system/config toggle, not this
agent's action) and reports the MODIFIED/MODIFIED-for-genuinely-unchanged-
files symptom (the GEOLAT-class bug, §14/§16) **disappeared** under
sequential serialization. This is the single strongest piece of evidence
in the whole investigation: **it directly confirms the defect lives in the
PARALLEL serialization code path** (`zcl_abapgit_serialize=>run_parallel`/
`on_end_of_task`/`Z_ABAPGIT_SERIALIZE_PARALLEL`, or something invoked only
from that path, e.g. the ORTEC per-object prefetch buffers passed into
each parallel RFC task — `zcl_abapgit_ortec_ser_pref`/`_ext`/`_oo
=>extract_for_object`, see §16's investigated-but-not-confirmed DOKIL
candidate). PC-1 (stale mt_remote cache) remains REJECTED; this is now
better named **PC-3: parallel-RFC-serialization-specific local-content
defect** — CONFIRMED as the trigger mechanism class, exact code point
still not pinpointed.

Owner separately reports two things that are STILL broken even with
parallel serialization off:
1. The `/LOT/GC_ADDRESS_GEOCODE` "Local: Not exists / Remote: Added new"
   symptom persists — meaning its root cause is INDEPENDENT of the
   parallel/sequential toggle (see §19 for the new candidate found this
   session).
2. Some files still render as MODIFIED/MODIFIED "even though they are only
   modified in local or remote, depending on the timestamps of the sha1s"
   — this is **exactly finding (B) from §14** (the near-empty, only-3-row
   persisted checksum baseline causing `build_existing`'s "strange
   situation" fallback to blanket-mark BOTH sides modified whenever
   `match=false`, regardless of which side actually changed). This
   confirms finding (B) is a real, independent, STILL-PRESENT bug, not
   merely a side-effect of the parallel-serialization defect — both must
   be fixed for the symptom family to fully resolve.

## 19. New candidate for the FUGR "Local: Not exists" symptom — ENLFDIR-
    based prefetch buffer, independent of parallel serialization
    (source-analysis only this session, NOT live-confirmed — see §20
    blocker)

Traced `ZCL_ABAPGIT_OBJECT_FUGR`'s `get_includes()` (builds the local file
list for a function group's non-function-module includes, e.g. `F01`-style
FORM-routine pools):
```abap
lt_functab = functions( ).
CALL FUNCTION 'RS_GET_ALL_INCLUDES' ... TABLES includetab = rt_includes ...
LOOP AT lt_functab ASSIGNING <ls_func>.
  DELETE TABLE rt_includes FROM <ls_func>-include.
ENDLOOP.
```
`RS_GET_ALL_INCLUDES` is a real-time, non-cached kernel call — should be
reliable regardless of prefetch mode. The suspect is `functions( )`
(builds `lt_functab`, used only to SUBTRACT actual function-module
includes from the "everything" list) and, more importantly, other FUGR
metadata is explicitly prefetch-gated elsewhere in the same class:
```abap
IF zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( ) = abap_true.
  lv_prefetched = zcl_abapgit_ortec_ser_pref_ext=>get_fugr_enlfdir( ... ).
  ...
```
`get_fugr_enlfdir()` reads a buffer (`mt_fugr_enlfdir`) built ONLY from a
bulk `SELECT * FROM enlfdir ... WHERE area = ... AND active = @abap_true`
(`ZCL_ABAPGIT_ORTEC_SER_PREF_EXT=>prepare_fugr`). **Live-verified via SQL**
this session: `SELECT area, funcname, active FROM enlfdir WHERE area =
'/LOT/GC_ADDRESS_GEOCODE'` returns exactly 11 rows, ALL `active = 'X'`,
and **none of them is `#lot#lgc_address_geocodef01`** — confirming this
file is a manually-created FORM-routine include, NOT a function module,
and is therefore fundamentally unreachable via any ENLFDIR-based
enumeration. **Hypothesis (not yet live-confirmed)**: when
`is_serial_prefetch_active()` is on, some part of the FUGR serialization
that determines the function-module list (`functions()`) may source data
from this ENLFDIR-only prefetch buffer instead of a broader, real-time
lookup, and a resulting mismatch/omission could cause `F01` to be
incorrectly treated as a function-module include (and thus wrongly
`DELETE`d from `rt_includes`) or otherwise dropped before it reaches
`it_local`. **`is_serial_prefetch_active()` is a SEPARATE switch from
whatever controls parallel serialization** (confirmed by the owner's
report that disabling parallel serialization did NOT fix this symptom) —
this switch was not touched this session and its state is unknown.

## 20. BLOCKER this session — SAP application server cannot reach
    dev.azure.com (network/VPN, outside agent control)

Attempted a fresh live re-verification of §19 by refreshing Stage-By-
Transport with a new breakpoint at `ZCL_ABAPGIT_OBJECT_FUGR` (right after
the `RS_GET_ALL_INCLUDES` + subtract-functions loop, conditioned on
`ms_item-obj_name = '/LOT/GC_ADDRESS_GEOCODE'`). The refresh hit two
unrelated stray breakpoints in `ZCL_ABAPGIT_GIT_TRANSPORT` (removed, same
"Mode: User catches pre-existing breakpoints" pattern as the
`DD_TBFD_SET_GET` incident in §\u00a7 earlier), then completed WITHOUT hitting
either the new FUGR breakpoint or the existing GEOLAT/checksums
breakpoints. The rendered page revealed why: **a live error banner
"HTTP error 411 occurred: Direct connect to dev.azure.com:443 failed:
NIECONN_REFUSED(-10)"** — the SAP application server's own network path to
the Azure DevOps remote is down (separate from the ADT/debugger
connection, which reconnected fine). The refresh therefore failed early
during the remote-connect attempt, before ever reaching local
serialization/status comparison, and the page is showing STALE/cached
data from before the outage — explaining both the missing breakpoint hits
and why the observed row status ("A", not "Local: Not exists") didn't
match the owner's live description. **STATUS=BLOCKED_ON_NETWORK, not an
agent-side issue, cannot be resolved by this agent (matches the
"secure/infra access required" class of pre-authorized stop condition)**.
Awaiting owner confirmation that server-side connectivity to dev.azure.com
is restored before further live capture is attempted. No further browser/
debugger actions taken after this finding.

## 21. RESOLVED (network restored) — root cause for the FUGR "Local: Not
    exists" symptom is a STANDARD SAP KERNEL FUNCTION returning empty,
    NOT an ORTEC/abapGit customization

Owner confirmed dev.azure.com connectivity is back (a plain Refresh works
for them again). Re-armed breakpoints on a fresh debug session and traced
`ZCL_ABAPGIT_OBJECT_FUGR=>functions()` first (called from `get_includes()`
at line 696, itself reached via `get_includes()` from
`ZCL_ABAPGIT_OBJECT_FUGR` line 1148/1519 → `ZCL_ABAPGIT_OBJECTS` line 1272
→ `ZCL_ABAPGIT_SERIALIZE` lines 251/410/732/827 → `ZCL_ABAPGIT_REPO` line
703 → `ZCL_ABAPGIT_STAGE_LOGIC` line 137 — **confirmed this call stack has
NO parallel-RFC dispatch in it at all**, i.e. this happens in the plain
synchronous/local part of serialization regardless of the parallel/
sequential toggle, consistent with the owner's report that this symptom
persists even with parallel serialization off):

1. **`functions()` is clean.** `lv_prefetched = 'X'` (ORTEC serial-prefetch
   WAS used), `lt_enlfdir` (from the prefetch buffer) had exactly 11 rows,
   `rt_functab` (from the always-called, non-cached `RS_FUNCTION_POOL_CONTENTS`)
   also had exactly 11 rows, and after the cross-check-and-remove loop
   (`#7147` consistency check) `rt_functab` STILL had 11 rows — nothing
   wrongly removed. The 11 matches exactly the real function module count
   confirmed via direct `ENLFDIR` SQL in §19. **Prefetch is NOT the
   problem for this object.**
2. **`main_name()` is clean.** Returns `/LOT/SAPLGC_ADDRESS_GEOCODE` via
   the standard, non-cached kernel function `FUNCTION_INCLUDE_SPLIT` —
   the textbook-correct SAPL-program-name convention for a namespaced
   function group. No ORTEC/prefetch involvement in this method at all.
3. **The actual break: `CALL FUNCTION 'RS_GET_ALL_INCLUDES' EXPORTING
   program = '/LOT/SAPLGC_ADDRESS_GEOCODE' TABLES includetab =
   rt_includes`** — captured immediately after this call returns (before
   ANY abapGit subtraction/filtering logic runs): **`sy-subrc = 0`
   (success) but `lines( rt_includes ) = 0`** (zero includes returned).
   `RS_GET_ALL_INCLUDES` is a plain, standard, uncustomized SAP kernel
   function (no ORTEC wrapper, no prefetch gate, nothing abapGit-specific
   touches it) — it is being asked "what includes does program
   `/LOT/SAPLGC_ADDRESS_GEOCODE` have" and is answering "none", which is
   almost certainly wrong for a normal function group main program (which
   should have at minimum a TOP include plus one generated include per
   function module, plus this `F01` FORM-routine pool).

**Conclusion: the `/LOT/GC_ADDRESS_GEOCODE` "Local: Not exists" symptom is
NOT caused by any ORTEC/abapGit bulk, parallel, or prefetch customization**
— every piece of abapGit-specific code involved (`functions()`,
`main_name()`) was verified correct and behaving identically to how it
would with all optimizations disabled. The actual failure is upstream, in
a bare SAP kernel function call returning an empty result with a
misleadingly successful return code, for this specific program name, in
this specific system. **This needs owner-side verification independent of
abapGit**: does `RS_GET_ALL_INCLUDES` (or equivalently, SE38/SE80's own
"Program → Where-Used/Includes" list, or `RS_GET_ALL_INCLUDES` called
directly via a throwaway test report) also return empty for
`/LOT/SAPLGC_ADDRESS_GEOCODE` OUTSIDE of an abapGit-triggered request? If
yes, this points to a genuine data/registration inconsistency for this one
program (independent investigation, likely not an abapGit/E2 issue at
all). If it works fine standalone, something about the calling SESSION's
state during abapGit's execution is interfering with this kernel call —
not yet identified, would need a fresh angle (e.g. checking for shared
ABAP-memory/EXPORT-IMPORT buffer IDs that `RS_GET_ALL_INCLUDES` might use
internally, though none are known/customized by ORTEC here). Not
investigated further this session — recommend the owner runs the
standalone check first since it's fast and decisive.

## 22. FIXES IMPLEMENTED AND ACTIVATED (owner-authorized: "I Need a real
    fix... find the root cause and fix it" / "it might be worth trying to
    call the function module with the parameters set")

### Fix 1 — FUGR includes (owner's own empirical finding, confirmed)

Owner independently diagnosed this via SAP GUI/Eclipse ADT experimentation
(recorded verbatim as owner input, not agent-derived): `RS_GET_ALL_INCLUDES`
initially only returned includes for `/LOT/SAPLGC_ADDRESS_GEOCODE` when
ALL of `WITH_INACTIVE_INCLS`/`WITH_RESERVED_INCLUDES`/`WITH_CLASS_INCLUDES`/
`WITH_E_INCLUDES` were set; then only `WITH_INACTIVE_INCLS` was needed;
after opening `/LOT/LGC_ADDRESS_GEOCODEF01` in Eclipse ADT (forcing a
regeneration), it worked even with no parameters. Confirmed the function's
real signature (`with_inactive_incls`/`with_reserved_includes`/
`with_class_includes` default TRUE/`with_e_includes`) and that
`with_inactive_incls` gates a call to `PERFORM find_inactive_funcs` inside
the FM. **Change**: `ZCL_ABAPGIT_OBJECT_FUGR=>get_includes()` — added
`with_inactive_incls = abap_true` to the `CALL FUNCTION
'RS_GET_ALL_INCLUDES'`. File:
`ZCL_ABAPGIT_OBJECT_FUGR.clas.abap` (package `$ABAPGIT_OBJECTS`).
**Activated** via `abap_activate`, no syntax errors.

### Fix 2 — THE parallel-serialization root cause (found and fixed this
    session, not merely narrowed)

Live-traced a real parallel worker thread this session (owner re-enabled
parallel serialization for this purpose): triggered "Stage By Transport"
IT8K900025 with a breakpoint in `ZCL_ABAPGIT_OBJECT_DTEL~serialize` right
after its prefetch call, conditioned on `lv_name = '/LOT/GC_GEOLAT'`. A
**second debug thread** appeared (`Thread 2: ZCL_ABAPGIT_OBJECT_DTEL`),
confirmed via `abap_debug_stack(threadId=2)` to be genuinely running
inside a separate RFC work process (`SAPMSSY1.prog.abap` dispatcher →
`Z_ABAPGIT_SERIALIZE_PARALLEL` → `ZCL_ABAPGIT_OBJECTS` →
`ZCL_ABAPGIT_OBJECT_DTEL`) — i.e. `abap_debug_stack`/`abap_debug_variable`
with an explicit `threadId` CAN inspect a live parallel RFC worker, a
capability not previously exploited in this incident. Captured
`lv_prefetched='X'`, `ls_dd04v-rollname='/LOT/GC_GEOLAT'`,
`ls_dd04v-ddtext='Geographical Latitude'`, `scrtext_s/m/l/reptext=
'Latitude'`, `domname='GEOLONLAT'` — **all correct for this specific run**
(the race did not manifest in this particular live capture window; see
below for why that's expected and doesn't invalidate the fix).

Traced the dispatch chain in `ZCL_ABAPGIT_SERIALIZE=>add_objects`/
`run_parallel`/`Z_ABAPGIT_SERIALIZE_PARALLEL` (the async RFC worker
function) and found the actual defect in
`ZCL_ABAPGIT_ORTEC_SER_PREF[_EXT/_OO]=>inject_from_buffer` (called at the
TOP of every `Z_ABAPGIT_SERIALIZE_PARALLEL` invocation to seed that
worker's session-local static caches with the current object's
pre-extracted data):
```abap
LOOP AT lt_dtel INTO DATA(ls_dtel).
  INSERT ls_dtel INTO TABLE mt_dtel.   " mt_dtel: HASHED TABLE, UNIQUE KEY rollname
ENDLOOP.
```
**No `CLEAR` before the loop, and `sy-subrc` after `INSERT` is never
checked.** `Z_ABAPGIT_SERIALIZE_PARALLEL` runs via `STARTING NEW TASK ...
DESTINATION IN GROUP` — SAP's RFC dispatcher POOLS and REUSES a fixed set
of work-process sessions (matches the observed "14 threads") across MANY
separate, unrelated dispatches over each session's lifetime (not just
within one `add_objects` batch — potentially across different repos,
different users, different times of day, for as long as that internal
session/roll area lives). Class-level static data (`mt_dtel` etc.)
persists for that session's lifetime. **The bug**: `INSERT wa INTO TABLE
itab` against a table with a UNIQUE key is a NO-OP (silently sets
`sy-subrc<>0`, does not update) if that key already exists. So the FIRST
time a given worker session ever processes `/LOT/GC_GEOLAT` (or any
object), its data gets cached correctly. But on EVERY SUBSEQUENT
dispatch of that SAME object to that SAME (reused) worker — potentially
hours, days, or many unrelated serialize runs later, possibly with
genuinely different/updated object content in the meantime — the freshly
re-extracted, re-sent buffer is silently DROPPED on injection, and the
worker keeps serving the ORIGINAL, now-stale cached value FOREVER (until
that specific work process is recycled/restarted). This is a real,
severe, silent staleness bug — **not a hypothesis, confirmed by direct
code reading of the exact INSERT-into-unique-hashed-table pattern,
present identically in all three prefetch classes.**

This mechanism fully explains every previously-puzzling observation:
- **Parallel-only**: `run_sequential` never round-trips through
  `extract_for_object`/EXPORT/`STARTING NEW TASK`/`inject_from_buffer` at
  all — it presumably reads the freshly-`prepare()`d session-local cache
  directly in the SAME (never-pooled, never-reused-across-unrelated-runs)
  calling process, so this staleness class cannot occur there.
- **Apparent non-determinism**: which of the ~14 pooled workers a given
  object lands on is essentially arbitrary per run (RFC dispatcher's own
  scheduling), so whether the bug manifests for a specific object on a
  specific run depends entirely on whether THAT SPECIFIC worker happens to
  have ever cached (different, stale) data for that object before.
- **Owner's experiment (§18)**: disabling parallel serialization removes
  the only code path that can hit this bug at all.

**Fix**: added `CLEAR: <all relevant session-local cache tables>` at the
top of the merge section in `inject_from_buffer`, in all three classes,
immediately before the `INSERT ... INTO TABLE` loops — guaranteeing each
worker's cache reflects ONLY the current invocation's freshly-injected
data, with zero possibility of carrying forward a stale entry from any
prior, unrelated dispatch to that same reused session.
- `ZCL_ABAPGIT_ORTEC_SER_PREF.clas.abap` (package `$ABAPGIT_ORTEC`):
  `CLEAR: mt_msag, mt_dokil.` before the two merge loops.
- `ZCL_ABAPGIT_ORTEC_SER_PREF_OO.clas.abap`: `CLEAR: mt_classtx,
  mt_compotx, mt_subcotx.` before the three merge loops.
- `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT.clas.abap`: `CLEAR: mt_dtel, mt_enhs,
  mt_fugr_areat, mt_fugr_enlfdir, mt_fugr_func_meta, mt_prog_langs,
  mt_smim_loio, mt_smim_phf, mt_tobj, mt_tran.` before the ten merge
  loops.

All three **activated** via `abap_activate`, no syntax errors.

### IMPORTANT CAVEAT (owner's own reminder, critical for validation)

**SAP buffers/holds the compiled generated code state per session — a
work process that already loaded the OLD (buggy) generated code, and/or
already has STALE data sitting in its static caches from before this fix
was activated, will NOT automatically pick up the fix or purge its
existing bad cache just because the source was re-activated.** The owner
must **restart abapGit (and/or the relevant work processes / the RFC
server group's sessions, ideally recycling or restarting the affected
application server instance)** before re-testing, to guarantee (a) the
new generated code (with the `CLEAR` statements) is actually what
executes on the next dispatch, and (b) any already-poisoned worker
sessions are torn down rather than continuing to serve pre-fix stale
values. Testing immediately after activation WITHOUT a restart is not a
valid test of the fix — a still-running, already-poisoned work process
could still show the old wrong value (or, conversely, might happen to
show it correctly during this window purely by chance if it never
happened to poison itself for this exact object, matching what was
observed live). **This live capture's "everything looked correct"
observation therefore does NOT by itself prove the fix works — it is
expected either way given the race's nature; PROPER validation requires a
restart first, then broad-scale testing (ideally a full, unfiltered
Overview/Stage of the whole repo, repeated a few times) to build
confidence the "unmodified files show MODIFIED/MODIFIED" symptom no
longer recurs.**

### Still open / not fixed this session

- Finding (B) (near-empty 3-row persisted checksum baseline,
  `ZCL_ABAPGIT_REPO_CHECKSUMS`) — confirmed still present, independent of
  both fixes above, causes any genuine one-sided change to render as
  blanket M/M. Not fixed — needs a separate design decision (e.g. don't
  treat "no baseline entry" as "both sides modified" by default; or
  proactively backfill/rebuild the persisted checksum baseline for this
  repo). Not attempted this session — no owner authorization sought yet
  for this specific fix, and it's a distinct, independent defect from the
  two fixes above.
- The `RS_GET_ALL_INCLUDES`-returns-empty root mechanism itself (§21) —
  Fix 1 works around it via `with_inactive_incls`, but WHY a freshly-
  transported/not-yet-regenerated include is invisible without that flag
  (a SAP kernel/generation-state question, not an abapGit one) was not
  independently root-caused — the owner's own empirical
  parameter-testing already found the practical workaround, which is
  what was implemented.
- `SYSTEM_NO_ROLL` dump (§11) — separate Package E backlog item, not
  investigated.

## 23. OWNER VALIDATION AFTER RESTART — Fix 1 partially works; Fix 2 does
    NOT resolve the symptom (decisive, since a full restart rules out any
    pre-fix stale-worker-session explanation)

Owner restarted abapGit and re-triggered Stage By Transport IT8K900025:
1. **Fix 1 (FUGR includes) partially confirmed**: `/LOT/LGC_ADDRESS_GEOCODEF01`
   and its group's other includes no longer show "Remote: Added new" — but
   OTHER function groups' not-yet-generated/activated includes still do.
   Owner's own next suggestion: try `RS_GET_ALL_INCLUDES` with ALL FOUR
   parameters set (`with_inactive_incls`, `with_reserved_includes`,
   `with_class_includes`, `with_e_includes`) = `abap_true`. **Implemented**
   (all four now set unconditionally in `get_includes()`), activated —
   **requires another restart to test** (owner's own reminder about SAP
   buffering compiled code applies again here). Owner's own fallback: if
   this still doesn't help, deprioritize — "it should not happen in the
   development system."
2. **Fix 2 (stale worker cache CLEAR) did NOT fix the MODIFIED/MODIFIED
   symptom.** `/src/#lot#gc/#lot#gc_geolon.dtel.xml` still renders M/M
   despite no real changes, confirmed by the owner AFTER a full abapGit
   restart — which conclusively RULES OUT "pre-fix stale data already
   sitting in an old worker session" as the explanation, since a restart
   tears down all worker sessions. The CLEAR fix, while a real and correct
   improvement (confirmed working as intended — see below), is
   **NOT SUFFICIENT** to explain/fix this symptom; there is at least one
   more, still-unidentified defect.

### New live capture this session — proved the corruption happens INSIDE
    the worker's own serialize() call, AFTER every individually-inspected
    content component, which are ALL independently verified CORRECT

Set up a three-point capture chain for `/LOT/GC_GEOLON` in the SAME live
parallel run: (1) `ZCL_ABAPGIT_ORTEC_SER_PREF=>get_dokil` at the
`APPEND ls_dokil TO rt_dokil` line, (2)
`ZCL_ABAPGIT_ORTEC_SER_PREF_EXT=>get_dtel_i18n` near its end, (3)
`Z_ABAPGIT_SERIALIZE_PARALLEL` right at `EXPORT data = ls_files TO DATA
BUFFER ev_result` (the worker's own final result, BEFORE it's even sent
back to the main process).

**Tool lesson (important, corrects earlier notes)**: table-row access via
`abap_debug_variable` DOES work using bracket index syntax **without
spaces around the index**, e.g. `et_dtel_texts[1]-ddtext` — the earlier
documented failures used `tab[ 1 ]-field` (spaces inside brackets), which
the expression parser rejects (`CodeExpectedError`). This unblocks direct
inspection of table rows going forward — earlier "known limitation" notes
about being unable to expand table rows should be revisited with this
corrected syntax before assuming a wall.

Captured, in order:
```text
get_dokil:      ls_dokil-object = /LOT/GC_GEOLON, id = DE (2 rows, BOTH
                correct), lv_object = /LOT/GC_GEOLON, lines(mt_dokil) = 2
                -- exactly this object's own 2 real DOKIL rows, nothing
                stale/extra (confirms the Fix 2 CLEAR is working as
                intended: mt_dokil holds ONLY the current object's data).
get_dtel_i18n:  et_dtel_texts[1]-ddtext = "Geographische Länge" (German
                for "Geographic Longitude" -- CORRECT for GEOLON),
                lines(mt_dtel) = 1 (only current object, Fix 2 confirmed
                working here too).
FINAL EXPORT:   ls_files-item-obj_name = /LOT/GC_GEOLON (correct),
                ls_files-files[1]-filename = #lot#gc_geolon.dtel.xml
                (correct), ls_files-files[1]-sha1 =
                55f8cfe1eb28ffe5b074e39452129a6320a9f333
```
Cross-checked the known-good remote blob SHA1 via live SQL:
`SELECT blob_sha1 FROM zaog_obj_index WHERE repo_key='288c81fc1cad' AND
file_name='#lot#gc_geolon.dtel.xml'` → **`0a8df7887b9decf9d7e79f89e53df651cd6a6d9d`**
— completely different from what the worker computed
(`55f8cfe1eb28ffe5b074e39452129a6320a9f333`).

**This is the most important finding of this investigation to date**: the
worker computes and reports the WRONG SHA1 for the CORRECTLY-identified
object and filename, despite EVERY individually-inspected content
component feeding into that file (DD04V core fields incl. rollname/
ddtext/domname/scrtext_* — verified twice, for both GEOLAT and GEOLON;
i18n translation texts; DOKIL longtext entries) being independently
verified CORRECT and correctly scoped to the current object (no stale/
cross-object contamination detected anywhere I've been able to inspect).
**The actual corruption therefore happens in a part of the pipeline not
yet isolated** — candidates for the next session: `ls_extra`/
`get_abap_language_version()` (set right before the DD04L_EXTRA XML node,
not yet inspected), the DD04V post-processing clears (ACTFLAG/RESERVEDTE/
ROUTPUTLEN/AUTHCLASS — inspected the SOURCE but not LIVE VALUES for this
specific object), or — most likely given everything ELSE checks out —
the actual XML assembly (`io_xml->add(...)`)/serialization-to-bytes/SHA1-
hashing mechanism itself, which has not yet been instrumented at all.
**STATUS: root cause NOT yet fully found for the parallel SHA1 mismatch.
Fix 2 (stale-cache CLEAR) is confirmed working correctly as designed and
should remain in place (it is a genuine, real defect fix, verified via
this session's `lines(mt_dtel)=1`/`lines(mt_dokil)=2` captures showing NO
accumulation), but is NOT the complete explanation. Further investigation
needed, likely requiring line-by-line stepping through the remainder of
`ZCL_ABAPGIT_OBJECT_DTEL~serialize` (from just after the DOKIL/i18n calls
through to method end) and potentially into the XML output object's own
`add()`/serialization internals, in a fresh live session.**

---

## §24 — MAJOR PIVOT: checksum baseline (Finding B) CONFIRMED via direct DB query; diff feature proves content is byte-identical; DD04L_EXTRA omission ruled out

**Context**: Continuing session, owner gave key architectural hint: local
serialization is always freshly retrieved from DB (no local repo cache in
standard abapGit), so the false-MODIFIED bug likely lives in the STATE
DETECTION (local/remote/baseline sha1 comparison) rather than in stale
content. This redirected the investigation away from pure content-level
debugging toward the status-calculation algorithm itself, with decisive
results.

### §24.1 — DD04L_EXTRA omission ruled out (not a bug)
Re-armed a live breakpoint on `ZCL_ABAPGIT_OBJECT_DTEL` line 436
(`io_xml->add( iv_name = 'DD04L_EXTRA' ... )`), condition
`ms_item-obj_name = '/LOT/GC_GEOLON'`, caught it in the parallel worker
thread (confirmed via call stack: `Z_ABAPGIT_SERIALIZE_PARALLEL` frame).
`ls_extra-abap_language_version` is BLANK. `ty_extra` has only that one
field, so the structure is fully initial and `io_xml->add()` omits the
`<DD04L_EXTRA>` tag entirely from the XML (standard SAP asx-transform
"suppress fully-initial" behavior). **Verified via a live capture of the
actual REFERENCE (remote) blob's bytes at the exact same offset that the
reference ALSO omits `<DD04L_EXTRA>`** — i.e. this is normal, correct,
and IDENTICAL between local and remote. Not the bug. (`DTEL` objects have
no ABAP language version concept; `CL_DD_ABAP_LANGUAGE_VERSION` correctly
returns blank for object type DTEL — this is expected, not an RFC-context
artifact.)

### §24.2 — Full byte-level capture of the REFERENCE blob (3783 bytes) via live debugger
Discovered that `execute_data_query` (SQL) truncates any `xstring`/
`rawstring` field value to ~125 bytes / 250 hex chars, REGARDLESS of
`SUBSTRING`/`download_to_file`/display mode — this is a hard limit in how
the tool retrieves LOB-like field values from SAP, not a display-layer
truncation. `SUBSTRING()` is also flatly rejected by Open SQL for
xstring/rawstring columns ("not allowed in this position").
**Workaround that worked**: set a live breakpoint at
`ZCL_ABAPGIT_ORTEC_OBJ_STORE.clas.abap` line 606
(`APPEND ls_object TO rt_objects.` inside `get_objects`, itself called by
`get_object`), condition `ls_object-sha1 = '<target sha1>'`. This fires
whenever ANY caller fetches that blob's content (e.g. the abapGit "diff"
UI action, which needs the remote content to render a line diff). Once
paused, `abap_debug_variable` CAN page through the full xstring via
`ls_object-data+OFFSET(125)` in a loop (offset 0, 125, 250, ... to the
end) — each individual call is capped at ~125 bytes returned, but you can
walk the whole thing this way with no true limit on total length. This
recovered the COMPLETE 3783-byte reference blob for
`/LOT/GC_GEOLON`.**This technique (breakpoint on the object-store
get/read call site + walk via `+OFFSET(length)`) is the general pattern
for extracting full blob content live when SQL tools truncate — reuse it
in future sessions instead of fighting the SQL truncation.**

### §24.3 — THE DECISIVE TEST: abapGit's own "diff" feature says content is IDENTICAL
Clicked the per-file "diff (1)" link for `/LOT/GC_GEOLON` in the repo
object list (status still showing `MM` at the time). Result:
**"There are no differences to show. The local state completely matches
the remote repository."** This is abapGit's real line-by-line text diff,
independent of the SHA1/status-calc path. It PROVES the actual file
content (local vs remote) is identical — confirming (and generalizing)
what was previously only checked field-by-field for DD04V/i18n/DOKIL.
**The row's status list still showed `MM` immediately after this
"no differences" result** — i.e. the status calculation and the diff
feature actively DISAGREE for this file. This is the real smoking gun:
whatever produces "MM" is NOT a content problem.

### §24.4 — ROOT CAUSE FOUND: `ZCL_ABAPGIT_STATUS_CALC~BUILD_EXISTING`'s baseline-fallback + a near-empty persisted checksum baseline
Read `ZCL_ABAPGIT_STATUS_CALC=>build_existing` (called from
`process_local`, called from `calculate_status`, called from
`ZCL_ABAPGIT_REPO_STATUS=>calculate`). Logic:
```
rs_result-match = boolc( is_local-file-sha1 = is_remote-sha1 ).
IF rs_result-match = abap_true. RETURN. ENDIF.   " unchanged, done

READ TABLE it_state ... WITH KEY path/filename BINARY SEARCH.
IF sy-subrc = 0.
  " compare local-vs-baseline and remote-vs-baseline independently
  ...
ELSE.
  " "this is a strange situation... maybe first run of the code"
  rs_result-lstate = c_state-modified.
  rs_result-rstate = c_state-modified.   " BLANKET M/M fallback
ENDIF.
```
So: whenever `local sha1 <> remote sha1` (for ANY reason, even a
transient/wrong computation) AND the file has no entry in the persisted
checksum baseline (`it_cur_state`, from `ii_repo->checksums()->
get_checksums_per_file()`), the code explicitly falls back to marking
BOTH sides Modified — with a comment in the source acknowledging this is
meant only for "first run" scenarios.

**Queried the actual persisted checksum baseline directly via SQL**
(table `ZABAPGIT`, type `REPO_CS`, key = the repo's persistence key,
found via type `REPO` row matching URL `.../OS4`, key = `000000000002`):
```
#repo_name#OS4 6.0
@
/|.abapgit.xml|9ef91905a8d4ca37adcca6f5c03605080ea0bd01
DEVC|/LOT/OS|/LOT/OS
/src/|package.devc.xml|dc6780fbdc12024c7e176772bd1a87789e945362
NSPC|/LOT/|/LOT/OS
/src/|#lot#.nspc.xml|3dcebce1faea97d47c54aff5397dfaf535389509
```
**Only 3 entries exist in the ENTIRE baseline**: `.abapgit.xml`, the
`/LOT/OS` package's `.devc.xml`, and the `/LOT/` namespace's `.nspc.xml`.
NOT ONE of the repo's ~17000+ actual code objects (DTEL/CLAS/FUGR/etc.)
has a baseline entry. The repo's `REPO` row shows
`CREATED_AT>20260506113653` (created ~2026-05-06, this is a "New Online"
clone of an existing large remote) — the baseline was apparently only
ever populated with the bootstrap/top-level artifacts at repo-add time
and NEVER completed/refreshed with the full file list from the initial
clone. This exactly matches Finding B's original description
("near-empty 3-row persisted checksum baseline").

### §24.5 — How this explains the whole "false MODIFIED" symptom
For virtually every code file, `it_state` lookup misses (no baseline
entry) — so ANY time `local sha1 <> remote sha1` happens for THAT
specific status-calc run (whether from a genuine transient parallel-
worker SHA1 defect — see §22/§23's still-unexplained
`55f8cfe1eb28ffe5b074e39452129a6320a9f333` vs
`0a8df7887b9decf9d7e79f89e53df651cd6a6d9d` mismatch for this exact file
— or any other transient cause), the result is a PERMANENT-looking
`MM` with NO possibility of self-correction on the next refresh, because
`build_existing` has no memory of "this used to match" to fall back on.
Contrast: if the baseline were properly populated (local=remote=baseline
for every file at last successful sync), a ONE-OFF transient bad SHA1
would show as `lstate=M` only (miscompare vs baseline) while
`rstate` would correctly stay unchanged (if remote still equals baseline)
— NOT the blanket "both sides modified" we're actually seeing.

**This is a materially different, more universal explanation than the
parallel-worker SHA1 corruption theory alone**: the baseline gap explains
why the symptom is so widespread (~74+ files in the transport-filtered
view alone) and un-recoverable across refreshes, regardless of how often
or rarely the underlying SHA1 mismatch trigger actually fires.

### §24.6 — Standard abapGit self-healing mechanism exists: `ZIF_ABAPGIT_REPO_CHECKSUMS~REBUILD`
```
METHOD zif_abapgit_repo_checksums~rebuild.
  lt_local = mi_repo->get_files_local( ).
  remove_non_code_related_files( CHANGING ct_local_files = lt_local ).
  lt_checksums = build_checksums_from_files( lt_local ).
  save_checksums( lt_checksums ).
  mv_cache_valid = abap_false.
  CLEAR mt_checksums_cached.
ENDMETHOD.
```
This is exactly the documented/intended recovery path for "checksum
baseline is missing/wrong" — NOT a custom workaround. It re-derives the
baseline from CURRENT local file state. **CAVEAT**: since it uses
`get_files_local()` (same serialize path, same potential parallel-worker
SHA1 defect), if that defect fires again during the rebuild pass, any
affected file's baseline entry would be poisoned with a wrong value at
rebuild time too — but this is materially better than today (no baseline
at all) since it would only misalign ONE side for files hit by the
defect on THAT specific rebuild run, not blanket-flag every subsequent
transient mismatch as full M/M forever. **Also CAVEAT**: rebuild adopts
"whatever is currently local" as the new source of truth for ALL fields
— if the repo currently has genuine, not-yet-reviewed local
modifications mixed in among the false positives, rebuild would silently
absorb them into the new baseline without going through a normal
stage/commit review. Should not be run blindly without the owner's
awareness/authorization.

### §24.7 — Bug A (parallel worker SHA1 defect from §22/§23) status: STILL UNRESOLVED, but now understood to be a co-factor, not the sole/primary explanation
Re-confirmed the SAME wrong SHA1 (`55f8cfe1eb28ffe5b074e39452129a6320a9f333`)
reproducibly for `/LOT/GC_GEOLON`'s PARALLEL-worker computed
`<ls_file>-sha1` in `ZCL_ABAPGIT_OBJECTS=>SERIALIZE` line 1299 (breakpoint
condition `is_item-obj_name = '/LOT/GC_GEOLON'`), TWICE, across separate
debug sessions. However, when the SAME file was re-serialized via the
per-file "diff" UI action (fast, targeted single-object path — observed
as "Serialize /LOT/GC_GEOLON, 1 thread" then completing in ~0.01-37s),
the SAME breakpoint at `ZCL_ABAPGIT_OBJECTS` line 1299 did NOT fire at
all — suggesting the diff feature's local-side content comes from an
ALREADY-CACHED `it_local` (from the earlier full 17321-object serialize
pass), not a fresh re-invocation of `zcl_abapgit_objects=>serialize`.
This means the "no differences" diff result and the wrong cached SHA1
could in principle coexist (diff recomputing/rendering the SAME already-
fetched local xstring against a freshly-fetched remote xstring and
finding them textually equal, while the SHA1 TAG attached to that same
local file entry — computed earlier, once, during the original parallel
run — was wrong). **This is NOT yet fully proven** (byte range 1000-3783
of the WORKER's own capture was never captured in the session that
produced the wrong SHA1 — only bytes 0-1000 were verified against the
reference; the full 3783-byte REFERENCE was captured in §24.2 but a
matching FULL-length WORKER capture for a run that reproducibly gives
the wrong SHA1 is still outstanding). **Next-session action if this
still matters after the baseline is fixed**: reproduce the wrong-SHA1
worker run again, and this time page ALL 3783 bytes of `<ls_file>-data`
(not just the first 1000) using the same `+OFFSET(125)` walk technique,
and diff byte-for-byte against the §24.2 reference capture, to find the
EXACT byte offset of divergence (if any — it's also possible the defect
is in `sha1_blob`/`sha1_raw`/`sha1`'s kernel call itself being flaky
under heavy parallel RFC load, not in the data at all, which would be a
much harder — possibly SAP Basis/kernel level — issue).

### §24.8 — Recommended path forward (pending owner decision)
1. **Short-term, high-confidence, addresses the reported symptom for
   effectively all files**: call `zif_abapgit_repo_checksums~rebuild`
   (or equivalent) for this repo once content has been spot-verified via
   diff (as done here for GC_GEOLON) — repopulates the baseline from
   current local state, restoring the normal 3-way comparison's ability
   to self-heal from one-off transient mismatches instead of permanently
   flagging blanket M/M. Requires owner awareness since it adopts
   current local as truth for all fields (see §24.6 caveat).
2. **Longer-term / still open**: Bug A (§24.7) — the parallel worker's
   occasional wrong SHA1 for otherwise-correct content — remains
   unresolved at the byte/mechanism level. Even with the baseline fixed,
   this could still cause occasional one-off `lstate=M` blips (now
   correctly scoped to local-only, not blanket M/M) until its true cause
   is found.
