# Variant B / Package E — E2 Diagnostic Handoff — OS4 (LARGE REPO, active incident)

```text
STATUS=ROOT_CAUSE_B_FOUND_CHECKSUM_BASELINE_NEAR_EMPTY_FIX_PENDING_AUTHORIZATION
PHASE=FIX_B_PROPOSED_AWAITING_OWNER_DECISION
E2_FIX_AUTHORIZED=YES (owner, explicit)
FIX_1=PARTIAL: with_inactive_incls fixed GC_ADDRESS_GEOCODE but not other
  FUGRs' not-yet-generated includes. Now trying all 4 RS_GET_ALL_INCLUDES
  params = abap_true (implemented, activated, needs another restart to
  test). Owner will deprioritize if still unresolved (dev-system-only).
FIX_2=CONFIRMED WORKING AS DESIGNED (stale-cache CLEAR verified live, no
  accumulation) but was NEVER the primary explanation for the MM symptom.
MAJOR_PIVOT=Root-caused the actual MM symptom to a near-empty persisted
  checksum baseline (table ZABAPGIT type=REPO_CS key=000000000002, repo
  "OS4 6.0"): only 3 bootstrap entries exist (.abapgit.xml, package
  devc.xml, nspc.xml), NONE of the ~17000+ code objects have a baseline
  row (repo created 2026-05-06, baseline never populated after initial
  clone). ZCL_ABAPGIT_STATUS_CALC~BUILD_EXISTING falls back to blanket
  lstate=MODIFIED + rstate=MODIFIED whenever local-sha1<>remote-sha1 AND
  no baseline entry exists (source comment: "maybe first run of the
  code"). DECISIVE PROOF: abapGit's own per-file "diff" for GC_GEOLON
  (while status showed MM) returned "There are no differences to show.
  The local state completely matches the remote repository." -- content
  is byte-identical, the bug is purely in status/baseline logic.
  PROPOSED FIX: ZIF_ABAPGIT_REPO_CHECKSUMS~REBUILD is the standard,
  documented self-healing method for exactly this scenario (repopulates
  baseline from current local state) -- NOT yet invoked, needs owner
  awareness/authorization since it adopts current local as truth for ALL
  fields (could mask genuine unreviewed local changes). Full details:
  D1 log §24.
TOOL_LESSON_1=abap_debug_variable table-row access works with tab[1]-field
  (no spaces) -- earlier tab[ 1 ]-field (spaces) failures were a syntax
  issue, not a real tool limitation. Use this going forward.
TOOL_LESSON_2=execute_data_query truncates xstring/rawstring field values
  to ~125 bytes regardless of SUBSTRING/download_to_file/display mode
  (hard limit, not a display issue); SUBSTRING() is also rejected outright
  for xstring/rawstring columns in Open SQL. WORKAROUND: set a live
  breakpoint at the object-store's read call site (e.g.
  ZCL_ABAPGIT_ORTEC_OBJ_STORE line 606, condition on the target sha1) and
  walk the full blob via abap_debug_variable expressions of the form
  `variable+OFFSET(125)` in a loop -- no length limit walking this way.
STILL_OPEN=(1) Bug A, the parallel worker's occasional wrong SHA1 for
  otherwise byte-identical content (GC_GEOLON reproduced twice:
  55f8cfe1eb28ffe5b074e39452129a6320a9f333 vs correct
  0a8df7887b9decf9d7e79f89e53df651cd6a6d9d) -- still not root-caused at
  the byte/mechanism level; full 3783-byte worker-vs-reference byte diff
  still outstanding (only bytes 0-1000 were cross-checked). Even after
  fixing the baseline, this could still cause occasional one-sided
  lstate=M blips until found. (2) SYSTEM_NO_ROLL dump backlog item, not
  investigated.
```

## What this replaces

The small-repo (`abapGit-testing`) D0/D1/handoff artifacts are retracted —
see
[.memory/handoffs/variant-b-package-e-e2-diagnostic.md](.memory/handoffs/variant-b-package-e-e2-diagnostic.md)
(`STATUS=SUPERSEDED_INVALID_REPRODUCTION`). This OS4 incident is the active,
genuinely reproducible E2 investigation.

## Reproduction summary

Repo OS4 (`repo_key=288c81fc1cad`, branch `refs/heads/development/6.0.x`,
tip `81157b1448b4183f38403ec63caad1291a2226a4`), DTEL `/LOT/GC_GEOLAT`

## What this replaces

The small-repo (`abapGit-testing`) D0/D1/handoff artifacts are retracted —
see
[.memory/handoffs/variant-b-package-e-e2-diagnostic.md](.memory/handoffs/variant-b-package-e-e2-diagnostic.md)
(`STATUS=SUPERSEDED_INVALID_REPRODUCTION`). This OS4 incident is the active,
genuinely reproducible E2 investigation.

## Reproduction summary

Repo OS4 (`repo_key=288c81fc1cad`, branch `refs/heads/development/6.0.x`,
tip `81157b1448b4183f38403ec63caad1291a2226a4`), DTEL `/LOT/GC_GEOLAT`
(`/src/#lot#gc/#lot#gc_geolat.dtel.xml`): repository overview AND Full Stage
both show Local MODIFIED + Remote MODIFIED for this object; the Diff action
for the same object reports "There are no differences to show; local state
completely matches remote repository." No Pull/Refresh/branch-switch/
checksum-update/Cache-Admin action was taken between the three views
(owner-reported discipline, required for the comparison to be meaningful).

## What was done this phase (read-only)

- Retracted the small-repo D0/D1/handoff artifacts with the exact structured
  `STATUS=SUPERSEDED_INVALID_REPRODUCTION` field block and an explicit
  valid-vs-invalid claims split (see the three superseded files).
- Verified `CURRENT_HEAD=8b382a5eab948f67a9ca6239293e193b70d8d38c`, working
  tree clean apart from this session's `.memory/*` edits.
- Confirmed E1-PERF-A (`c_index_write_chunk_size=30000` in
  `zcl_abapgit_ortec_obj_index=>rebuild_index`) is already implemented at
  this head and is independent of E2 (write-batch size only; no index
  content/lookup/readiness/branch-selection/status-semantic impact). Flagged
  (not fixed) a documentation mismatch: `.memory/state.md`'s design contract
  says 5000, the implemented constant is 30000 — out of scope for E2, noted
  for Package E bookkeeping only.
- Queried OS4's repo identity and current branch/commit/index state
  read-only, bounded to exact keys (`ZAOG_REPO_STATE`, `ZAOG_COMMIT_HIST`,
  `ZAOG_OBJ_INDEX`, `ZAOG_OBJ_STORE` — never repo-wide/unbounded).
- Traced the full current-source call graph for all three access modes
  (repository overview, Full Stage, single-object Diff) and found a
  **structurally confirmed asymmetry**: overview and Full Stage resolve
  remote content via the standard, session-cached
  `zcl_abapgit_repo_online`/`mt_remote` path (`get_files_remote`, gated by
  `mv_request_remote_refresh`), while single-object Diff always resolves
  remote content via the ORTEC filtered-walk facade
  (`zcl_abapgit_ortec_git_facade=>resolve_filtered_remote` →
  `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`), which
  independently re-validates the branch tip live against the remote and
  reads `ZAOG_OBJ_INDEX`/`ZAOG_OBJ_STORE` at the (possibly newer) fetch
  commit — entirely bypassing the standard cache. Full detail, exact
  file/line citations, and the hypothesis matrix (H1-H12, reconstructed
  since the owner's original H-text was not preserved verbatim across a
  mid-session compaction) are in
  [.memory/logs/variant_b_package_e_false_modified_os4_d1.md](.memory/logs/variant_b_package_e_false_modified_os4_d1.md).
- Wrote the OS4 D0 skeleton (repo identity, symptom, OBJ_INDEX/OBJ_STORE
  evidence for the exact object — all independently verified — with every
  field requiring live data explicitly marked `NOT_CAPTURED`, never
  invented) in
  [.memory/incidents/variant_b_package_e_false_modified_os4_d0.md](.memory/incidents/variant_b_package_e_false_modified_os4_d0.md).
- Did NOT assume `ZAOG_OBJ_INDEX` is used by the overview/Full-Stage flow —
  confirmed via source that it is NOT (only the single-object Diff and
  Stage-by-Transport flows reach it); its presence for this repo/commit is
  incidental (built by an earlier, unrelated filtered access), not part of
  the overview/Stage call path.
- No code was modified. No `.memory/state.md` rewrite beyond the pointer
  described below (state did not previously cite the invalid small-repo
  finding as active truth, so no retraction was needed there — only a
  pointer to this new incident was added).

## Next required action (owner)

Execute the bounded, 5-breakpoint debugger worksheet in
[.memory/logs/variant_b_package_e_false_modified_os4_d1.md](.memory/logs/variant_b_package_e_false_modified_os4_d1.md)
§5, in ONE uninterrupted session (overview → Full Stage → Diff, no
Pull/Refresh in between), and report the captured values. This will confirm
or refute candidate PC-1 (stale `mt_remote` cache vs. live ORTEC
revalidation) and populate the `NOT_CAPTURED` fields in the D0 packet.
`E2_FIX_AUTHORIZED=NO` until a live mismatch is confirmed and a fix design
is separately reviewed.

### Session update (ABAP-FS MCP, this session) — breakpoints already armed,
   awaiting only the UI trigger

Verified this session, read-only, via ABAP-FS MCP:

```text
CONNECTED_SYSTEM=it8 (get_connected_systems)
SAP_SYSTEM=IT8, CLIENT=100 (Test-System), RELEASE=758/S4HANA (get_sap_system_info)
DEBUG_AUTHORIZATION=AVAILABLE (abap_debug_session start -> Mode: User, Status: Ready for debugging)
TARGET_CLASSES_READABLE=all 9 classes named in the debugging prompt's
  pre-flight list, confirmed via get_abap_object_info
GIT_BASELINE=HEAD 8b382a5e, unchanged since the D0/D1 packets were written
  (git rev-parse HEAD / git status --short / git log --oneline -12, all
  read-only, run against the local C:\Projects\abap\abapGit memory repo —
  NOT the adt://it8 ABAP virtual filesystem)
E1_PERF_A_CONSTANT=30000, re-confirmed via direct active-source read
  (unchanged from D0; independent of E2, see D0 §3b)
OS4_DB_STATE=ZAOG_REPO_STATE / ZAOG_OBJ_INDEX / ZAOG_COMMIT_HIST re-queried
  live, byte-identical to the D0 packet's recorded values — no drift
```

10 session-scoped breakpoints are now SET (not just planned) across 7
classes — full list with exact lines/conditions in
[.memory/logs/variant_b_package_e_false_modified_os4_d1.md](.memory/logs/variant_b_package_e_false_modified_os4_d1.md)
§6. A new source-level finding this session (`ZCL_ABAPGIT_STATUS_CALC`'s
`build_existing`, D1 §3b) definitively eliminates the checksum-baseline
hypothesis (H3) as a sole cause and further narrows the remaining
explanation space to PC-1 (remote-SHA1 divergence between the
session-cached `mt_remote` path and the ORTEC facade's live-tip/OBJ_INDEX
path).

**Blocking gap (matches this task's own pre-authorized stop condition
#3):** no tool available in this ABAP-FS MCP profile can drive the abapGit
web UI itself (no SAP GUI/browser automation connected to that application).
The reproduction sequence (Overview → Full Stage → Diff) must be triggered
by the owner in their own already-authenticated IT8 SAP GUI session, using
the SAME SAP user this ADT/debug connection is logged in as, with no
Pull/Refresh/branch-switch/checksum-update/Cache-Admin action in between the
three screens. Once triggered, all further stepping and value capture will
be performed autonomously — the owner does not need to interact with the
debugger itself.

No code was modified, no breakpoints were left unaccounted for, and no
DB write occurred this session (only read-only `execute_data_query` SELECTs
and ABAP-FS source reads).

### Scope violation record

```text
SCOPE_VIOLATION=git-state-notes.md (repository memory) read despite being a
  forbidden path for this class of task. Read at the start of the OS4
  baseline-verification step in this same session, alongside the (in-scope)
  direct `git rev-parse HEAD` call. No prior scope note for THIS session
  existed; this is a new occurrence, distinct from (but the same class of
  mistake as) the two prior occurrences already logged in
  `.memory/logs/regression_variant_b_package_e_checkpoint_1.md`.
DEPENDENCY_ON_FORBIDDEN_MEMORY=NONE_REQUIRED. No claim in this OS4 D0/D1
  packet or handoff depends on git-state-notes.md content. Re-verified
  independently, directly from Git/source, after this violation was
  flagged: `CURRENT_HEAD=8b382a5eab948f67a9ca6239293e193b70d8d38c`
  (`git rev-parse HEAD`), committed 2026-07-30 10:49:18 +0200
  (`git log -1 --format="%H %ci"`), working tree unchanged except this
  session's own `.memory/*` edits (`git status --short` — the 4 pre-existing
  `M` entries under `.memory/handoffs|logs/*e1-perf-a*` predate this session
  and were not touched here; all `??` entries are this session's own new
  artifacts). `c_index_write_chunk_size TYPE i VALUE 30000` in
  `zcl_abapgit_ortec_obj_index.clas.abap` line 75 (batch trigger at line
  456) reconfirmed via direct source grep, independent of any memory file.
  Repository/editor memory (`/memories/repo/**`, `/memories/session/**`,
  `.memory/archive/**`) will not be read again for the remainder of this
  incident; all further baseline/git-state claims will be sourced directly
  from `git`/source reads.
```
