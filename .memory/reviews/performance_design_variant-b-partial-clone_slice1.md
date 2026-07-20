# Performance design review — variant-b-partial-clone / Slice 1 (durable materialization model)

Mode: `DESIGN_GATE`
Date: 2026-07-20
Reviewed: `.memory/logs/variant_b_design.md` (sections 1-5, "Mandatory performance
model", "Review resolution"), against `.github/prompts/variant-b.prompt.md`
("Mandatory performance model") and current productive source.

## Verdict: `APPROVE`

## Evidence verified against current source (not just the design doc's claims)

- `src/ortec/git/zaog_commit_hist.tabl.xml`: confirmed primary key is exactly
  `(MANDT, REPO_KEY, COMMIT_SHA1)` — the 5 new fields are a pure DDIC append,
  no key change. Matches design §1.
- `src/ortec/git/zaog_repo_state.tabl.xml`: confirmed primary key is
  `(CLIENT, REPO_KEY, BRANCH_NAME)`, `FETCH_COMMIT`/`CURR_COMMIT`/
  `IS_SHALLOW`/`DEEPEN_LVL` already exist as non-key fields. `REPO_KEY` is
  the leading key component, so the branch-cascade `WHERE repo_key = ...`
  in `invalidate_commit` is an index-bounded range scan over that repo's
  branch rows only (never cross-repo, never `ZAOG_OBJ_STORE`) — genuinely
  `O(B)`, confirmed structurally, not just asserted.
- `zcl_abapgit_ortec_fetch_neg=>is_commit_complete` (current code, lines
  ~159+): confirmed it calls `get_reachable_sha1s` — an actual reachable-set
  walk of the commit's tree/blob graph — to prove have-eligibility today.
  The design's central performance claim (replacing this with a single
  indexed row read via `is_graph_have_eligible`) is a real, verified
  improvement, not a restatement of already-good behavior.

## Required design inputs — all present (per mode instructions)

Cardinality, full method-by-method SQL shape, HTTP shape (0, stated),
transaction owner (orchestrator, zero `COMMIT WORK`, grep-verifiable target
recorded as AC2), cache scope (explicitly none for Slice 1, with a documented
deferral condition), and lookup complexity (none — pure single-row keyed
access) are all stated. Row/byte batch limits and payload/XSTRING peak are
correctly marked not applicable and justified: this class never reads or
writes `ZAOG_OBJ_STORE` or any payload, so no batching dimension exists to
omit. `clean_incomplete_attempts` is the one multi-row method and is
correctly specified as a single bounded bulk `UPDATE` keyed by
`repo_key + attempt_id <> space`, not a per-row loop.

## Scale check (1 / 1,000 / 40,000 / 1,000,000 stored objects)

Every method operates only on `ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE` by primary
key or a `REPO_KEY`-prefixed bounded range. None reference `ZAOG_OBJ_STORE`.
Cost is structurally independent of stored-object count `N` at every method
— confirmed by the absence of any `ZAOG_OBJ_STORE` reference in the class
design, not merely claimed in prose.

## Minor observations (non-blocking)

- The one-time additive-column migration `UPDATE` (space → `'U'`/`'N'`) is
  correctly scoped as a single set-based statement and a one-time activation
  cost (AC4), distinct from steady-state O(1) calls — matches the mandatory
  rule against per-row migration loops.
- No batch "verify everything now" pass exists; certification is lazy via
  `begin_attempt` → `mark_graph_complete` → `publish_snapshot_complete`,
  confirmed absent from this slice's method list and explicitly called out
  as avoiding the reconciliation-flagged O(N) `populate_cache` anti-pattern.
- Slice 1 has no bulk object-processing hot path of its own, so the
  "medium/large acceptance scenario" input is satisfied by structural
  acceptance criteria (AC1–AC5) rather than a live 40k-object load test —
  appropriate here since nothing in this slice scales with object count;
  the real 40k-scale scenarios apply to Slices 3/4/7 and must be re-verified
  at those `DESIGN_GATE`/`IMPLEMENTATION_AUDIT` passes, not skipped there.
- DR-001/DR-002 (stale `FETCH_COMMIT` readers, writer relocation) are
  correctly deferred as Slice 2/3 preconditions, not Slice 1 rework — no
  performance objection to that sequencing since Slice 1 ships no new
  reader/writer of the hot fast-path field.

## No blocking findings

No SQL/HTTP per object, no repository-wide scan, no missing batch limit, no
hidden `COMMIT WORK`, no auto-backfill of certification state.

---

# Implementation audit — variant-b-partial-clone / Slice 1

Mode: `IMPLEMENTATION_AUDIT`
Date: 2026-07-20

## Verdict: `PASS`

## Evidence reviewed (current committed source, full call chain)

- [zcl_abapgit_ortec_mat_state.clas.abap](src/ortec/git/zcl_abapgit_ortec_mat_state.clas.abap) —
  full class read line-by-line (PROTECTED/PRIVATE sections empty: no hidden
  helper methods exist, so all 9 public methods are the complete call chain).
- [zcl_abapgit_ortec_mat_state.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_mat_state.clas.testclasses.abap) —
  full test suite read (20 test methods, `CL_OSQL_TEST_ENVIRONMENT` doubles).
- [zaog_commit_hist.tabl.xml](src/ortec/git/zaog_commit_hist.tabl.xml) /
  [zaog_repo_state.tabl.xml](src/ortec/git/zaog_repo_state.tabl.xml) — confirmed
  DDIC append matches design exactly; primary keys unchanged
  (`MANDT+REPO_KEY+COMMIT_SHA1` / `CLIENT+REPO_KEY+BRANCH_NAME`).
- `zcx_abapgit_ortec_git=>raise` — confirmed a plain `RAISE EXCEPTION`, no SQL/HTTP
  side effect hidden behind the exception helper called from every guard clause.
- Local `get_errors` on both files: 0 errors. Object not yet imported onto the
  connected SAP system (404 on read) — audit is against local/committed source
  only, consistent with `.memory/state.md` phase (`design complete, awaiting
  review`, productive changes not yet promoted).

## Per-method SQL shape verification (all 9 public methods)

| Method | SQL statements found | Key used | Verdict |
|---|---|---|---|
| `get_state` | 1x `SELECT SINGLE *` | full PK (`repo_key`+`commit_sha1`) | O(1) |
| `begin_attempt` | 1x `SELECT SINGLE *` + 1x `MODIFY` (upsert) | full PK | O(1) |
| `mark_graph_complete` | 1x `SELECT SINGLE *` + 1x `MODIFY` | full PK | O(1) |
| `publish_snapshot_complete` | 1x `SELECT SINGLE *` (hist) + 1x `MODIFY` (hist) + 1x `SELECT SINGLE *` (repo) + 1x `MODIFY` (repo) | full PK both tables | O(1), 4 calls, none looped |
| `mark_full_complete` | 1x `SELECT SINGLE *` + 1x `MODIFY` | full PK | O(1) |
| `invalidate_commit` | 1x `SELECT SINGLE *` + 1x `MODIFY` (hist, full PK) + 1x set-based `UPDATE` (repo cascade) | hist: full PK; repo cascade: `WHERE repo_key = ... AND fetch_commit = ...` — `repo_key` is the leading non-client key field of `ZAOG_REPO_STATE`, so this is a primary-index-prefix range scan bounded by that repo's branch count, never cross-repo/never `zaog_obj_store` | O(1) + O(B), single statement, no loop |
| `is_graph_have_eligible` | 1x `SELECT SINGLE hist_level` | full PK | O(1) |
| `is_full_have_eligible` | 1x `SELECT SINGLE hist_level` | full PK | O(1) |
| `clean_incomplete_attempts` | 1x set-based `UPDATE ... WHERE repo_key = ... AND attempt_id <> space AND updated_at < cutoff` | `repo_key`-prefixed | O(in-flight attempts for repo), single statement, no loop |

Grep of the full class source for `COMMIT WORK`, `LOOP AT`, plain `SELECT \*`
(non-SINGLE), and `FOR ALL ENTRIES` returns zero real occurrences — the only
`COMMIT WORK` text matches are ABAP Doc comments. **AC1, AC2, AC3 confirmed
structurally against actual source, not just design prose.**

## AC4 / AC5 status

- AC5 (no auto-backfill): confirmed both structurally (every reader compares
  against explicit `cs_hist_level`/`cs_snap_state` constants that space never
  equals) and by test — `get_state_legacy_row` seeds a space-valued legacy row
  and asserts `is_graph_have_eligible`/`is_full_have_eligible` both return
  `abap_false`. Directly verified, not assumed.
- AC4 (one-time cosmetic migration `UPDATE`): the design marks this
  **optional**; it is not present anywhere in the committed source (grep found
  no `hist_level = 'U'`/`snap_state = 'N'` bulk update outside the class
  itself). Since it is optional and every reader already treats space
  identically to the explicit constant (proven above), its absence is not a
  functional or performance gap. Noted for completeness only — not a finding.

## Other checks

- Zero `HTTP`/RFC/network calls anywhere in the class — matches design (0).
- Zero `COMMIT WORK` — transaction ownership stays with the (not-yet-built)
  orchestrator, as designed.
- Peak memory per call: one `zaog_commit_hist` row and/or one `zaog_repo_state`
  row (well under 200 bytes each) — no XSTRING/payload ever touched, no
  reference to `zaog_obj_store` anywhere in the class.
- No secondary-key/`sy-tabix` internal-table patterns exist — the class holds
  no internal tables at all (single-row work areas only).
- Diagnostics: not yet wired to an aggregated observability path, but Slice 1
  has no bulk/hot loop to aggregate over (each call is already O(1)/O(B)) — not
  a finding for this slice.
- Test suite (`ltcl_mat_state`) is a small, focused ABAP Unit suite (`RISK
  LEVEL HARMLESS`) using `CL_OSQL_TEST_ENVIRONMENT`; no test performs a
  repository-scale loop, which is correct since this slice has no
  object-count-scaling behavior to load-test (matches the DESIGN_GATE's
  documented deferral of 40k-scale scenarios to Slices 3/4/7).

## Mandatory scale scenarios

- Small (1-20 rows): exercised directly by the unit test suite.
- Medium (5,000+ mixed objects), large (40,000+), shared-branch (95-98%),
  incremental-store (~100 of ~1,000,000 keys), interrupted-attempt/retry:
  **not applicable to this slice** — every method here is a single-row or
  `repo_key`-bounded operation structurally independent of `zaog_obj_store`
  row count; there is no code path in this class whose cost scales with `N`
  or with total repository object count. This is consistent with, not a
  deviation from, the DESIGN_GATE's explicit statement that Slice 1 has "no
  bulk object-processing hot path of its own." The interrupted-attempt/retry
  scenario IS exercised at the unit level (`clean_attempts_clears_stale`,
  `begin_attempt_no_downgrade`, `begin_attempt_keeps_complete`).
- No live SAT/ST05 trace was taken (object not yet imported to a connected
  system) — this is expected/acceptable at this slice since there is no
  scale-dependent behavior to measure; real-system trace verification is
  required starting at whichever slice first reads/writes `zaog_obj_store` at
  volume (Slice 3/4/7 per state.md routing).

## Blocking fixes

None.

## Report path

This file: `.memory/reviews/performance_design_variant-b-partial-clone_slice1.md`
(appended, `DESIGN_GATE` section preserved above unchanged).

## Next handoff

Regression validation for Slice 1, then Slice 2 design (fetch modes) may
proceed. DR-001 (stale `FETCH_COMMIT` readers) and DR-002 (writer relocation)
remain binding preconditions for Slice 2/3, unaffected by this audit.
