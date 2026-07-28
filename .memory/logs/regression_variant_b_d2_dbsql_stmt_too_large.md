# Regression validation: Variant B / Package D2 — DBSQL_STMNT_TOO_LARGE fix

## Summary

- Task: read-only regression validation for the get_objects chunking fix
  (SQLSIZE incident, `zcl_abapgit_ortec_obj_store`).
- Scope: bulk object-store reads (`get_objects`), and every caller that
  passes `iv_bulk_fetch = abap_true`.
- Validation mode: source inspection, workspace diagnostics, and a focused
  independent implementation audit (delegated,
  `.memory/logs/performance_audit_variant_b_d2_dbsql_stmt_too_large.md`,
  verdict PASS, 0 blocking). No live SAP ABAP Unit or activation run was
  available in this environment.
- Result: PASS_WITH_FINDINGS (informational-only finding, non-blocking; see
  audit artifact).

## Files reviewed

- [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.testclasses.abap)

## Evidence collected

- `get_errors` reports 0 errors for both changed files.
- `git diff --stat` confirms the diff is scoped to exactly these two files
  (plus the three new `.memory` artifacts); no other productive source file
  is touched.
- A workspace-wide grep of `iv_bulk_fetch` across `src/ortec/git/*.abap`
  confirms 5 existing production callers
  (`zcl_abapgit_ortec_obj_store=>get_reachable_objects` ×3 call sites,
  `zcl_abapgit_ortec_delta` ×1, `zcl_abapgit_ortec_obj_index` ×2,
  `zcl_abapgit_ortec_walk_prep` ×1) - none require any change, since the
  fix is entirely internal to `get_objects`' own chunking logic and its
  external signature/contract is unchanged.
- A workspace-wide grep of `iv_bulk_fetch` across `src/ortec/git/*.testclasses.abap`
  confirms only this incident's own 6 new tests reference the parameter
  explicitly - no pre-existing test anywhere in the project asserted the
  old unchunked-single-call behavior, so no existing test can regress from
  this change.
- The SYSTEM_NO_ROLL fix (`2111b288`) and TIME_OUT fix (`17513ba7`) were
  confirmed untouched: `git diff --stat` for this working-tree change does
  not list `zcl_abapgit_ortec_missing_obj.clas.abap`,
  `zcl_abapgit_ortec_cold_init.clas.abap`, or their testclasses; the
  unrelated pre-existing line-ending-only modification in
  `zcl_abapgit_ortec_missing_obj.clas.testclasses.abap` (confirmed via an
  empty `git diff`/`git diff --stat` for that file) was left untouched and
  is excluded from this incident's commit.

## Scenario matrix

| Scenario | Status | Evidence |
| --- | --- | --- |
| `get_objects(iv_bulk_fetch=abap_true)` chunks at `c_select_package_size` for K > 1000 | PASS | `bulk_fetch_uses_pkg_size` (1500 entries, 2 SQL packages asserted); confirmed by focused audit item 9 |
| `get_objects(iv_bulk_fetch=abap_true)` still issues exactly 1 SQL call for K <= 1000 (no regression for the common case) | PASS | `bulk_fetch_no_per_key_sql` (250 entries, 1 call asserted); confirmed by focused audit item 9 |
| Duplicate input SHA1s deduplicated | PASS | `bulk_fetch_dedups_input` |
| Empty input executes zero SQL | PASS | `bulk_fetch_empty_no_sql` |
| `status = 'R'` predicate preserved (staged 'D' rows stay invisible) | PASS | `bulk_fetch_preserves_where` |
| Small K request unaffected by large N under an unrelated repo_key | PASS | `bulk_fetch_large_n_small_k` |
| Complete result/missing set preserved across package boundaries | PASS | `bulk_fetch_uses_pkg_size` returns all 1500 objects, not just the last chunk; confirmed by focused audit item 4 |
| No per-object SQL introduced | PASS | `bulk_fetch_no_per_key_sql`; confirmed by focused audit item 5 |
| `get_reachable_objects` (SYSTEM_NO_ROLL fix site) unaffected | PASS | grep-confirmed no `populate_cache` call remains; `get_objects` call sites unchanged; confirmed by focused audit item 8 |
| `materialize_missing_batches`/`ensure_available` (TIME_OUT fix) unaffected | PASS | zero matches for either symbol in the changed file; confirmed by focused audit item 8 |
| Focused independent performance/correctness audit | PASS | [.memory/logs/performance_audit_variant_b_d2_dbsql_stmt_too_large.md](.memory/logs/performance_audit_variant_b_d2_dbsql_stmt_too_large.md), 0 blocking, 1 non-blocking informational note |
| Live SAP activation / ABAP Unit / ATC execution | NOT RUN | No connected SAP system write/execution was used for this local checkpoint (read-only IT8 queries were used only for incident diagnosis, per the run brief's restriction) |

## Failure analysis

- Failing class/method: none.
- No correctness defect was identified from the static review of this fix.

## Corrective proposal

- None required for this validation pass.
- Owner must import this checkpoint into IT8 and execute the exact retest
  sequence in the incident artifact §13 before Package D2 can be considered
  closed on this specific dump class.

## Final local regression confirmation

- Regression verdict: PASS_WITH_FINDINGS.
- Blocking findings: 0.
- Performance audit verdict: PASS (0 blocking, 1 non-blocking informational note).
- SAP validation: NOT RUN in this environment.
