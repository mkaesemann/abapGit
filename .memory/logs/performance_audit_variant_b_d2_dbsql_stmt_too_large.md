# Implementation Audit — SQLSIZE-6 (get_objects chunking fix, D2 DBSQL_STMNT_TOO_LARGE)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SQLSIZE-6-FOCUSED-AUDIT
MODE=IMPLEMENTATION_AUDIT (combined correctness + performance)
BASELINE=17513ba741322d11078b8c6ff5fe41b1c6978d4c + uncommitted working-tree diff
STATUS=PASS
```

## Scope

- src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap — `get_objects`,
  `read_object_rows`, plus `verify_tree_closure`/`get_tip_blob_sha1s` doc
  comments (context only).
- src/ortec/git/zcl_abapgit_ortec_obj_store.clas.testclasses.abap — the 6
  new tests: `bulk_fetch_dedups_input`, `bulk_fetch_uses_pkg_size`,
  `bulk_fetch_empty_no_sql`, `bulk_fetch_preserves_where`,
  `bulk_fetch_large_n_small_k`, `bulk_fetch_no_per_key_sql`.
- Evidence type: current-source call-chain analysis (read-only), grep-level
  caller check. No live SAP execution/trace was run in this pass.

## Required-check results

1. **Chunking unified regardless of `iv_bulk_fetch`** — CONFIRMED.
   `get_objects` now has a single, unconditional loop
   (`IF lines( lt_package ) >= c_select_package_size` → `read_object_rows` →
   `CLEAR lt_package`, plus a trailing flush) with no `IF iv_bulk_fetch =
   abap_true/abap_false` branch left anywhere in the method body.
   `iv_bulk_fetch` is no longer read inside `get_objects` at all — retained
   purely as a signature-compatible no-op, exactly as the doc comment on the
   method (line ~75) and the inline incident comment (line ~568) state.

2. **WHERE predicate unchanged** — CONFIRMED. `read_object_rows`
   (line 1259) still issues exactly
   `WHERE repo_key = iv_repo_key AND obj_sha1 IN lr_sha1s AND status = 'R'`,
   byte-identical to the incident artifact's pre-fix extract except for the
   inserted `gv_read_object_rows_calls = gv_read_object_rows_calls + 1.`
   counter line, which precedes the range-table build and has no bearing on
   the SQL predicate.

3. **Empty-set guard preserved** — CONFIRMED at two layers: `get_objects`
   returns immediately when `lt_unique_sha1s IS INITIAL` (before any
   `read_object_rows` call), and `read_object_rows` itself still has
   `IF it_sha1s IS INITIAL. RETURN.` ahead of the counter increment, so an
   empty package can never be counted either. Test `bulk_fetch_empty_no_sql`
   exercises this and asserts `gv_read_object_rows_calls = 0`.

4. **Complete result-set assembly across package boundaries** — CONFIRMED.
   `APPEND LINES OF lt_db_rows TO lt_rows` executes both inside the chunking
   loop and in the trailing `IF lt_package IS NOT INITIAL` flush, so `lt_rows`
   accumulates every chunk. The final "raise if any requested SHA1 not
   found" loop iterates `lt_unique_sha1s` against `lt_found_sha1s`, which is
   built from the fully-accumulated `lt_rows` (not a per-chunk table), so it
   correctly operates over the complete result. Test
   `bulk_fetch_uses_pkg_size` empirically proves this: 1500 stored objects,
   `lines( lt_objects ) = 1500` asserted (not 1000 or 500), which would fail
   if only the last chunk survived.

5. **No per-object SQL introduced** — CONFIRMED. Chunking is strictly
   count-gated at `c_select_package_size = 1000`; `read_object_rows` is
   never called inside the per-SHA1 `LOOP AT lt_missing_sha1s`, only after
   the package accumulator reaches the threshold or the loop ends. Test
   `bulk_fetch_no_per_key_sql` (250 entries, one call expected) is a direct
   regression guard against a per-key-SQL regression.

6. **Unrelated large N cannot affect small-K SQL call count** — CONFIRMED
   by construction: the SELECT is `repo_key = iv_repo_key`-scoped and
   chunking is driven purely by the size of the caller-supplied `it_sha1s`
   set, never by existing row counts under any other `repo_key`. Test
   `bulk_fetch_large_n_small_k` (50 objects under an unrelated repo_key,
   1-object request under `mc_repo`) confirms the small-K request still
   returns exactly 1 object; it does not independently assert the SQL call
   count, but no code path in `get_objects`/`read_object_rows` reads or
   iterates any other repo's rows, so this is a structural (not just
   test-observed) guarantee. Minor observation only, not a finding (see
   below).

7. **`gv_read_object_rows_calls` has no productive/persisted side effect**
   — CONFIRMED. Declared `CLASS-DATA ... TYPE i` in a section only reachable
   via `LOCAL FRIENDS` from the test class; workspace-wide grep shows its
   only non-test reference is the single increment statement inside
   `read_object_rows` (line 1267) and its own declaration/doc comment. It is
   never written to the database, never logged, and every test resets it to
   0 in both `setup` and `teardown`. It changes no externally observable
   behavior of `get_objects` for any existing caller.

8. **SYSTEM_NO_ROLL (2111b288) and TIME_OUT (17513ba7) fixes unaffected** —
   CONFIRMED. `get_reachable_objects` (line 718) contains no call to
   `populate_cache` (only reference to that method near this code is in a
   comment explaining its historical removal); it still calls
   `get_objects( iv_repo_key = ... it_sha1s = ... iv_bulk_fetch = abap_true )`
   identically at the commit, per-tree-level, and blob levels. A
   workspace-wide grep for `materialize_missing_batches`/`ensure_available`
   inside this file returns zero matches — this diff does not touch the
   TIME_OUT fix's code at all.

9. **New tests exercise their claims, arithmetic correct** — CONFIRMED.
   - `bulk_fetch_uses_pkg_size`: 1500 entries, `c_select_package_size = 1000`
     → loop accumulates to 1000, fires one `read_object_rows` call and
     clears; remaining 500 accumulate and flush via the trailing
     `IF lt_package IS NOT INITIAL` — exactly 2 calls. Test asserts
     `exp = 2`. Correct.
   - `bulk_fetch_no_per_key_sql`: 250 entries never reach the 1000
     threshold inside the loop, so the trailing flush issues the only call
     — exactly 1 call. Test asserts `exp = 1`. Correct.
   - `bulk_fetch_dedups_input` (3 identical SHA1s → 1 object),
     `bulk_fetch_empty_no_sql` (0 SQL calls for empty input),
     `bulk_fetch_preserves_where` ('D'-status row must stay invisible /
     raise), and `bulk_fetch_large_n_small_k` (unrelated-repo isolation)
     were each read in full and their assertions match their stated intent
     and the source's actual behavior.

10. **Test names ≤ 30 characters** — CONFIRMED for all 6 new methods:
    `bulk_fetch_dedups_input` (23), `bulk_fetch_uses_pkg_size` (24),
    `bulk_fetch_empty_no_sql` (23), `bulk_fetch_preserves_where` (26),
    `bulk_fetch_large_n_small_k` (26), `bulk_fetch_no_per_key_sql` (25).

## Findings

None blocking. No major findings.

- **Minor / informational (non-blocking)**: `bulk_fetch_large_n_small_k`
  does not itself assert `gv_read_object_rows_calls`, so it verifies
  functional isolation (correct row count returned) but not the SQL-call
  count in that specific scenario. Not required to fix — repo_key scoping
  in the WHERE predicate already makes this structurally guaranteed
  independent of any test assertion, and every other new test already
  covers call-count correctness for the chunking logic itself. No action
  required.

## Call-site impact

Grep of `get_objects(`/`iv_bulk_fetch` across `src/ortec/**` confirms
callers in `zcl_abapgit_ortec_cold_init`, `zcl_abapgit_ortec_obj_index`,
`zcl_abapgit_ortec_walk_prep`, `zcl_abapgit_ortec_pack_stream` testclasses,
and `zcl_abapgit_ortec_delta` (doc-comment reference only) are unchanged
call sites — none require modification, matching the performance scan's own
"callers affected, none require a code change" conclusion. Not opened
beyond this grep, per the audit's forbidden-paths scope.

## Evidence limits

- Read-only source/test review only; no live ABAP Unit execution, ATC run,
  or SAT/SQL trace was performed in this audit pass.
- Live re-verification of the fix under the original 40,891-SHA1 cold-branch
  scenario (owner retest) remains outside this audit's scope, consistent
  with the performance scan's own "Unverified paths" section.

## Verdict

```text
PACKET=COMPACT_HANDOFF_V1
task=SQLSIZE-6-FOCUSED-AUDIT
status=PASS
artifact=.memory/logs/performance_audit_variant_b_d2_dbsql_stmt_too_large.md
blocking=0
next=owner_retest_live_sat_recommended
```
