# Performance implementation audit — variant-b-partial-clone / Slice 2 (sub-slices 2A+2B only)

Mode: `IMPLEMENTATION_AUDIT`
Date: 2026-07-21
Scope: `zcl_abapgit_ortec_fetch_req` (new class) and `zcx_abapgit_ortec_git`
(extension) ONLY. Sub-slice 2C (call-site migration into
`zcl_abapgit_ortec_fastpath`/`zcl_abapgit_ortec_fetch_neg`) is explicitly out
of scope — no productive call site invokes this class yet, confirmed by the
class's own doc comment. AC5/AC6 (fetch_neg unit tests) are out of scope —
`zcl_abapgit_ortec_fetch_neg.clas.abap` is untouched by this sub-slice.

## Verdict: `PASS`

## Evidence: current committed source, line-by-line, not the static scan

Both files were read in full (not sampled) and independently grepped for
`SELECT |INSERT |UPDATE |MODIFY |DELETE |COMMIT WORK|http_client|cl_http|
deepen|shallow`.

### AC1 — zero SQL, zero HTTP in `build_request`/`parse_capabilities`

**PASS.**

- Grep of the entire
  [zcl_abapgit_ortec_fetch_req.clas.abap](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap)
  file for the SQL/HTTP pattern set returns exactly 3 hits, all inside
  `"!` doc comments, zero in executable code:
  - line 12: doc prose "progressive-deepen recovery"
  - line 14: doc prose "`deepen` or `shallow` token"
  - line 82: doc prose referencing `ZCL_ABAPGIT_HTTP_CLIENT=>GET_CDATA` as
    the *origin* of the `iv_ref_data` string parameter (an already-necessary
    upstream HTTP read done by the caller before calling
    `parse_capabilities` — not a call `parse_capabilities` itself makes).
- Full-body inspection of `build_request`
  ([zcl_abapgit_ortec_fetch_req.clas.abap:110-227](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap#L110-L227))
  confirms every statement is one of: `validate_single_want`/
  `build_want_lines`/`build_have_lines` (private, in-memory string builders,
  see below), `zcx_abapgit_ortec_git=>raise`/`raise_unsupported_capability`
  (in-memory exception construction), string template/concatenation
  (`&&`, `|...|`), and `xsdbool`/`CS` boolean tests against the
  already-passed-in `iv_server_caps` string. No DB-bearing type, no
  `zcl_abapgit_http_client` reference, no repo-key parameter anywhere in the
  signature.
- Full-body inspection of `parse_capabilities`
  ([zcl_abapgit_ortec_fetch_req.clas.abap:230-253](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap#L230-L253))
  confirms it is `FIND FIRST OCCURRENCE`/offset arithmetic/substring
  extraction over the already-supplied `iv_ref_data` string, wrapped in
  `TRY...CATCH cx_sy_range_out_of_bounds` — pure string parsing, no I/O.
- `zcx_abapgit_ortec_git.clas.abap` grep for the same pattern returns one
  hit: line 172, `DELETE TABLE mt_callstack FROM <ls_callstack>.` — this is
  an **internal-table** `DELETE` (trimming an in-memory `abap_callstack`
  itab inside `save_callstack`), not a database `DELETE`. No SQL, no HTTP,
  anywhere in this file. The only external call is `CALL FUNCTION
  'SYSTEM_CALLSTACK'` (line ~163), an in-memory kernel call fired only on
  the exceptional `raise`/`raise_unsupported_capability` path, never on
  `build_request`'s success path, and independent of repository size.

### AC2 — `INITIAL_BRANCH_BLOBLESS` has no have/shallow/deepen-capable branch

**PASS.**

[zcl_abapgit_ortec_fetch_req.clas.abap:116-133](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap#L116-L133)
(`WHEN cs_fetch_mode-initial_branch_blobless`) contains exactly these
buffer-mutating statements, in order:
1. `build_want_lines(...)` (emits `want <sha1> <capa>`)
2. `pkt_string( |filter blob:none...| )` (emits `filter blob:none`)
3. literal `'0000'` (flush-pkt)
4. literal `'0009done' && newline` (emits `done`)

`rs_request-have_count` is set to the **literal `0`**, not derived from
`it_certified_haves` or any loop — `build_have_lines` is never called in
this branch, and no `have `/`shallow `/`deepen` string token appears
anywhere in the branch. This is structural (a future maintenance edit to
this specific `WHEN` block would have to actively add a new call to
introduce one of those tokens — it cannot happen by touching a different
mode's branch), matching the AC2 intent exactly.

### AC3 — `MATERIALIZE_BLOBS` raises rather than truncates, both bounds

**PASS.**

[zcl_abapgit_ortec_fetch_req.clas.abap:161-168](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap#L161-L168):

```abap
IF it_want_hashes IS INITIAL.
  zcx_abapgit_ortec_git=>raise( |MATERIALIZE_BLOBS requires at least one blob SHA1| ).
ENDIF.
IF lines( it_want_hashes ) > c_materialize_batch_max.
  zcx_abapgit_ortec_git=>raise(
    |MATERIALIZE_BLOBS batch size { lines( it_want_hashes ) } exceeds maximum { c_materialize_batch_max }| ).
ENDIF.
```

Both the empty case and the over-`c_materialize_batch_max` (=100) case
`RAISE`, unconditionally, before any want-line is built — no code path in
this branch truncates `it_want_hashes` (e.g. no `DELETE ... FROM 101`, no
`LOOP ... UNTIL sy-tabix = 100`) or otherwise silently emits a partial/
oversized want list. `c_materialize_batch_max` is confirmed declared as a
public hard constant (`VALUE 100`,
[zcl_abapgit_ortec_fetch_req.clas.abap:29](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap#L29)),
enforced inside `build_request` itself as required.

### AC4 — no mode's code path emits a `deepen` token

**PASS.**

Grep of the full file for `deepen` returns only the 2 doc-comment hits
already listed under AC1 (lines 12, 14) — zero occurrences inside
`build_request`'s `CASE` body or any of the three private helpers
(`validate_single_want`, `build_want_lines`, `build_have_lines`). Manual
read of all five `WHEN` branches
([zcl_abapgit_ortec_fetch_req.clas.abap:114-224](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap#L114-L224))
confirms no constant, no loop, and no conditional emits a `deepen` line —
the only pkt-line verbs ever assembled are `want`, `have`, `filter`, `done`,
and the flush-pkt `0000`.

## Cost-model confirmation (structural, not measured — this class touches zero DB tables)

- `build_request` cost is: one `CASE` dispatch (O(1)) → at most two bounded
  linear loops:
  - `build_want_lines`
    ([zcl_abapgit_ortec_fetch_req.clas.abap:257-269](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap#L257-L269)):
    single `LOOP AT it_want_hashes` — count is exactly 1 for four of five
    modes (enforced by `validate_single_want`) or `1..100` for
    `MATERIALIZE_BLOBS` (enforced by the AC3 raise).
  - `build_have_lines`
    ([zcl_abapgit_ortec_fetch_req.clas.abap:272-280](src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap#L272-L280)):
    single `LOOP AT it_certified_haves` — count is `0..200` per the
    pre-existing, unchanged `get_have_commits` cap (this file does not
    itself enforce the 200 cap, consistent with the approved design, which
    places that cap upstream).
  - No nested loops, no recursion, no lookup-table construction.
- Zero references anywhere in either file to `ZAOG_OBJ_STORE`,
  `ZAOG_COMMIT_HIST`, `ZAOG_REPO_STATE`, or any other DB table — confirmed
  by the AC1 grep (no `SELECT`/`INSERT`/`MODIFY`/`UPDATE`/`DELETE FROM`
  hits) and by full-file read (no `TABLES`/`DATA ... TYPE <db-table>`
  declarations).
- Net result: `build_request`'s cost is `O(want-count + have-count)`,
  bounded at `O(1 + 200)` or `O(100 + 0)` depending on mode, and is
  **structurally identical at N = 1, 1,000, 40,000, and 1,000,000 stored
  repository objects** — the class has no code path through which N could
  enter the cost function. This matches §7 of the approved design exactly
  and introduces no regression relative to it.
- `zcx_abapgit_ortec_git`'s additions (`mv_unsupported_capability`,
  `mv_missing_capability` attributes; extended `constructor`;
  `raise_unsupported_capability` class-method) are all O(1) — plain field
  assignment plus one `RAISE EXCEPTION` — and fire only on the
  already-exceptional error path, never inside a loop over any
  repository-scale collection.

## No new N-dependent cost introduced

Confirmed by exhaustive grep + full read of both files: no SQL, no HTTP
client instantiation/call, no table type referencing a Z* DB table, no
recursive call, no loop over any input whose count is unbounded by design
(`it_want_hashes` bounded to 1 or ≤100 by this file's own validation;
`it_certified_haves` bounded to ≤200 by the pre-existing, out-of-scope
`get_have_commits`). The one known N-dependent cost in the surrounding
pipeline (`collect_ancestor_haves`'s unbounded `zaog_obj_store` commit read)
lives entirely in `zcl_abapgit_ortec_fetch_neg.clas.abap`, which is **not**
part of this sub-slice's file list and is untouched by this change —
correctly out of scope per the design's own DESIGN_GATE gate closure
(tracked as a Slice 3 candidate, not a Slice 2 blocker).

## Mandatory scale scenarios

- **Small (1–20 objects):** N/A at the class level — this class never reads
  repository object counts; behavior is identical regardless of repo size.
  Not separately measurable; structural argument above applies uniformly.
- **Medium/Large (5,000 / 40,000+ objects):** same — structurally
  N-independent, no measurement possible or necessary since no code path
  touches N. Marked **not executed as a runtime measurement** (none is
  possible for a pure function of bounded-size inputs); relying on
  structural/static evidence only, consistent with the approved design's
  own framing ("cost is a function of want-count and have-count, never of
  N").
- **Shared branches / incremental store / interrupted-attempt scenarios:**
  not applicable to this sub-slice — no branch, commit-history, or
  persistence interaction exists in either file.
- Sub-slice 2C's call-site migration (where these primitives are actually
  wired into a live, N-adjacent fetch/materialization flow) is the point at
  which a real medium/large exercise becomes meaningful and is deferred to
  that sub-slice's own audit.

## Findings

None. No blocking, major, or minor performance findings against AC1–AC4 or
the mandatory performance model for the files in scope.

## Summary

| AC | Result |
|----|--------|
| AC1 (zero SQL/HTTP) | PASS |
| AC2 (`INITIAL_BRANCH_BLOBLESS` no have/shallow/deepen) | PASS |
| AC3 (`MATERIALIZE_BLOBS` raises, not truncates, both bounds) | PASS |
| AC4 (no `deepen` token emitted by any mode) | PASS |
| Cost model (`O(want+have)`, N-independent at 1/1K/40K/1M) | Confirmed |
| New N-dependent cost introduced | None found |

## Report path

This file:
`.memory/logs/performance_audit_variant-b-partial-clone_slice2.md`

## Next handoff

Sub-slices 2A+2B pass the performance implementation audit and may proceed
to correctness/protocol regression validation for these two files in
isolation. Sub-slice 2C (call-site migration) requires its own senior
implementation, its own low-cost performance scan, and its own
`IMPLEMENTATION_AUDIT` pass before it can be considered complete — this
audit does not cover it and does not authorize skipping that step. AC5/AC6
(fetch_neg unit tests) remain open against `zcl_abapgit_ortec_fetch_neg`,
untouched by this sub-slice.
