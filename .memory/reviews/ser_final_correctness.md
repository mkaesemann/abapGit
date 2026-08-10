# SER-FINAL — correctness gate (2026-08-10)

No productive ABAP source was changed this pass (WAPA=D/no-change,
FUGR=F/no-change-this-pass, DDLS=DEFER). Correctness gate is therefore
evaluated against the **decision to make no change**, not a diff:

- Standard abapGit behavior (feature OFF): unaffected — no code touched.
- Adaptive batch path (feature ON): unaffected — no code touched.
- SLICE5-001 fix (previously local-only, not yet IT8-validated per
  `.memory/state.md`): this pass found concrete, positive, true-worker
  SAT evidence (`FUGR Set - Batch - Worker` traces) that the FUGR ENLFDIR/
  func-metadata provider is consulted and mostly HIT inside the RFC
  worker's own aRFC session — this is exactly the SAT retest the SER-
  SLICE-5 `OWNER_ACTION_REQUIRED` asked for, and it is satisfied.

`CORRECTNESS_GATE=APPROVE` (nothing to regress; one prior open item
closed by evidence).

## SER-FINAL-CONTINUOUS update (2026-08-10)

One productive change was implemented this pass:
`src/objects/zcl_abapgit_object_fugr.clas.abap`,
`ZIF_ABAPGIT_OBJECT~CHANGED_BY` - guard the `functions()` call with
`IF iv_extra IS NOT INITIAL`. Correctness proof: the guarded branch's
only effect (`funcname = to_upper( iv_extra )` match) can never fire when
`iv_extra` is initial, so removing it cannot change `lv_program`/
`lv_found`/`lt_stamps` for that case; the non-initial-`iv_extra` case is
left byte-for-byte unchanged. See `.memory/reviews/
fugr_changed_by_adversarial.md` for the full attack table (0 BLOCKER/0
MAJOR). WAPA and the remaining FUGR serializer/provider surface were
re-reviewed more thoroughly this pass and confirmed to have no further
safe change (see the respective design logs) - `CORRECTNESS_GATE=APPROVE`
unchanged, now covering one real productive diff plus two re-confirmed
no-change decisions.

## SER-FINAL-CORRECTION update (2026-08-10, this pass) — pure correctness proofs only

This section is intentionally correctness-only; see
`.memory/reviews/fugr_changed_by_adversarial.md` for the separate
adversarial attack pass on the same diffs (per owner instruction, the two
are not folded together).

1. **`needs_function_lookup`/`most_recent_user` extraction** —
   line-by-line equivalence proof:
   - Before: `TYPES: BEGIN OF ty_stamps ... END OF ty_stamps.` (method-
     local) / After: `ty_changed_by_stamp`/`ty_changed_by_stamp_tt`
     (class-level). Field list, types, and table kind (`STANDARD TABLE
     ... WITH DEFAULT KEY`) are identical - only the declaration's scope
     changed.
   - Before (inline): `SORT lt_stamps BY date DESCENDING time
     DESCENDING. READ TABLE lt_stamps INDEX 1 ASSIGNING <ls_stamp>. IF
     sy-subrc = 0. rv_user = <ls_stamp>-user. ELSE. rv_user = c_user_
     unknown. ENDIF.` / After: `most_recent_user( it_stamps )` performs
     `lt_stamps = it_stamps.` (value copy, no aliasing risk) then the
     **identical** four statements against the local copy. Same inputs
     produce the same outputs for every case (empty table, one row, many
     rows, ties).
   - Before (inline): `lt_functions = functions( ).` unconditionally,
     using local booleans / After: `IF needs_function_lookup( iv_extra )
     = abap_true.` where `needs_function_lookup` returns `boolc( iv_extra
     IS NOT INITIAL )` - identical truth table to the already-reviewed
     `IF iv_extra IS NOT INITIAL.` guard (same boolean expression, just
     named).
   - Net effect: `CHANGED_BY`'s observable behavior for every `iv_extra`/
     `lt_stamps` combination is unchanged by this refactor. `PASS`.

2. **`functions()` `BINARY SEARCH` addition** — correctness proof: ABAP's
   language reference guarantees that `READ TABLE ... WITH KEY <k> =
   <v> BINARY SEARCH` returns the identical found/not-found result (and,
   when found, references the identical row when the key is unique -
   `ENLFDIR-FUNCNAME` is unique per area, matching the code's own use as
   a lookup key) as a linear scan, **provided the table is sorted
   ascending by exactly the fields used in the key** at the time of the
   read. `lt_enlfdir` is unconditionally `SORT`ed by `funcname ASCENDING`
   on the statement immediately preceding the loop, for both the
   provider-HIT and direct-SELECT code paths (they converge into the same
   variable before that shared `SORT`). Precondition satisfied in 100% of
   executions ⇒ output is provably identical. `PASS`.

`CORRECTNESS_GATE=APPROVE` (updated, both new changes proven behavior-
preserving).

## SER-FINAL apply-IT8-results update (2026-08-10) — WAPA raw prefetch correctness proof

This section is intentionally correctness-only (equivalence proofs); see
`.memory/reviews/ser_final_wapa_adversarial.md` for the separate
adversarial attack pass on the same diff.

1. **Decode equivalence**: `assemble_and_decode`'s `IMPORT ... FROM DATA
   BUFFER <assembled>` calls use the **exact same field-name lists**
   (`content`/`xml_source` for PAGE, `evhandler` for EVHNDL,
   `typesource` for TYPES) and the **exact same additions**
   (`ACCEPTING PADDING IGNORING CONVERSION ERRORS`) as the reference
   `IMPORT ... FROM DATABASE o2pagcon(tr) ID ...` calls they replace -
   this is not an independent reimplementation of the decode logic, it
   is the identical ABAP `IMPORT` statement fed a different, but
   IT8-proven-identical, source buffer (169/169 exact matches, Experiment
   3). `PASS`.
2. **PAGE missing-content error parity**: the reference path raises
   `zcx_abapgit_exception` with the exact message `"WAPA page {name}/
   {pagekey} has no active content"` when `IMPORT` finds nothing.
   `assemble_and_decode` raises its own (differently worded) exception
   for the same underlying condition (`no content rows`), but this
   exception is only ever caught internally by `try_raw_prefetch` and
   converted into a **fallback to the reference path**, which then
   independently re-evaluates the same condition and raises the exact,
   original, unchanged error message and text. The user-visible error is
   therefore byte-for-byte identical to today's; only the *internal*
   signal used to decide "should I fall back" has a different message,
   which is never shown to a caller. `PASS`.
3. **EVHNDL/TYPES optional-missing parity**: the reference path tolerates
   a missing EVHNDL/TYPES cluster key silently (`IMPORT` sets `sy-subrc
   <> 0`, no check, target keeps its initial value). `assemble_and_decode`
   reproduces this exactly: a requested-but-rowless EVHNDL/TYPES key gets
   an **empty** map entry inserted (not an exception), so
   `add_full_page_details`'s consumption (`lt_ev_handler_sources`/
   `cs_page-types` left empty) is identical either way. `PASS`.
4. **Per-key substitution is the only change**: a line-by-line diff of
   `add_page_content_file`/`add_full_page_details` shows every statement
   *after* obtaining `lt_content`/`lv_xml_source`/`lt_ev_handler_sources`/
   `cs_page-types` is untouched, character-for-character, from the prior
   version - language conversion, `get_page_content`, `io_files->add_raw`,
   `CLEAR`/`FREE` cleanup, and the `LOOP AT is_context-event_handlers`
   block are all identical. `PASS`.
5. **Active-version-only guarantee**: `read_raw_rows`'s `WHERE version =
   @c_active` uses the class's own `c_active` constant (`'A'`, the exact
   same constant already used throughout the reference path's own
   `o2pconkey-version`/`WHERE version = c_active` clauses) - never a
   variable that could be influenced by row data or caller input. `PASS`.

`CORRECTNESS_GATE=APPROVE` (WAPA raw prefetch: proven behaviour-
preserving by construction; FUGR sections above unchanged).
