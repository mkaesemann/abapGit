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
