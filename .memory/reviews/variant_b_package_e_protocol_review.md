# Variant B Package E — Protocol/persistence review (corrective pass)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E0-PROTOCOL-PERSISTENCE-REVIEW-CORRECTIVE
REVIEWED_ARTIFACT=variant_b_package_e_design.md (corrective, §0-§12)
SUPERSEDES=the prior protocol/persistence review of the E0-DESIGN draft
  (APPROVE, scoped to the old single-tier E2-D slice only), retrievable via
  `git log -p -- .memory/reviews/variant_b_package_e_protocol_review.md`.
BASELINE=8eef0b55fb37892c3d6b6428c038886481c73192
```

## Applicability

Rerun in full against the corrective design's 9-slice model, since the
protocol/persistence surface materially changed shape (a single diagnostic
table proposal replaced by a 4-tier evidence ladder, plus a new scenario
matrix in E4-VERIFY and a new shared-constant change in E-HARDEN):

```text
E1-TEST  = test-only, no protocol/persistence surface touched
E1-PERF  = touches persistence ONLY via an existing bulk MODIFY's chunk-size
           constant — no new table, no new key, no new transaction boundary
E2-DIAG  = D0 = no code, no persistence; D1 = read-only, NO persistence at
           all (stronger than the prior draft's single write-only tier);
           D2 = gated, would reuse SAP-standard BAL/SLG1 persistence, not a
           new custom table; D3 = gated, last-resort, would require its own
           DDIC review if ever reached
E2-FIX   = NOT_AUTHORIZED placeholder, no persistence surface exists yet
E3-TEST  = test-only, no protocol/persistence surface touched
E4-VERIFY = test-only; scenario matrix documents existing repair mechanisms'
           persistence/transaction behavior, does not change it
E4-FIX   = NOT_REQUIRED, no persistence surface
E-HARDEN = a shared ABAP constant extraction only — no persistence surface
```

No slice changes the git wire protocol, the `MATERIALIZE_BLOBS` capability
contract, `zcl_abapgit_ortec_mat_state`'s `hist_level`/`snap_state`
semantics, or either existing repair mechanism's control flow. This review
is scoped to E2-DIAG's evidence ladder (the only slice with any persistence
surface at all, and only conditionally) and to E4-VERIFY's characterization
of the existing repair transactions' commit/retry boundaries.

## E2-DIAG persistence review (redesigned this pass)

- **D0 (no code)**: no persistence surface. No finding.
- **D1 (read-only comparison tool)**: this is a **strict improvement** over
  the prior draft's design from a protocol/persistence-risk standpoint — D1
  performs zero writes of any kind (not even a write-only diagnostic
  insert). It only issues reads against `zaog_obj_index` (existing row) and
  `zaog_obj_store` (existing tree objects, bounded to the path's depth).
  There is no new persistence object, no new key, no transaction boundary
  change, and — critically — no risk of the diagnostic mechanism itself
  ever becoming a second source of truth, since there is nothing to persist
  that a future consumer could mistakenly read. This satisfies INV-E2-D-3
  more strongly than the prior draft's write-only-log design did.
- **D2 (BAL/SLG1-gated)**: reusing SAP's standard Application Log function
  group instead of a bespoke table is the correct protocol/persistence
  choice if this tier is ever reached — it inherits `SBAL_DELETE`'s
  existing retention/purge semantics (resolving OQ-2 "for free" without a
  new custom job), and BAL log writes are well-understood by SAP Basis as
  an operational log, not a business-data table, which correctly signals
  its diagnostic-only nature to anyone auditing the system's persistence
  layer later. The design correctly keeps this gated behind D0/D1 proving
  insufficient, and correctly requires the flag be `repo_key`-scoped
  (§3 D2, resolving the prior review's MINOR finding directly in the
  design text this time, not left for a future fix).
- **D3 (new DDIC sink, last resort)**: correctly NOT authorized by default;
  the design defers its exact shape to a future review if D2 proves
  insufficient. No premature DDIC commitment is made — appropriate caution
  given no live reproduction has occurred yet.
- **Transaction boundary (if D2/D3 are ever implemented)**: the design's
  requirement (§3, carried over in spirit from the prior draft) that any
  diagnostic write must not introduce a new `COMMIT WORK` inside
  `build_files_from_rows`'s caller-owned LUW remains the correct constraint
  and is unchanged by this redesign — still necessary because that method
  runs inside the Stage-build caller's own transaction, per Package D2's
  closeout (D1 owns delta-base resolution, D2 owns attempt/transaction
  isolation); an uncoordinated commit would violate that model.
- **OF-1's gated single-row repair (§3)**: reviewed for persistence impact
  even though it is NOT_AUTHORIZED this pass — its stated design (DELETE +
  re-derive exactly one `(repo_key, commit_sha1, obj_name)` row) is
  correctly scoped as a single-row, primary-key-targeted operation, not a
  broader re-walk; this is the right shape to specify now so that, once
  authorized, its persistence footprint is already bounded by design
  rather than invented under time pressure during a live incident.

## E4-VERIFY persistence/transaction review (new this pass)

- The 9-scenario matrix in §6 correctly characterizes, without altering,
  the two existing repair mechanisms' transaction/retry boundaries: the
  filtered path's `ensure_available` bounded top-up (object-level, no
  repo-wide invalidation) and the unfiltered path's `invalidate_all_history`
  + one retry (repo-wide, with its own `COMMIT WORK`, per discovery §E4).
  E4-VERIFY's new regression tests (E4-D-01..04) pin these EXISTING
  boundaries; they do not add a new commit point or a new persistence key.
- **Dispatch-exclusivity claim (INV-E4-D-3, new)**: this invariant is
  protocol-relevant because it pins that the ORTEC-active and non-active
  `pull_by_branch` paths never both execute for the same call — a
  correctness property with direct persistence implications (if both paths
  were ever reachable for the same repository, `invalidate_all_history`
  could plausibly be invoked twice in overlapping transactions, a real
  double-invalidation risk). Confirming this stays mutually exclusive via a
  dedicated regression test (E4-D-04) is the correct way to protect this
  boundary going forward, rather than relying solely on today's code
  structure remaining unchanged by chance.
- Scenario 7's out-of-band residual risk (§6) correctly does NOT propose a
  new persistence safeguard for an out-of-band precondition — consistent
  with the correctness review's assessment that inventing one here would be
  premature relative to any evidenced likelihood.

## E1-PERF persistence/transaction review (new this pass)

- Raising the MODIFY chunk-size constant changes only the SIZE of each bulk
  array-DML statement's internal table, not its target table, its primary
  key, or its surrounding transaction. The repo lock is already held for
  the whole `rebuild_index` call regardless of chunk size (discovery §E1,
  re-confirmed); fewer, larger chunks can only shorten total lock hold time
  under the evidenced round-trip-bound cost model, not lengthen it. No new
  commit point, no new isolation concern, no migration. This is protocol/
  persistence-neutral in every respect that matters at this review's level.

## E-HARDEN persistence review (new this pass)

- OF-2's shared-constant extraction (`c_walk_error_prefix` or equivalent)
  is a pure ABAP source-level refactor with no persistence surface — no
  table, no key, no transaction. No finding.
- The explicitly NOT-authorized standard-file architecture question
  (`E-HARDEN-STANDARD-FILE-COUPLING`) is correctly left out of this pass's
  persistence review scope, since no change to that file is proposed here.

## Verdict

```text
PROTOCOL_PERSISTENCE_REVIEW=APPROVE
BLOCKING=0
MINOR=0
NOTE=Applicability remains narrow and, if anything, LOWER-RISK than the
  prior draft: D1 (the only AUTHORIZED_NOW piece of E2-DIAG with any data
  surface) is read-only with zero persistence; D2/D3's eventual DDIC/BAL
  shape remains correctly deferred and gated. E4-VERIFY and E1-PERF add
  regression coverage and a chunk-size constant respectively, neither of
  which touches protocol or persistence semantics. E-HARDEN's constant
  extraction has no persistence surface. Re-review only if D2/D3 are ever
  authorized, or if OF-2's excluded standard-file question is later
  reopened by the owner.
```
