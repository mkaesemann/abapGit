# Variant B Package E — Correctness design review (corrective pass)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E0-CORRECTNESS-REVIEW-CORRECTIVE
REVIEWED_ARTIFACT=variant_b_package_e_design.md (corrective, §0-§12)
SUPERSEDES=the prior single-tier correctness review of the E0-DESIGN draft
  (APPROVE_WITH_MINOR_REVISIONS, 1 MINOR), retrievable via
  `git log -p -- .memory/reviews/variant_b_package_e_correctness_review.md`.
BASELINE=8eef0b55fb37892c3d6b6428c038886481c73192
```

## Method

This is not a restatement of the design's own prose. Each of the 10
corrective findings (CR-01..CR-10) raised against the prior design/review/
bootstrap cycle is checked here against the corrective design's actual
resulting text and, where a correctness claim is load-bearing, against a
fresh independent re-read of the cited source lines — not assumed correct
because the design document asserts it.

## CR-by-CR resolution verification

| CR | Requirement | Verified in corrective design | Independent check performed | Result |
| --- | --- | --- | --- | --- |
| CR-01 | Single active design, superseded draft fully removed, `SUPERSEDES=` note | Header now carries `SUPERSEDES=` citing both prior versions by commit/history pointer; the entire old §1-§7 body and the full superseded historical draft (former "# (superseded, historical)..." section, INV-E-01..13, §3-§8) are gone | `grep -i "superseded|DRAFT_FOR_DISCOVERY"` against the current file returns zero matches outside the header's own `SUPERSEDES=` line; file line count dropped from 658 to 639 while gaining 5 new slices — consistent with deletion of a much larger duplicate block, not just cosmetic renaming | **RESOLVED** |
| CR-02 | Blanket authorization corrected to PARTIAL with per-slice gating | §0 table lists `Authorization this pass` per row; footer states `IMPLEMENTATION_AUTHORIZATION=PARTIAL` explicitly, not a single YES/NO | Cross-checked against §1-§7's own "Checkpoint boundary"/status lines — every slice's stated authorization in §0 matches its own section body (e.g. E2-FIX is `NOT_AUTHORIZED` in both §0 and §4; E1-PERF is `AUTHORIZED_NOW` in §0 and carries `AUTHORIZED_NOW=E1-A only` in §2) | **RESOLVED** |
| CR-03 | E2-DIAG redesigned as a least-invasive evidence ladder (D0-D3) | §3 defines D0 (no code) → D1 (read-only tool, authorized now) → D2 (BAL/SLG1, gated) → D3 (new DDIC, last resort) | Independently confirmed SAP's standard Application Log (`SBAL`/`SLG1`) is a real, system-wide, already-bounded mechanism (general SAP-domain knowledge, not requiring a source read of this fork) — a materially lower-cost D2 tier than the prior draft's bespoke DDIC table; D1's tool is scoped to O(path depth), not O(commit size), correctly matching "least invasive that could resolve the question first" | **RESOLVED** |
| CR-04 | E1 reclassified, 6 ranked/quantified performance candidates | §2 header states `CORRECT_BUT_PERFORMANCE_OPEN`; ranked table has all 6 IDs (E1-A..F) with SQL/rows, bytes/memory, lock/rollback, DDIC, and correctness-risk columns filled for every row | Re-verified the underlying SAT numbers myself against [variant_b_d2_sat_warm_to_cold_o4h8794.md](../incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md): Phase I = 9,390,412 µs of 31,417,756 µs total = 29.9%, matches the design's "~30%" and "~9.39 s" claims; ~730 bytes/row independently recomputed from `zaog_obj_index.tabl.xml`'s field lengths (3+12+40+4+40+40+255+255+40+40+1 = 730) — matches the design's figure exactly, not merely copied | **RESOLVED** |
| CR-05 | Full E4 state/scenario matrix, one of three specific verdicts | §6 has a 9-row scenario × property matrix and issues `E4_NOT_REQUIRED_CURRENTLY_COMPLETE` for 7/9, `OWNED_BY_E2-DIAG` for scenario 8, and an explicit accepted-risk classification (not a fourth invented verdict) for scenario 7 | Verified the dispatch-exclusivity claim underpinning scenario 6 independently is consistent with `zcl_abapgit_git_porcelain.clas.abap`'s described RETURN-based dispatch (per discovery evidence); verified scenario 7's framing does not silently reclassify an unreachable-by-normal-means precondition as either of the two "required" verdicts without justification — the design is explicit that it would use `E4_PARTIAL_GAP_REQUIRES_DESIGN` if scenario 7 were normally reachable, which it is not | **RESOLVED** |
| CR-06 | OF-1 kept active as a candidate, with a detection/repair design | §3's D1 tool and its dedicated "OF-1 as an active E2 candidate" subsection give a concrete, bounded single-row detection design and a gated (not yet authorized) single-row repair design | Confirmed the detection design does not silently re-scope into "not our problem" — it explicitly names the exact repair action it would take (DELETE + re-derive one row) once evidence exists, and INV-E2-D-4 pins "never re-walk the full commit's tree" | **RESOLVED** |
| CR-07 | OF-2 and OF-3 given stronger, decided dispositions | §7 gives OF-3 a closed `ALREADY_ADEQUATELY_MITIGATED` disposition (no further action) and OF-2 a decided, actionable `AUTHORIZED_NOW` fix (shared-constant extraction) with an explicit, named, NOT-unilaterally-resolved architecture question split out separately | Independently checked the OF-2 fix does not overreach into the standard abapGit file — the design's own text explicitly excludes `zcl_abapgit_git_porcelain.clas.abap` from this pass's authorization, correctly respecting the review boundary between "ORTEC-owned code" and "standard abapGit code" | **RESOLVED** |
| CR-08 | False "no IT8 validation needed for test-only" claim corrected | Every slice's "IT8 acceptance" subsection (§1, §5, §6, §7) now requires real IT8 activation/syntax check, ABAP Unit execution, and ATC — no slice claims a blanket exemption | Confirmed the design's own §1 explicitly states "test-only does **not** mean IT8-exempt," directly and unambiguously overturning the corrected claim rather than leaving it ambiguous | **RESOLVED in the design.** The companion bootstrap handoff (`variant-b-package-e-bootstrap.md`) still requires its own separate correction — tracked as a pending action on this corrective pass, not a design-doc defect (see Scope note below) |
| CR-09 | `.memory/state.md` corrections | Not a design-doc concern — tracked as a separate, still-pending corrective-pass action | N/A (out of this artifact's scope) | **OUT OF SCOPE FOR THIS REVIEW** — verified this review does not depend on stale `state.md` content for any of its own conclusions |
| CR-10 | Explicit proof/classification of all 9 original intended outcomes | §9's outcome-preservation matrix maps all 9 rows from the renumbering decision's 7 bullets (split 8/9) with a status, evidence citation, and owning slice per row | Cross-checked each of the 9 rows against [variant_b_package_renumbering.md](../decisions/variant_b_package_renumbering.md)'s own 7 scope bullets — no bullet is unaccounted for; row 9 (Diff/status regression coverage) is correctly flagged as only PARTIALLY covered rather than being rounded up to a false "done," which is the kind of overstatement CR-10 exists to prevent | **RESOLVED** |

## Additional independent correctness findings (this pass)

### Confirmed sound
- E1-PERF's core safety argument (fewer, larger MODIFY batches is behavior-
  preserving) is correct: batch size is purely a chunking constant on an
  already-idempotent bulk `MODIFY ... FROM TABLE` inside the pre-existing
  repo lock; no new SQL shape, no new transaction boundary, no DDIC change.
  This is a low correctness-risk change, matching its `AUTHORIZED_NOW`
  status.
- The E1-D "proven unsafe as a bare key" finding is independently
  re-derivable from first principles once `file_to_object`'s dependency on
  caller-supplied `io_dot`/`iv_devclass` is accepted as fact (per discovery
  evidence) — a shared raw tree SHA1 truly does not imply an identical
  `.abapgit`-driven object-type/name resolution across commits/branches.
  The design correctly declines to authorize E1-D/E1-E without a richer key.
- D1's read-only tool design in §3 introduces no write path and no new
  correctness-decision consumer of its own output (INV-E2-D-3), so it
  carries effectively zero regression risk to production behavior even
  though it directly re-implements part of the tree-walk/decode logic for
  verification purposes — the "independently recompute and compare" pattern
  is the correct way to build a trustworthy detector (it does not simply
  re-read the same row it's trying to validate).
- E4-VERIFY's scenario 7 (out-of-band deletion) is correctly NOT converted
  into new production code — inventing a defensive mechanism against a
  precondition that requires bypassing the application's own persistence
  APIs would be over-engineering relative to any evidenced risk, and the
  design correctly defers to an owner risk-acceptance decision instead.

### MINOR — must be resolved before implementation (not blocking design approval)
1. **MINOR-1 — E1-PERF's exact new chunk-size constant is not fixed in the design**
   (intentionally, per §2's disposition: "exact target value... to be
   confirmed against a memory-headroom check at implementation time"). This
   is an acceptable design-time deferral (the design already bounds the
   decision — recommended range 5,000-10,000, ~3.65-7.3 MB peak buffer) but
   the implementer must not silently pick a value without recording the
   chosen constant and its memory-headroom justification in the
   implementation handoff.
2. **MINOR-2 — §9 row 9 (Diff/status regression coverage after a cold branch switch)**
   is honestly flagged as a gap requiring implementation-time action (grep
   for an existing fixture before adding one) — this is correctly not
   claimed as done, but the design does not yet assign it a hard slice
   checkpoint (it says "add to E1-TEST or E4-VERIFY's fixture set" as an
   either/or). **Required resolution before implementation**: the
   implementing session must pick exactly one owning slice for this fixture
   (recommend E4-VERIFY, since it's about post-branch-switch behavior, not
   OBJ_INDEX internals) and record that decision, rather than leaving it
   ambiguous between two slices at commit time.
3. **MINOR-3 (carried forward from the prior review, already resolved by the design text — not one of the 2 counted MINOR findings)**:
   D2's diagnostic flag, if ever implemented, must be scoped
   by `repo_key` per §3's own text — this is already stated correctly in
   the corrective design (unlike the prior draft's ambiguity), so this is
   listed here only as a confirmation that the earlier MINOR finding is now
   resolved by the design text itself, not as a new finding.

## Resolution status (bootstrap consistency pass, 2026-07-29)

Both counted MINOR findings are resolved before any implementation starts.
No `AUTHORIZED_NOW` slice depends on an unresolved minor finding as of this
pass.

```text
MINOR-1: ALREADY_APPLIED. `variant_b_package_e_design.md` §2 now fixes the
  E1-A contract exactly (constant `c_index_write_chunk_size` = 5000, byte
  bound ~3.65 MB, full memory/lock/transaction/rollback model, exact IT8
  before/after measurement plan). The prior "to be confirmed at
  implementation time" deferral is withdrawn — there is no longer an open
  choice for the implementer to make or silently skip.
MINOR-2: IMPLEMENTATION_PRECONDITION for E4-VERIFY (hard-assigned, no
  longer an either/or with E1-TEST). `variant_b_package_e_design.md` §9
  row 9 now names E4-VERIFY as the sole owning slice. This is not a design
  defect and does not block E4-VERIFY's `AUTHORIZED_NOW` status — it is a
  concrete, tracked deliverable that must be completed as part of that
  slice's own implementation (add the status-calc-after-cold-switch
  fixture, or confirm via grep that an equivalent fixture already exists)
  before E4-VERIFY is considered complete.
```

### Not applicable / no finding
No new exception class, no new DDIC object authorized in this pass (D1's
tool reads existing tables only), no new transaction boundary in any
`AUTHORIZED_NOW` slice. Nothing else to check for the project's own
documented ABAP correctness pitfalls (`DEFAULT`+inline-`LENGTH`, `FOR ALL
ENTRIES` clause order, EXPORTING/IMPORTING direction mismatches) since no
new method signature or SELECT statement shape is specified at this design
granularity; the implementer must still self-check these at implementation
time as with any new ABAP code, per standing project practice.

## Verdict

```text
CORRECTNESS_REVIEW=APPROVE_WITH_MINOR_REVISIONS
BLOCKING=0
MINOR=2, BOTH RESOLVED this pass (bootstrap consistency review, 2026-07-29
  — see "Resolution status" section above): MINOR-1=ALREADY_APPLIED (E1-A
  contract fixed exactly in design §2); MINOR-2=IMPLEMENTATION_PRECONDITION
  for E4-VERIFY (hard-assigned in design §9 row 9, no longer either/or).
  Verdict label retained as APPROVE_WITH_MINOR_REVISIONS rather than
  upgraded to APPROVE, since this consistency pass did not re-run a full
  review cycle — no new blocking finding was raised, and neither remaining
  item is now capable of blocking an AUTHORIZED_NOW slice.
ALL_10_CR_ITEMS=RESOLVED_IN_DESIGN (CR-09 is out of this artifact's scope by
  nature; CR-08's bootstrap-handoff half remains a separate pending action
  tracked outside this review, not a design defect)
```
