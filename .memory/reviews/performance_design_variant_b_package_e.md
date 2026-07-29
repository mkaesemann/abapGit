# Variant B Package E — Performance design gate (DESIGN_GATE, corrective pass)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E0-PERFORMANCE-DESIGN-GATE-CORRECTIVE
MODE=DESIGN_GATE
REVIEWED_ARTIFACT=variant_b_package_e_design.md (corrective, §0-§12)
SUPERSEDES=the prior performance DESIGN_GATE of the E0-DESIGN draft
  (APPROVE_WITH_MINOR_REVISIONS, 1 MINOR, OQ-2 only), retrievable via
  `git log -p -- .memory/reviews/performance_design_variant_b_package_e.md`.
BASELINE=8eef0b55fb37892c3d6b6428c038886481c73192
```

## Applicability

Of the 9 slices, two touch repository-scale processing and require a full
gate; the rest are approved by inspection:

```text
E1-TEST  = test-only; bounded fixtures — approved by inspection
E1-PERF  = repository-scale: directly changes the write-path chunk size for
           `rebuild_index`, exercised on every cold-init/rebuild at up to
           ~42,000 rows per commit — FULL GATE REQUIRED (primary subject of
           this corrective pass's CR-04)
E2-DIAG  = D0 = no code; D1 = on-demand, single-path, O(path-depth) —
           bounded by construction, not repository-scale in the way a
           per-row hot-path change would be, but still reviewed below since
           it touches the persistence/read layer; D2/D3 = gated, reviewed
           only for their AUTHORIZATION preconditions, not their exact
           implementation (not yet designed in enough detail to gate)
E2-FIX   = NOT_AUTHORIZED placeholder — no design to gate yet
E3-TEST  = test-only; bounded fixtures — approved by inspection
E4-VERIFY = test-only; exercises existing repair mechanisms at fixture scale
           — approved by inspection (no new production algorithm)
E4-FIX   = NOT_REQUIRED — no design to gate
E-HARDEN = pure constant extraction, zero runtime cost difference —
           approved by inspection
```

## E1-PERF — full DESIGN_GATE (CR-04 primary deliverable)

### Ranked, quantified candidate table (independently re-verified this pass)

| ID | Candidate | Round trips (before→after) | Peak memory | DB/lock impact | Risk | Gate verdict |
| --- | --- | --- | --- | --- | --- | --- |
| E1-A | Raise MODIFY chunk 1000→5000 | 41 MODIFY/82 exec → ~9 MODIFY/~18 exec (≈4.5x fewer round trips) | ~0.73 MB → ~3.65 MB (`lt_rows` at 730 B/row) — trivial against any realistic ABAP work-process memory budget | Repo lock hold time expected to SHRINK (fewer round trips over the same total row volume); no new COMMIT; no lock escalation risk (still one MODIFY per chunk, same table) | Very low — pure constant change, same statement shape, same table, same key | **APPROVE_UNCONDITIONALLY** |
| E1-B | Adaptive byte-target batching | Same or worse than E1-A in the achievable case (row size variance ≈0, per `zaog_obj_index.tabl.xml`'s fixed-width CHAR fields) | Adds runtime response-size measurement logic and its own buffer bookkeeping — net memory overhead for zero evidenced benefit here | No additional impact vs. E1-A, but no benefit either | Medium (more code paths, more edge cases, e.g. div-by-zero guards on a byte-target formula) for no evidenced gain | **DO NOT PURSUE** unless E1-A's post-implementation SAT remeasurement proves insufficient AND shows real byte-size variance (not expected for this DDIC shape) |
| E1-C | Reduce ~2 execs/MODIFY HANA behavior | N/A — the incident's own evidence (§6 of the SAT report) attributes this to normal HANA array-upsert client/server round-trip behavior, not an ABAP-controllable lever | N/A | N/A | N/A — no actionable ABAP-level design exists | **NOT A SEPARATE CANDIDATE** — its benefit is captured proportionally by E1-A (fewer total MODIFYs ⇒ fewer total execs even at the same ~2x/statement ratio) |
| E1-D | Tree-SHA1-keyed cross-commit reuse (bare key) | Would eliminate the walk+write entirely for a repeat subtree — but see correctness gate below | New secondary index memory/maintenance cost, unquantified pending a real design | New non-unique secondary DDIC index; write-amplification on every index maintenance | **BLOCKED ON CORRECTNESS** — proven unsafe as a bare `(repo_key, tree_sha1)` key this pass (see design §2's `file_to_object`/`.abapgit`-context finding) | **REJECT AS DESIGNED; NOT GATED FURTHER until a corrected composite-key design exists** |
| E1-E | Incremental tree-diff vs. known prior commit | Highest theoretical reduction (rows changed only, not full re-walk) but wholly undesigned at the algorithm level | Unquantified — depends entirely on a diff algorithm not yet specified | Unquantified | High engineering/correctness complexity (deletion handling, "which prior commit" policy, same `.abapgit`-context proof E1-D needs) | **DEFER — requires its own future DESIGN_GATE once a concrete algorithm exists; not gateable from a one-line description** |
| E1-F | No change, accept current cost | 0 | 0 | 0 | 0 | **NOT RECOMMENDED given E1-A is free-to-approve and evidence-backed; retained only as an explicit fallback if E1-A were ever found to be unsafe (it is not)** |

### Why E1-A is approved unconditionally (not merely "with minor revisions")

Unlike the prior review's blanket, unranked treatment of "batch size" as an
unexamined implementation detail, this pass's independent re-derivation of
the underlying SAT numbers (Phase I = 9,390,412 µs of 31,417,756 µs total =
29.9% — recomputed by this reviewer directly from
[variant_b_d2_sat_warm_to_cold_o4h8794.md](../incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md),
not copied from the design's own prose) confirms:
1. The cost is round-trip-count-driven (the incident's own §6 states this),
   so increasing chunk size directly and proportionally reduces the
   dominant cost driver.
2. The per-row byte size (~730 B, independently recomputed from
   `zaog_obj_index.tabl.xml`'s field lengths: 3+12+40+4+40+40+255+255+40+40+1
   = 730) makes even a 10,000-row batch trivially small in absolute memory
   terms (~7.3 MB) — no realistic risk of exceeding any ABAP work-process
   memory ceiling.
3. No DDIC, lock, or transaction-boundary change accompanies it.

This combination — a change proven to target the actual dominant cost,
bounded and quantified memory impact, and zero correctness surface — is
exactly the profile of a change this gate should approve without
conditions, rather than deferring behind an open question as the prior
review's blanket MINOR treatment implicitly did.

### Required post-implementation validation (not a gate condition, a closure condition)

`E1-PERF` still carries a CLOSURE_CONDITION (stated in the design §2, not
invented here): a live post-implementation SAT remeasurement of the same
warm-to-cold reproduction must confirm the expected round-trip reduction
before E1's overall performance story is declared fully closed. This is a
standard IMPLEMENTATION_AUDIT step, not a precondition for starting
implementation.

## E2-DIAG — evidence-ladder performance review (CR-03 redesign)

- **D0**: zero runtime cost (no code). Approved by inspection.
- **D1**: the read-only comparison tool is bounded by tree DEPTH for one
  path, not by commit size — for a repository whose deepest path is, say,
  10-15 directory levels, this is a handful of `zaog_obj_store` reads per
  invocation, independent of the ~42,000-row commit-wide scale that made
  E1-PERF's cost significant. Because it is an ADMINISTRATOR-TRIGGERED,
  ON-DEMAND diagnostic tool (not a hot-path addition), its acceptable
  latency budget is far more permissive than a per-Stage-build cost; even
  if it were an order of magnitude more expensive than estimated, it would
  not threaten repository-scale throughput since it never runs
  automatically. **APPROVE_UNCONDITIONALLY.**
- **D2 (gated)**: reusing BAL/SLG1 for one bounded log write per served
  batch (never per row) mirrors the exact "batch DB writes, never per-row"
  principle this project has already learned the hard way (per the
  project's own incident history) — the design correctly avoids repeating
  that class of mistake. Because it is gated behind D0/D1 proving
  insufficient and is off by default, this gate does not need to fully
  quantify its cost now; it is conditionally pre-approved on the same
  batching principle, with a full quantification required only if/when D2
  is actually proposed for implementation.
- **D3 (last resort)**: not designed in enough detail to gate; explicitly
  deferred, consistent with its NOT_AUTHORIZED status.

## E4-VERIFY / E-HARDEN — approved by inspection

Neither slice introduces a new production algorithm or hot-path code path.
E4-VERIFY's new tests exercise EXISTING repair mechanisms at fixture scale;
E-HARDEN's constant extraction has zero runtime cost difference (a `CONSTANTS`
reference resolves at compile time, identical generated code shape to two
independent literals). No gate objection.

## Deferred item re-confirmation (E1-D/E1-E, D-1 in design §8)

This gate explicitly re-confirms: E1-D is REJECTED AS DESIGNED (not merely
deferred) due to the correctness finding, and E1-E remains UNDESIGNED at the
algorithm level. Neither receives a conditional pre-approval — both require
a genuinely new design and their own future DESIGN_GATE before any
implementation authorization, tracked as `E1-TREE-REUSE` in
`.memory/state.md`.

## Verdict

```text
PERFORMANCE_DESIGN_GATE=APPROVE
BLOCKING=0
MINOR=0
E1_PERF_CANDIDATE_APPROVED=E1-A (unconditional)
E1_PERF_CANDIDATES_REJECTED_OR_DEFERRED=E1-B (not justified), E1-C (folded
  into E1-A), E1-D (rejected as designed, correctness-blocked), E1-E
  (undesigned, deferred), E1-F (fallback only, not needed)
NOTE=This supersedes the prior review's single unranked MINOR finding
  (OQ-2) with a fully quantified, ranked E1 evaluation per CR-04. OQ-2
  (D2's retention/capping) is resolved by design itself this pass (BAL/
  SLG1's own SBAL_DELETE housekeeping) rather than left open, so it is no
  longer carried as a MINOR finding.
```
