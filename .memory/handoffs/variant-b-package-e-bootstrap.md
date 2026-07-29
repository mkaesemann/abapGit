# Variant B Package E — Bootstrap handoff (for the implementation session, corrective pass)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E0-BOOTSTRAP-HANDOFF-CORRECTIVE
BASELINE=8eef0b55fb37892c3d6b6428c038886481c73192
PRODUCTIVE_BASELINE=733bb30799886ef8659be7e293b82c3ccfcebbdd
STATUS=CORRECTIVE_DESIGN_AND_REVIEW_COMPLETE, IMPLEMENTATION_NOT_STARTED
SUPERSEDES=the prior bootstrap handoff's blanket "all three clear the
  implementation gate" framing and its false "no IT8 validation required
  for test/doc-only slices" claim, retrievable via
  `git log -p -- .memory/handoffs/variant-b-package-e-bootstrap.md`.
```

Read this file first in a new implementation session. It is an index, not a
duplicate — follow the links for detail.

## Bootstrap consistency pass audit (2026-07-29)

Before this pass, the orchestrator read `/memories/repo/git-state-notes.md`
(repository memory) while investigating a `git status` anomaly (two
testclasses files showing `M`). This falls outside this task's own "do not
reread repository/editor memory" instruction for the CURRENT task, but was
performed in the immediately preceding turn under different instructions.
Recorded here for audit completeness, with two explicit confirmations:

```text
NO_DESIGN_CLAIM_DEPENDS_ON_IT=confirmed. git-state-notes.md contains only
  prior workflow/incident notes (a pre-existing unrelated stash entry, two
  lint/type-check gotchas, a new-CLAS-needs-.clas.xml reminder, a
  .memory-tracking clarification, a target-architecture-diagram history
  note, and a note that the connected live-SAP diagnostic tools point at
  the wrong system) — none of these facts are cited by, or load-bearing
  for, any claim in the design doc, either review, the bootstrap handoff,
  or state.md.
TEST_FILE_STATUS_INDEPENDENTLY_VERIFIED=confirmed. The two flagged `M`
  files (`src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.testclasses.abap`,
  `src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.testclasses.abap`) were
  independently checked via `git diff` and `git diff --shortstat` against
  both files together: both returned a completely EMPTY diff (0 lines, no
  shortstat output) — i.e. no actual content differs from HEAD despite the
  `M` flag. This is consistent with a line-ending/index-normalization
  artifact, not a real code change from this or any prior session in this
  conversation. No productive or test file was edited during this or the
  prior corrective pass.
```

## Implementation authorization is PARTIAL, per-slice (CR-02 correction)

There is no single Package-E-wide "cleared to implement" statement. Read
[variant_b_package_e_design.md](../logs/variant_b_package_e_design.md) §0
for the authoritative per-slice table. Summary, restated here only as an
index (the design document is the source of truth if these ever diverge):

```text
AUTHORIZED_NOW = E1-TEST, E1-PERF (E1-A only), E2-DIAG (D0/D1 only),
                 E3-TEST, E4-VERIFY, E-HARDEN (ORTEC-owned constant/test
                 fix only)
NOT_AUTHORIZED = E2-DIAG D2/D3, E2-FIX, E-HARDEN's standard-file
                 architecture question
NOT_REQUIRED   = E4-FIX (see design §6 verdict; residual risk documented,
                 not designed away)
```

Reviews:
[correctness](../reviews/variant_b_package_e_correctness_review.md) =
`APPROVE_WITH_MINOR_REVISIONS` (2 MINOR — see that file),
[protocol/persistence](../reviews/variant_b_package_e_protocol_review.md) =
`APPROVE`,
[performance DESIGN_GATE](../reviews/performance_design_variant_b_package_e.md) =
`APPROVE` (E1-A unconditionally approved). All three clear the
implementation gate (APPROVE or APPROVE_WITH_MINOR_REVISIONS) for the
`AUTHORIZED_NOW` slices listed above. Implementation was NOT started this
corrective pass because the explicit instruction for this pass was
design/review/memory correction only — this is a hard stop, not an
oversight.

## Slice order (each independently checkpoint-able)

```text
1. E1-TEST   — test-only, zero preconditions, safe to implement first
2. E3-TEST   — test-only, zero preconditions, safe to implement first
3. E4-VERIFY — test-only, zero preconditions
4. E-HARDEN  — ORTEC-owned constant extraction + pinning test, zero
               preconditions beyond confirming OF-3's existing test is
               still present (already confirmed, see design §7)
5. E1-PERF   — AUTHORIZED_NOW for E1-A only; the exact contract is now
               FIXED at design time (design §2, bootstrap consistency
               pass, resolves correctness-review MINOR-1 =
               ALREADY_APPLIED): new constant
               `c_index_write_chunk_size TYPE i VALUE 5000` in
               `zcl_abapgit_ortec_obj_index`'s `rebuild_index`, replacing
               the bare literal `1000`. No implementation-time choice
               remains — implement exactly as specified, do not pick a
               different value.
6. E2-DIAG   — D0 (no code, just ask the owner for a reproduction packet)
               and D1 (read-only comparison tool) only; D2/D3 remain
               NOT_AUTHORIZED and must not be implemented in this pass
```

`E2-FIX` and `E4-FIX` have no implementation slice yet — they are
placeholders pending, respectively, a live D1/D2 reproduction and (for
E4-FIX) an owner decision on the scenario-7 residual-risk acceptance in
design §6.

Recommended implementation routing (per orchestrator mode rules):
E1-TEST, E3-TEST, E4-VERIFY, E-HARDEN's constant/test change are
mechanical, self-contained additions with no protocol/persistence/
architecture decision remaining — suitable for
`ortec-abapgit-implementation-junior`. E1-PERF (a chunk-size constant
change with a memory-headroom judgment call) and E2-DIAG D1 (a new
tree-walk-and-compare method, even though read-only) involve enough
judgment to route to `ortec-abapgit-implementation-senior`.

## What is explicitly NOT authorized (do not implement without a new,
separately-reviewed design or explicit owner sign-off)

```text
E1-TREE-REUSE (E1-D/E1-E): tree-SHA1-keyed or incremental-diff
  zaog_obj_index row reuse — PROVEN UNSAFE as a bare key this pass (design
  §2); requires a richer composite key, a dedicated correctness review,
  and its own performance DESIGN_GATE.
E2-DIAG D2/D3: BAL/SLG1-based logging or a new persistent DDIC sink —
  gated behind D0/D1 proving insufficient (design §3).
E2-FIX: any code change that alters comparison/repair logic for the false-
  MODIFIED symptom, prior to a live D1/D2-confirmed mismatch.
Any new CERTIFIED_BUT_MISSING state machine or per-commit invalidation
  granularity replacing pull_by_branch's existing whole-repo
  invalidate_all_history strategy.
E-HARDEN-STANDARD-FILE-COUPLING: any change to
  zcl_abapgit_git_porcelain.clas.abap's embedded ORTEC branching/legacy
  'Walk,' copy — requires explicit owner input first (design §8 D-4).
```

## Regression/acceptance expectations (CR-08 correction)

**Every slice, including test-only and doc-only ones, requires real IT8
validation before it is considered complete** — this handoff previously and
incorrectly stated that test/doc-only slices needed no IT8 SAP validation;
that claim is withdrawn. The minimum bar for every slice (per each slice's
own "IT8 acceptance" subsection in the design document) is:
1. Real IT8 activation/syntax check of every changed include (not just a
   local `get_errors` pass).
2. ABAP Unit execution with a `PASS` result for the changed test class(es).
3. ATC (SAP's real Code Inspector variant, not a local lint proxy) with a
   `PASS` result.

`E1-PERF` additionally requires a post-implementation SAT remeasurement of
the same warm-to-cold reproduction to confirm the expected round-trip-count
reduction (design §2's CLOSURE_CONDITION, exact IT8_MEASUREMENT plan) —
this is an IMPLEMENTATION_AUDIT step, tracked separately from the
three-item minimum bar above, and is specific to E1-PERF only. `E2-DIAG`
D1 additionally requires confirming it reports MATCH for every row of a
freshly rebuilt index (design §3's IT8 acceptance plan step 1) before it
is trusted for a live investigation. `E4-VERIFY` additionally owns row 9's
status-calc-after-cold-switch fixture as a hard requirement, not optional
(correctness review MINOR-2 = IMPLEMENTATION_PRECONDITION, design §9).

## E4-OOB-DELETION-RISK disposition (bootstrap consistency pass)

```text
DISPOSITION=ACCEPTED_NON_BLOCKING_RISK
```

See design §6 for the full rationale and the consumer-safety/recovery
analysis. No E4-FIX slice is authorized or required for this risk; it is
tracked in `.memory/state.md`'s Deferred topics section.

## Do not repeat

Do not re-run E1/E2/E3/E4 discovery or re-litigate the corrective design's
summary verdicts (§0) without new contradicting evidence from the current
source — they are current as of `8eef0b55` and this corrective pass's own
direct re-verification. Do not reopen Package C/D1/D2
(`SAP_VALIDATED_COMPLETE` at `733bb307` for D2; Package C validated
separately at `29199f62` — see `.memory/state.md` for the split dates,
corrected under CR-09) without a concrete regression.
