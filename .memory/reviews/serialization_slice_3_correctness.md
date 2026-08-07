# SER-SLICE-3 Phase 2 — independent correctness review (DOMA/DTEL provider)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_2_IMPLEMENTATION_CORRECTNESS_REVIEW
REVIEWER_VERDICT=REVISE_AND_REVIEW_ONCE (1 blocker, 2 major, 2 minor)
STATUS=FIXES_APPLIED_SELF_VERIFIED_NOT_INDEPENDENTLY_RE_REVIEWED
```

Honesty disclosure: the reviewing subagent's exact required-changes list was
applied mechanically by the orchestrator and re-verified by re-reading the
corrected source (not by a second independent subagent pass). This is
disclosed explicitly rather than claimed as a fresh APPROVE - see
`.memory/logs/serialization_slice_3_doma_dtel.md` for the exact fix-by-fix
disposition (DR-001..DR-005) and self-verification notes.

## Reviewer's raw findings (verbatim from the compact envelope)

```text
DR-001|blocking|prepare_doma's lt_langs (DD01T∪DD07T text-row langs)
  doesn't guarantee iv_main_language membership; if a domain has no
  DD01T/DD07T row in mv_language, ls_cache-dd01v/dd07v_tab stay fully
  INITIAL (never assigned via CORRESPONDING). get_doma_data then returns
  es_dd01v=initial with rv_found=TRUE, so the seam's "IF ls_dd01v IS
  INITIAL...RETURN" silently drops the WHOLE domain (header+fixed values)
  - unlike DDIF_DOMA_GET, which unconditionally returns DD01L-derived
  header fields regardless of text presence. Real data-loss risk in
  multi-language use.
DR-002|major|No feature-ON/provider-hit parity test exists for DOMA
  (design §7 required byte-identical-XML feature-ON cases); ltcl_doma_
  parity is unchanged from SER-SLICE-1, never sets is_serial_prefetch_
  active. This gap is why DR-001 wasn't caught (test fixtures XFELD/CHAR30
  happen to have text in 'E').
DR-003|major|No test drives a genuinely corrupt/truncated buffer through
  the IMPORT TRY/CATCH in inject_batch_from_buffer; existing reject_*
  tests use well-formed EXPORT/IMPORT structures with only semantic
  mismatches (version/count/dup), never exercise the CATCH cx_root branch.
DR-004|minor|Design §3 "unexpected entry" and "valid empty payload" cases
  have no dedicated batch-wire test (structural argument is sound but
  unverified).
DR-005|minor|Latent (not currently reachable): if a future dispatch ever
  sends both iv_prefetch_buffer_ext and iv_prefetch_buffer_dd non-initial,
  inject_from_buffer populates mt_dtel then inject_batch_from_buffer
  unconditionally CLEARs+overwrites it - silent DTEL loss for ext-only
  entries. before_dispatch never sets iv_prefetch_buffer_ext today so no
  current trigger.

DOMA_SEAM_PARITY=GAP-main-language merge fails to reproduce DDIF_DOMA_GET's
  unconditional header return when no DD01T/DD07T text exists in
  mv_language (DR-001)
LANGUAGE_GUARD_SAFE=YES-get_doma_data guard mirrors get_dtel_data exactly;
  inject_batch_from_buffer writes mv_language before any per-object
  serialize() call in Z_ABAPGIT_ORTEC_SER_BATCH
WIRE_VALIDATION_ORDER=CORRECT-IMPORT(try/catch)->version->object_count->
  duplicate-sort-check all precede CLEAR mt_doma/mt_dtel; RFC TRY/CATCH
  swallows zcx_abapgit_exception without aborting the batch
SPLIT_CAP_REPURPOSE_SAFE=YES-grep confirms c_max_pre_dispatch_splits was
  DECLARED_ONLY/telemetry (no other reads); now enforced via
  split_depth_at_cap with 3 boundary tests; over-cap groups route to
  route_to_sequential_fallback
DD_BUFFER_THREADING=CORRECT-computed once per before_dispatch invocation,
  passed unchanged to that invocation's dispatch_batch call; split
  recursion legitimately recomputes per (smaller) half per design, not a
  bug
DTEL_REGRESSION_RISK=NONE-today; DR-005 is a latent risk only if
  iv_prefetch_buffer_ext is ever wired into this dispatch path
```

## Disposition (applied by the orchestrator after this review)

```text
DR-001 FIXED - lt_langs now seeded with iv_main_language unconditionally
  before the DD01T/DD07T discovery loops (prepare_doma).
DR-002 FIXED - PROVIDER_HIT_MATCHES_BASELINE, PROVIDER_HIT_NO_FIXED_VALUES
  added to ltcl_doma_parity.
DR-003 FIXED - REJECT_CORRUPT_IMPORT added to ltcl_dd_batch_wire.
DR-004 FIXED - UNEXPECTED_ENTRY_IGNORED, EMPTY_PAYLOAD_IS_HIT added.
DR-005 NOT FIXED, DOCUMENTED - no current trigger; recorded as a binding
  constraint for whichever future slice wires iv_prefetch_buffer_ext into
  this same dispatch path.
```

All fixes independently re-read by the orchestrator against the reviewer's
exact wording; `get_errors` re-run clean after each fix. Not re-submitted
for a second independent subagent review pass (see STATUS above) - flagged
as an explicit IT8 pre-condition in the consolidated validation plan
(`.memory/logs/serialization_slice_3_it8_validation_plan.md`).

## Addendum: parity-incident fix review (2026-08-07)

See `.memory/incidents/serialization_slice_3_dtel_doma_parity.md` for the
full incident. A dedicated correctness+regression review of the parity-
incident fixes (Fix A/B/C, prepare()/clear() wiring, extract_for_batch's
all-miss guard, and the zero-file-success guard) was run:

```text
VERDICT=APPROVE_WITH_MINOR_REVISIONS (0 blocker, 1 major, 1 minor)
DR-001 (major, ACCEPTED as disclosed scope boundary) - ser_pref/ser_pref_
  oo (MSAG/CLAS/INTF families) are now prepare()'d/clear()'d on every
  SERIALIZE() call, matching the classic path's own cost, but are still
  never extracted/injected into Z_ABAPGIT_ORTEC_SER_BATCH (only pref_ext's
  DOMA/DTEL envelope is wired) - the bulk SELECTs for those two families
  benefit only the forced_seq/WAPA/in-process-fallback subset, not the
  RFC-dispatched majority. Pre-existing SER-SLICE-2 scope boundary,
  unchanged by this incident fix, not a correctness defect.
DR-002 (minor, FIXED) - the run-id UUID-generation-failure path in
  SERIALIZE (before any run context exists) did not call clear() on the
  three prefetch classes before re-raising - fixed by adding the same
  three clear() calls to that CATCH block.
```
