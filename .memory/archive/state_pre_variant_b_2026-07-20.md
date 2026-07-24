# Ortec abapGit opt-rework persistent state

## Working conventions
- Commit messages: describe functionality/technical changes only. Do not reference local
  memory files (`.memory/...`) or Michael by name in commit messages (2026-07-12, Michael's
  instruction).
- Delegate simple/mechanical work to subagents on smaller models to conserve context budget
  for the rest of the implementation (reinforced 2026-07-12: token efficiency is important
  until the whole implementation is complete).
- After a phase is imported into IT8, run an ATC check via the `mcp_arc-1_SAPDiagnose`
  (action=`atc`) tool - delegate this to a subagent - rather than waiting for a manual report.
  ATC is a separate signal from the ADT syntax check Michael runs on import: syntax-clean code
  can still have ATC findings (e.g. unhandled/undeclared exceptions) that need a follow-up fix.

## Repository state
- Work branch: `ortec/abapgit_1_133-opt-rework`
- Source branch: `ortec/abapgit_1_133-optimized`
- Historical fast-but-wrong baseline: `b4f41e38372a0fe9f67483f71e968b1885b594c1`
- Current HEAD analyzed: workspace snapshot from the discovery pass

## Current phase
- Phase: implementation-phase-7-complete (Phases 1, 3, 4, 4b, 5a, 5b.1, 5b.2, 6, and 7 all implemented/validated and committed; the crash-fix commit `46b3f398` is also landed. All planned phases are now done. Remaining open item: an empirical large-repo latency benchmark once Michael has a live environment available - everything else is closed.)
- Owner agent: orchestrator (direct implementation) + ortec-abapgit-regression + ortec-abapgit-performance (validation)
- Productive ABAP changes allowed: yes, phase-by-phase per approved design

## Active topic

- Topic ID: variant-b-partial-clone
- Status: APPROVED_PENDING_RECONCILIATION
- Owner-approved specification:
  copilot-prompt-variant-b-abapgit.md
- Current phase: Slice 0 – current-source reconciliation
- Last completed slice: none
- Next action:
  Run focused reconciliation against current productive source.
- Blocking condition: none
- Supersedes:
  H4 walk delegation as a standalone topic
- Last updated: 2026-07-20

## Topic index

### variant-b-partial-clone
- Status: ACTIVE
- Current decision: Variant B
- Design review: pending
- Latest handoff: none
- Latest regression: none

### H4 walk delegation
- Status: SUPERSEDED_AS_STANDALONE_TOPIC
- Superseded by: variant-b-partial-clone
- Existing code/tests: retain pending reconciliation

## Current implementation slice
- Slice: phase-1-streaming-base-cache
- Scope: standalone byte-budgeted LRU cache for future delta-base bytes; no decode-path integration yet.
- Implemented files: [src/ortec/git/zcl_abapgit_ortec_base_cache.clas.abap](src/ortec/git/zcl_abapgit_ortec_base_cache.clas.abap), [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap)
- Validation evidence: workspace diagnostics report no errors for either file; ABAP method-name length check completed with no over-limit names.
- Follow-up: next phase can wire the cache into the decode path once the design scope is approved.

## Superseded or absorbed topics

### H4 walk delegation

- Status: SUPERSEDED_AS_STANDALONE_TOPIC
- Superseded by: variant-b-partial-clone
- Reason:
  The original H4 topic addressed missing tree/blob recovery during branch
  switching. Variant B now defines the broader durable architecture:
  repository-shared object storage, cold-branch blobless graph acquisition,
  bulk current-tip blob materialization, verified have eligibility, and bounded
  recovery.
- Reuse:
  Existing H4 walk-preparation, bulk collection, batching, and regression tests
  remain implementation evidence and may be reused where compatible.
- Restrictions:
  Do not resume H4 as an independent architecture project.
  Do not delete working H4 code merely because the standalone topic is closed.
  During Variant B reconciliation, classify each H4 component as:
  - REUSE_UNCHANGED
  - ADAPT
  - REPLACE
  - OBSOLETE
- Replacement topic:
  `variant-b-partial-clone`

## Discovery 2026-07-14
- Completed a read-only audit of the fastpath-active pull/fetch/Stage/Diff path and captured the current as-is call chains in [.memory/diagrams/current_slow_path.mmd](.memory/diagrams/current_slow_path.mmd).
- Confirmed the main anomaly: in [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap), the ORTEC `upload_pack` path attempts `decode_and_persist`, and when that path fails, it falls back to the standard [src/git/zcl_abapgit_git_pack.clas.abap](src/git/zcl_abapgit_git_pack.clas.abap) decoder rather than staying within the ORTEC pack-decoder path. This is the clearest crossover back to standard pack-decode logic while the fastpath switch is active.
- Captured the intentional fallback points as well: the filtered Stage/Diff read path in [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap) and the transport-layer fastpath cascade in [src/git/zcl_abapgit_git_transport.clas.abap](src/git/zcl_abapgit_git_transport.clas.abap). Both are deliberate; the in-method pack-decoder fallback above is the unexpected anomaly that warranted the separate audit entry.
- Wrote the detailed evidence log to [.memory/logs/fastpath_fallback_audit.md](.memory/logs/fastpath_fallback_audit.md). No productive ABAP code was changed.
- **Orchestrator correction (2026-07-14, same day):** the discovery subagent's diagram output overwrote the pre-existing archaeology diagram [.memory/diagrams/current_slow_path.mmd](.memory/diagrams/current_slow_path.mmd) instead of creating a new file as instructed. Since `.memory/` is untracked by git (confirmed via `git log`/`git status` - no history to recover from), the original content would have been permanently lost; it was restored verbatim from this session's own earlier context. The new fastpath-active call-trace diagram was moved to its own correctly-named file
  [.memory/diagrams/fastpath_active_call_trace.mmd](.memory/diagrams/fastpath_active_call_trace.mmd), and the audit log was superseded by a more precise version at
  [.memory/logs/fastpath_isolation_audit.md](.memory/logs/fastpath_isolation_audit.md) (the old `fastpath_fallback_audit.md` is now redundant with it).
  **Root-cause detail the subagent's summary missed** (verified directly against
  `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap` `METHOD upload_pack`, L590-779): the
  fallback to standard `zcl_abapgit_git_pack=>decode` is reached via a **silent, unconditional
  fallthrough** - `decode_and_persist` is called inside `TRY ... CATCH zcx_abapgit_exception.`
  with an **empty catch body** (no re-raise, no log), and the standard-decode line immediately
  below runs unconditionally whenever the Ortec attempt didn't already `RETURN`. This happens
  identically on **both** the thin (`iv_allow_thin=abap_true`) and non-thin retry
  (`iv_allow_thin=abap_false`) attempts in `upload_pack_by_branch`/`upload_pack_by_commit`
  (L438-583), so the documented Phase 5b.2 "3-tier cascade" (thin Ortec -> non-thin Ortec ->
  standard, with "standard" meaning a separate re-negotiation caught by
  `zcl_abapgit_git_transport`'s `CATCH zcx_abapgit_ortec_git`) is not what actually runs in
  most cases: both of the first two tiers privately absorb `decode_and_persist` failures via
  this silent fallback and often return "successfully" using the standard decoder, so the
  documented third tier rarely fires. This matches the live SAT-trace evidence exactly (the
  slow `ZCL_ABAPGIT_ZLIB=>DECODE_LOOP_FAST` calls were traced as originating directly from
  `ZCL_ABAPGIT_ORTEC_FASTPATH`, not from `zcl_abapgit_git_transport`). **Not yet fixed** - the
  correct fix depends on knowing *why* `decode_and_persist` is failing in the live system,
  which the silent catch currently hides. Recommended next step: add temporary diagnostic
  logging in that catch block before deciding between (i) a bug fix in `decode_and_persist`
  or (ii) tightening the fallback to only silently substitute standard decode for the
  specific "pack wasn't actually thin" case, re-raising visibly for anything else. See
  [.memory/logs/fastpath_isolation_audit.md](.memory/logs/fastpath_isolation_audit.md) for
  the full write-up. Next action: awaiting Michael's direction on whether to add the
  diagnostic logging now, or go straight to a design pass for the fallback fix.
- **Root-cause fix implemented (2026-07-14, not yet committed at time of writing):** Michael
  live-debugged the actual trigger for the silently-swallowed `decode_and_persist` failure
  found above: a full/deepen "complete refresh" pack decodes correctly, then
  `zcl_abapgit_ortec_delta=>resolve_all` raises `Delta copy instruction exceeds base length`
  inside `resolve_one`'s REF_DELTA base lookup. Root cause: `resolve_one`'s
  `READ TABLE ct_objects ASSIGNING <ls_base> WITH KEY sha COMPONENTS sha1 = <ls_object>-sha1`
  used a plain first-match lookup on the table's `NON-UNIQUE SORTED KEY sha COMPONENTS sha1`
  (declared in `src/zif_abapgit_definitions.intf.abap`). The `sha1` field is overloaded: for
  an unresolved `ref_d`/`ofs_d` entry it holds that entry's own DECLARED BASE (a placeholder),
  never its own eventual identity - so a genuinely unresolved candidate (including possibly
  the searching entry itself, or the true base positioned AFTER its dependent - REF_DELTA
  carries no ordering guarantee unlike OFS_DELTA) could win the lookup instead of a genuinely
  resolved base, feeding wrong/short raw delta-instruction bytes into `apply()`. Fixed in
  `src/ortec/git/zcl_abapgit_ortec_delta.clas.abap` `METHOD resolve_one` by replacing the
  first-match `READ TABLE` with a `LOOP AT ct_objects ... USING KEY sha WHERE sha1 = ...`
  that skips any candidate still of type `ref_d`/`ofs_d`, only ever accepting an
  already-resolved base. New regression test `ltcl_ref_delta=>base_positioned_after_dependent`
  added to `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap` (immediately
  after `ltcl_ofs_delta`), constructing a REF_DELTA whose true base is deliberately
  positioned AFTER it in the objects table (hand-verified byte-exact: delta bytes
  `060790060121` correctly decode to a 6-byte copy + 1-byte insert against a "Hello!" base,
  producing "Hello!!" = `48656C6C6F2121`). Implemented by a delegated `MAI-Code-1-Flash`
  subagent from an exact, fully-specified diff; independently spot-checked by the
  orchestrator (`get_errors` clean on both files, manual re-read confirming exact match to
  spec, manual hex/byte-level verification of the new test's delta-instruction stream). Zero
  standard abapGit files touched. This directly closes the root cause of the earlier-found
  isolation violation (violation #1 in
  [.memory/logs/fastpath_isolation_audit.md](.memory/logs/fastpath_isolation_audit.md)): with
  `decode_and_persist` no longer failing on this class of pack, the silent fallback to
  standard `zcl_abapgit_git_pack=>decode` should no longer fire for this scenario. Not yet
  imported/verified on a real system.
- **Follow-up fix (2026-07-15, commit pending at time of writing):** the 2026-07-14 fix was
  necessary but insufficient - Michael's next live retest still hit
  `Delta copy instruction exceeds base length`, this time raised from `apply` via a
  `resolve_one -> resolve_one` chain (confirmed by the call stack), meaning the lookup fix DID
  find a genuinely-resolved (non-`ref_d`/`ofs_d`) candidate, but that candidate was still the
  WRONG object. Root cause: `resolve_one`'s final promotion step mutated `<ls_object>-sha1`
  (a component of the `sha` `NON-UNIQUE SORTED KEY`) via a plain field-symbol write obtained
  through `ASSIGNING`. Per documented ABAP behavior, writing to a key-participating field this
  way does NOT update the secondary key's internal sort structure, leaving it stale; every
  subsequent REF_DELTA base lookup in the same `resolve_all` pass (including the fixed
  `LOOP AT ... USING KEY sha` from the prior fix) then risks matching an unrelated row via
  that now-inconsistent key. This explains why the small 2-object unit test passed (a single
  resolution isn't enough to observably corrupt the key) while a real, large "complete
  refresh" pack (many sequential resolutions) reliably hit it. Fixed by replacing the
  field-symbol promotion with `MODIFY ct_objects FROM <ls_object> INDEX iv_tabix
  TRANSPORTING type data sha1.`, which correctly re-integrates the row and keeps the `sha`
  key valid for every later lookup. Before implementing, verified with Michael (who raised a
  sharp, correct question) that `iv_tabix`/`lv_base_tabix` index values themselves remain
  logically stable throughout (only `APPEND`-driven reallocation invalidates field-symbol
  *references*, never the logical index, since `ct_objects` is only ever grown via `APPEND`
  or modified in place - no `DELETE`/`SORT`/positional `INSERT` occurs anywhere in this code),
  and that the new `MODIFY` does not overlap with the read-only `LOOP AT ... ENDLOOP` scan
  from the prior fix (the loop always closes before any mutation happens). Added a second
  regression test, `ltcl_ref_delta=>resolve_after_prior_resolution_in_same_pass`, with four
  objects specifically forcing one delta to be fully resolved and promoted BEFORE a second,
  unrelated delta performs its own base lookup for a true base positioned even later - the
  scenario the first test's single-resolution setup couldn't exercise. Implemented by a
  delegated `MAI-Code-1-Flash` subagent from an exact, fully-specified diff; independently
  spot-checked by the orchestrator (`get_errors` clean on both files, manual re-read
  confirming exact match to spec, manual hex/byte-level verification of the new test's two
  delta-instruction streams). Zero standard abapGit files touched. Not yet imported/verified
  on a real system - this is the second attempt at the same live symptom, so the next retest
  result is important to confirm before considering this fully closed.
- **Root cause #4 found and hotfixed (2026-07-15, commit `43bbce1`):** the third live recurrence
  of the same symptom (after the two fixes above) triggered escalation to
  `ortec-abapgit-design` (fixed a broken agent registration along the way - `.github/agents/
  03b_design_review` was missing the required `.agent.md` extension, so `ortec-abapgit-
  design-review` had never been invokable; renamed to `03b_design_review.agent.md`). Design
  found the actual root cause: `resolve_one`'s REF_DELTA lookup captured `sy-tabix` from
  `LOOP AT ct_objects ... USING KEY sha` (a sorted SECONDARY key) and reused it directly as a
  PRIMARY table index for the recursive `resolve_one` call and `READ TABLE ... INDEX`/
  `MODIFY ... INDEX`. Per documented ABAP behavior, `sy-tabix` inside a secondary-key loop
  reflects that key's own iteration position, not the primary index - whenever a pack's
  SHA1-sorted order differs from its physical pack order (the normal case for any non-trivial
  pack), this silently operated on an unrelated row, explaining exactly why all three prior
  live failures recurred despite two independently-correct fixes, and why the small
  (2-4 object) unit tests never caught it (secondary and primary index coincided by
  construction in those tiny tables). Confirmed independently by `ortec-abapgit-design-review`
  (verdict APPROVE for this hotfix) before implementation, per the project's gating rule.
  Fixed in `zcl_abapgit_ortec_delta=>resolve_one` by capturing the candidate's own stable
  `index` field from within the secondary-key loop, then re-deriving the correct primary
  tabix via a separate, non-keyed `READ TABLE`; also hardened the thin-base-fetch path to bind
  directly to the just-appended row by its known position (`lines(ct_objects)`) instead of a
  non-unique-key lookup. Implemented by a delegated `MAI-Code-1-Flash` subagent from an exact
  diff; independently spot-checked by the orchestrator. Design also produced a broader
  architectural recommendation (`.memory/logs/delta_resolver_redesign.md`, reviewed with
  verdict APPROVE_WITH_MINOR_REVISIONS): replace the recursive, secondary-key-lookup-based
  mechanism entirely with a non-recursive fixpoint driver over an explicit hashed
  `sha1 -> primary tabix` side-index, which would eliminate all four discovered defect classes
  by construction. This broader redesign is NOT yet implemented - it is a tracked, reviewed,
  ready-to-implement follow-up pending Michael's go-ahead on scope/timing, not an immediate
  requirement, since the narrow hotfix directly addresses the confirmed root cause. Not yet
  imported/verified on a real system - this is the fourth attempt at the same live symptom
  area, so the next retest result remains important.
- **Follow-up performance + correctness gap found and fixed (2026-07-15, same session):**
  Michael reviewed the hotfix and correctly flagged that
  `READ TABLE ct_objects TRANSPORTING NO FIELDS WITH KEY index = lv_found_obj_index.` has no
  secondary key backing "index" on `ty_objects_tt`, so it is a full linear scan executed once
  per REF_DELTA object needing it - worst case O(n^2) on a large pack, and asked whether a
  once-built lookup index (forwarded from `resolve_all` to `resolve_one`) had been considered.
  It had not - investigating this also surfaced a related, previously-undiscovered
  correctness gap: `zcl_abapgit_ortec_obj_store=>get_object`/`get_objects` never populate the
  returned object's `index` field (confirmed by reading that class - only `sha1`/`type`/`data`
  are set), so every thin-base-fetched object defaulted to `index = 0`; two or more thin
  fetches in the same `resolve_all` pass could collide on that value, risking the same class
  of misresolution bug already fixed twice. Fixed by adding a new private hashed type
  `ty_tabix_by_index` (`obj_index -> primary tabix`) built once in `resolve_all` via a plain
  (non-keyed) loop and threaded through `resolve_one` as a new `CHANGING` parameter; all three
  `index`-based lookups in `resolve_one` (the REF_DELTA hotfix's fallback derivation, and the
  two pre-existing OFS_DELTA linear scans) now use this O(1) hashed lookup instead. Thin-fetched
  objects are now assigned a real, unique `index` (`lines(ct_objects) + 1`, before append) and
  immediately registered in the side-index. Added `ltcl_ref_delta=>two_thin_bases_do_not_collide`
  (using the same real-DB-fixture pattern already established elsewhere in this test file, via
  `zcl_abapgit_ortec_obj_store=>store_object` + `setup`/`teardown`) proving two distinct thin
  fetches in one pass resolve against their own correct bases. Implemented by a delegated
  `MAI-Code-1-Flash` subagent from an exact diff; independently spot-checked by the orchestrator
  (full re-read of both changed methods, manual trace of both existing `ltcl_ref_delta` tests
  against the new code confirming they still pass via the correct mechanism rather than
  small-table coincidence, and manual hex/byte-level verification of the new test's two delta
  streams). `resolve_all`'s public signature is unchanged; only `resolve_one`'s private
  signature gained the new parameter. Not yet committed at the time of this note - committing
  together with the tabix hotfix in one pass.
- **Delta resolver deep design pass (2026-07-15, agent: ortec-abapgit-design, model tier:
  large-reasoning [escalated; reason: root-cause + architecture decision after 3 failed fixes]).**
  Files/methods inspected first-hand: `src/ortec/git/zcl_abapgit_ortec_delta.clas.abap`
  (`resolve_all`/`resolve_one` REF+OFS branches, thin-fetch, promotion, `apply`, `get_offset`,
  `skip_size_header`); `src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap` `resumable_decode`
  (object-parse loop L712-930, resume-rehydrate branch, Targeted delta-base prefetch L1167-1235,
  `resolve_all` call site); `src/zif_abapgit_definitions.intf.abap` L106-118 (`ty_objects_tt`).
  **Exact finding — a concrete FOURTH bug, distinct from the three prior fixes, matching the live
  symptom exactly:** in `resolve_one`'s REF_DELTA branch, `lv_base_tabix = sy-tabix` is captured
  inside `LOOP AT ct_objects USING KEY sha` (a sorted **secondary** key), so it is a
  **secondary**-table-index value; it is then used **implicitly as a primary index** in
  `resolve_one( iv_tabix = lv_base_tabix )`, `READ TABLE ct_objects INDEX lv_base_tabix`, and
  transitively `MODIFY ct_objects ... INDEX iv_tabix` - all of which operate on the WRONG row
  whenever the pack's SHA1-sorted order differs from pack order (the normal case for large packs).
  When the wrong primary row is itself an unresolved delta, the inner `resolve_one` reaches
  `apply` against a wrong/short base -> "Delta copy instruction exceeds base length" via the exact
  reported `resolve_one -> resolve_one -> apply` 2-level chain. Invisible to all current unit tests
  (<=4 objects, adjacent base -> secondary index == primary index). OFS_DELTA is unaffected because
  it locates its base with `READ ... WITH KEY index =` (a FREE key -> primary `sy-tabix`), which is
  why OFS has always been correct - strong independent corroboration. Also found a related latent
  defect in the thin-fetch branch (post-`APPEND` `READ ... WITH KEY sha COMPONENTS sha1` non-unique
  first-match can bind an unresolved sibling instead of the fetched real base). **Evidence source:**
  official SAP ABAP docs `ABENSECONDARY_KEY_GUIDL` (verbatim: "the `sy-tabix` system field is
  populated by the assigned secondary index, if sorted secondary keys are used ... If used
  implicitly, the value would be interpreted as a primary index."). **Architectural conclusion:**
  all four defects (#1 non-unique first-match, #2 stale-key-after-promote, #4 secondary-vs-primary
  index, thin first-match) share ONE root cause - resolving base identity via a non-unique sorted
  secondary key on the mutable dual-purpose `sha1` field. Recommend STOP patching and REPLACE with a
  hybrid: a non-recursive **fixpoint (multi-pass) driver** over an explicit **HASHED known-identity
  side-index** (`sha1 -> primary tabix`, seeded from non-delta rows, INSERT-only as deltas resolve),
  which kills all four causes by construction and also removes the recursion + depth-guess machinery.
  `resolve_all`'s signature and its `resumable_decode` caller stay unchanged; `apply`/`get_offset`/
  `skip_size_header` unchanged; zero standard-abapGit files touched. Full analysis, strategy
  comparison (A/B/C/D), method signatures, thin+OFS interaction, and the 7 required regression tests
  (incl. the currently-untested multi-byte offset/length `apply` vectors and the SHA1-order != pack-
  order chain that reproduces #4) in
  [.memory/logs/delta_resolver_redesign.md](.memory/logs/delta_resolver_redesign.md). No productive
  ABAP modified. **Next action:** Michael to review/approve either (i) the narrow bug-#4 hotfix
  (never reuse a secondary-key `sy-tabix` as a primary index) or (ii, recommended) the hybrid
  fixpoint + side-index redesign, before `ortec-abapgit-implementation` executes. Note: once the
  diagnostics from commit `97d914e` are live, the next live retest should surface as "Delta base
  identity mismatch" or again "exceeds base length" - both consistent with bug #4.
- Michael was asked (2026-07-11) to choose the next slice after the rename-fix IT8 syntax check came back clean: "Functional validation first" vs "Complete deferred Phase 4 scope" vs "Start Phase 5". Michael chose **Complete deferred Phase 4 scope** (six-state model + D4 STRICT/RELAXED switch). See [.memory/handoffs/implementation_phase4b.md](.memory/handoffs/implementation_phase4b.md) for the full implementation record, including a genuine latent correctness bug found and fixed along the way (see below).
- Phase 4b committed (2026-07-11, commit `c8fbdf23`, work branch only, not pushed), independently regression-validated PASS_WITH_NOTES, and staged for IT8 import. Not yet imported/compiled on a real system. Same caveat as every prior phase: a clean IT8 syntax check would confirm compilation only, not functional/runtime correctness.
- Phase 5 investigation (2026-07-11, model: large-reasoning tier / orchestrator): before implementing "delta-base completeness + protocol hardening", verified the target design's Phase 5 assumption was outdated - `ZAOG_PACK_IDX.DELTA_BASE` and REF_DELTA handling already exist and work. The real, verified gap is `OBJ_OFS_DELTA` support, which is completely unimplemented in both standard and Ortec pack decoders, but is currently dead/unreachable code because abapGit's capability negotiation (`zcl_abapgit_git_transport`) never advertises `ofs-delta` or `thin-pack`. Enabling thin-pack (Michael's D3 ask) would make OFS_DELTA a live code path and risks decode failures if implemented incorrectly. Full findings and options presented to Michael: [.memory/logs/phase5_findings.md](.memory/logs/phase5_findings.md). Awaiting Michael's direction before any Phase 5 code is written.
- Phase 5a (OFS_DELTA decode-only) implemented, independently regression-validated PASS_WITH_NOTES (see [.memory/logs/regression_phase5a.md](.memory/logs/regression_phase5a.md)), and about to be committed for IT8 syntax-check import. Zero standard abapGit files touched. The feature remains dead/unreachable in production until Phase 5b (capability negotiation) is implemented as a separate future phase. Full record: [.memory/handoffs/implementation_phase5a.md](.memory/handoffs/implementation_phase5a.md).
- Michael confirmed (2026-07-11): Phase 1 + Phase 3 changes imported into real SAP system IT8 and compiled successfully. This resolves the residual "real system validation" gap noted after Phase 3 (the local ABAP-to-JS transpile harness remains blocked by unrelated dependency drift, but real-system compilation is a stronger signal anyway).
- Phase 4 committed (2026-07-11, commit `aa8229bd`, work branch only, not pushed) and staged by Michael for IT8 import. **Explicit caveat from Michael: the IT8 import will only run an ADT syntax check, not functional testing.** Do not treat a successful Phase 4 IT8 syntax check as evidence that the bulk missing-object collector, the safety gate, or the negotiated fetch behave correctly at runtime - only that the code compiles. Functional/runtime validation of Phase 4 (real negotiated fetch, real persistence under load, live safety-gate behavior) remains outstanding until Michael explicitly confirms it.
- **Incident + fix (2026-07-11):** the first IT8 import of `aa8229bd` failed because the new class was named `ZCL_ABAPGIT_ORTEC_MISSING_OBJECTS` (33 chars), which exceeds the ABAP 30-char global-object name limit; SAP silently created a truncated/mismatched object and broke compilation. Fixed by renaming the class (source file, XML metadata, and all call sites in `zcl_abapgit_ortec_obj_index` and `zcl_abapgit_ortec_git_tests.clas.testclasses.abap`) to `zcl_abapgit_ortec_missing_obj` (29 chars), verified zero remaining references to the old name, and committed as `cd2b602f` on top of `aa8229bd`. Michael re-imported `cd2b602f` into IT8 (2026-07-11) and confirmed the syntax error is gone. **The same caveat still applies**: this confirms compilation only, not functional/runtime correctness of Phase 4 behavior. General lesson captured in user-level memory: always check candidate ABAP global object names against the 30-char limit before creating/renaming.
- Post-import follow-up (2026-07-12): Michael imported Phase 5a into IT8 and ADT syntax-check passed, but default-variant ATC reported unhandled/undeclared exceptions in `zcl_abapgit_ortec_fastpath` and `zcl_abapgit_ortec_delta`. `mcp_arc-1_SAPDiagnose` (action=`atc`) could not be run because the arc-1 SAP connection was unreachable (confirmed via fresh `mcp_arc-1_SAPManage` probe), so the fix was implemented from direct source review using the existing in-repo `CATCH cx_sy_range_out_of_bounds` idiom (`src/objects/zcl_abapgit_objects_generic.clas.abap`) as the reference pattern. Committed as `b6b8372a`: added additive `try/catch` guards for risky offset/length access, and fixed a real latent pkt-line bug in `zcl_abapgit_ortec_fastpath=>parse` where invalid non-flush pkt lengths 1-3 could strip a 4-byte header out of bounds.
- Phase 5b.1 committed (2026-07-12, `1b0dccc5`): implemented the approved delta-base completeness gate slice (`target_design_phase5.md` §5, D-P5-2) by adding `has_dangling_delta_base`, `is_commit_complete`, and `get_verified_have_commits`, and exposing `is_index_ready` for reuse (visibility-only change). New tests added (`ltcl_completeness_gate`, 4 methods). This slice is intentionally additive/inert and not yet wired into capability negotiation or production transport paths; the next not-yet-started, highest-risk step remains Phase 5b capability negotiation + thin/non-thin/standard 3-try fail-safe cascade.
- Phase 5b.2 committed (2026-07-12, commit `ecb6ad2c`, work branch only, not pushed): implemented capability negotiation + fail-safe cascade + decode-failure cleanup per `target_design_phase5.md` §4/§5/§6. Thin/ofs capability advertisement is now gated by both explicit allow-thin and verified-complete haves; Ortec transport now runs a 3-tier fail-safe (thin Ortec -> non-thin Ortec -> standard); and `decode_and_persist` now performs full catch-path cleanup (temp object/index/meta/raw/session) before re-raising so catchable decode/resolve failures leave no orphan temp rows. Independently regression-validated PASS_WITH_NOTES. Not yet imported/compiled on a real system.
- **IT8 crash fix (2026-07-12, commit `46b3f398`):** after `ecb6ad2c` was pulled into IT8, `zcl_abapgit_ortec_obj_store=>has_dangling_delta_base` (Phase 5b.1) dumped with a real syntax error ("LV_CHUNK_BASE and the row type of LT_BASES are incompatible"). Root cause: a single-column `SELECT delta_base ... INTO TABLE @DATA(lt_chunk_bases)` (inline declaration) is inferred by the real SAP compiler as a one-field STRUCTURE, not an elementary table, even though the local ADT tooling in this workspace never flagged it; the elementary `ty_sha1_set` target of the subsequent `INSERT` then genuinely mismatches. Fixed by declaring the intermediate SELECT targets explicitly as `zif_abapgit_git_definitions=>ty_sha1_tt` instead of relying on inline inference; the same latent pattern was found and fixed in `zcl_abapgit_ortec_repo_state=>get_complete_commits`. Implemented by a delegated `MAI-Code-1-Flash` subagent, independently re-verified (`get_errors` clean, abaplint before/after diff via targeted `git checkout <parent>/`git checkout HEAD` on just the two files - NOT `git stash`, see the repo-memory note on the pre-existing unrelated stash - showed zero genuinely new findings, only line-shifted pre-existing ones). Michael confirmed (2026-07-12) the syntax error is gone, verified live in IT8. Michael also fixed the local MCP tooling access issue (2026-07-12) - arc-1/vsp connectivity confirmed working again via a fresh probe. **ATC prio-1/prio-2 triage explicitly deferred by Michael (2026-07-12)**: skip for now, continue with the next phase, and Michael will run another ATC check once Phase 6 is also implemented - so ATC is a single combined check covering Phase 5b.2 + the crash fix + Phase 6, not three separate ones.
- **Phase 6 committed (2026-07-12, commit `eafdc61a`, work branch only, not pushed):** large-repo index/schema optimisation per `target_design.md` Phase 6. A discovery pass (`ortec-abapgit-discovery`) found two genuine secondary-index gaps (`ZAOG_PACK_IDX` had zero secondary indexes despite REPO_KEY+OBJ_SHA1 lookups in the completeness gate; `ZAOG_OBJ_STORE`'s existing RPK index doesn't help REPO_KEY+STATUS-only queries) and one residual per-object SELECT SINGLE inside a loop (the crash-resume rehydration branch of `resumable_decode`, warm/cold path). Added both indexes (additive, byte-for-byte matching the existing RPK/RST/URL index XML structure) and replaced the per-object SELECT with one bulk `FOR ALL ENTRIES` + hashed-table lookup. Also implemented the D5 admin report: new `zcl_abapgit_ortec_cache_admin` class (read-only per-repo size/count overview + a safeguarded manual `clear_repo` reusing the existing `clear_repo_cache`, with its own enqueue lock, S_DEVELOP authorization check, and a confirmation popup) and new executable report `ZABAPGIT_ORTEC_CACHE_ADMIN` (selection-screen driven, ALV display), both off the hot path and unreachable from Stage/Diff/Patch/fetch (confirmed via where-used search in regression). Compact/ref-cleanup/stale-session cleanup were deliberately deferred per the design's own documented fallback (computing a provably delta-base-safe reachable set for compaction is a separate, higher-risk undertaking). New test `ltcl_cache_admin=>overview_aggregates_counts`. Independently regression-validated PASS_WITH_NOTES (`.memory/logs/regression_phase6.md`). Used live arc-1 syntax dry-runs (`SAPDiagnose action=syntax` with `source=`) against the real IT8 system for both new/modified classes, which caught and fixed a genuine strict-SQL clause-ordering bug (see user-level memory) before commit - local tooling alone had missed it.
- **Phase 7 complete (2026-07-12, commits `8bab40e3`, `1090dadc`, `85d44685`):** final regression + performance gate, plus the combined ATC check.
  - Regression (`ortec-abapgit-regression`, `.memory/logs/regression_phase7.md`, PASS_WITH_NOTES): checked off all 4 formal Phase 7 success criteria. Independent orchestrator spot-checking of the subagent's report found and corrected two inaccuracies: (1) the "stale-tip fallback" criterion cited test coverage that didn't actually exist - closed by adding `ltcl_repo_state=>stale_tip_invalidated_from_cache` (commit `8bab40e3`), which proves `invalidate_tip_commit` correctly clears both the commit-history "fully materialised" signal and the state row's `fetch_commit` pointer; (2) the "NOT_BUFFERED never deleted" claim overstated an active mechanism - the six-state model (`cs_object_state`) is confirmed (via a source-wide reference search) to be referenced in exactly one production location, an error-message string, not in any actual Deleted-classification logic; the invariant holds today via the older, coarser "any uncertainty triggers a full safe fallback" mechanism instead, which is fine but a materially different claim worth recording precisely for whoever eventually wires the six-state model into the deferred unified status engine.
  - Performance (`ortec-abapgit-performance`, `.memory/logs/performance_phase7.md`, static audit only, no live large-repo benchmark environment available): no fixable bottleneck found. All 6 audited areas (index usage, N+1 patterns, bulk-collection shape, thin-pack reachability, overall filtered Stage/Diff call chain, admin-report overhead) came back confirmed-fine; the only caution is that the thin-pack/OFS_DELTA win is conditional on a warm, fully-materialised repo (a correctness-safe, intentional trade-off, not a bug).
  - ATC (commits `1090dadc`, `85d44685`): ran the default check variant across all 20 `zcl_abapgit_ortec_*`/`zcx_abapgit_ortec_git` classes plus the new `ZABAPGIT_ORTEC_CACHE_ADMIN` report, now that arc-1 connectivity is confirmed working. A delegated `MAI-Code-1-Flash` subagent found and fixed one real prio-1 (`zcl_abapgit_ortec_cache_admin`'s enqueue/dequeue calls passed a 12-char repo key directly where a 32-char session-id field was expected - fixed with an explicitly-typed intermediate variable, matching the existing `zcl_abapgit_ortec_pack_dec=>acquire_repo_lock` convention; commit `1090dadc`). Independent orchestrator spot-checking (running ATC directly on `zcl_abapgit_ortec_fastpath`, which the subagent had reported as having no prio-1 issues) found **11 real, previously-missed prio-1 findings**: `pull_by_branch`, `upload_pack`, and `parse` all call standard abapGit HTTP/utility methods (`zif_abapgit_progress~show`, `zcl_abapgit_http_client=>set_headers`/`send_receive_close`, `zcl_abapgit_git_utils=>pkt_string`/`length_utf8_hex`, `zcl_abapgit_git_pack=>decode`) that declare `RAISING zcx_abapgit_exception`, without any of the three methods declaring or catching it themselves. Fixed by adding `zcx_abapgit_exception` to all three methods' RAISING clauses and catching it wherever their callers already handle `zcx_abapgit_ortec_git` (commit `85d44685`). **While fixing this, found and fixed a genuine, more serious latent bug in standard code**: `zcl_abapgit_git_transport=>upload_pack_by_commit` called the Ortec fastpath cascade with NO exception handling at all (unlike its `upload_pack_by_branch` sibling, which has `CATCH zcx_abapgit_ortec_git.`) - a full thin+non-thin cascade failure would have propagated uncaught instead of falling back to the standard fetch path, i.e. the commit-based fetch path's fail-safe fallback has been silently non-functional. Fixed by adding the same `TRY...CATCH zcx_abapgit_ortec_git zcx_abapgit_exception.` pattern already used by the branch-based sibling. All fixes verified via `get_errors` (clean) and abaplint before/after diffs (zero genuinely new findings, only line-shifted pre-existing ones, confirmed via `git stash`/`git stash pop` - stash list checked first per the established safety practice).
  - Lesson: a delegated ATC sweep across 20+ objects can still miss real findings (the subagent reported zero prio-1 issues for `zcl_abapgit_ortec_fastpath`, when there were actually 11) - spot-checking at least the highest-risk/most-recently-changed objects directly is worth the extra tool calls before trusting a "clean" summary on a final gate.

## Regression phase 1 status
- Regression validation completed for the Phase 1 implementation slice.
- Verdict: PASS_WITH_NOTES.
- Recorded evidence: [.memory/logs/regression_phase1.md](.memory/logs/regression_phase1.md).
- Notes: no hard-stop violations were found; the repo-key isolation regression test and the fallback chain remain intact.

## Regression phase 3 status
- Regression validation completed for the Phase 3 implementation slice.
- Verdict: PASS_WITH_NOTES.
- Recorded evidence: [.memory/logs/regression_phase3.md](.memory/logs/regression_phase3.md).
- Notes: the new facade and optional remote seam behave as intended, and removing the ping-pong round-trip did not introduce a new dependency on the old remote-cache priming behavior.

## Regression phase 4 status
- Regression validation completed for the Phase 4 implementation slice.
- Verdict: PASS_WITH_NOTES.
- Recorded evidence: [.memory/logs/regression_phase4.md](.memory/logs/regression_phase4.md).
- Notes: the new bulk missing-object collector uses a strict safety gate before any remote fetch, and the filtered index builder still falls back to the existing miss-handling path if the top-up fetch is unavailable.

## Regression phase 4b status
- Regression validation completed for the Phase 4b implementation slice.
- Verdict: PASS_WITH_NOTES.
- Recorded evidence: [.memory/logs/regression_phase4b.md](.memory/logs/regression_phase4b.md).
- Notes: STRICT ships as the default; the marker-based readiness check and the always-write-marker fix in `rebuild_index` were traced end-to-end; the new CORRUPT_OR_INCOMPLETE raise still routes through the existing safe fallback chain; the new `marker_required_for_ready` test was confirmed logically sound against the standard filename-logic conventions.

## Regression phase 5a status
- Regression validation completed for the Phase 5a implementation slice.
- Verdict: PASS_WITH_NOTES.
- Recorded evidence: [.memory/logs/regression_phase5a.md](.memory/logs/regression_phase5a.md).
- Notes: offset-varint math, dependency-ordered chain resolution, fail-safe error handling, and dead-code reachability were independently traced and confirmed.

## Regression phase 5b.1 status
- Regression validation completed for the Phase 5b.1 implementation slice.
- Verdict: PASS_WITH_NOTES.
- Recorded evidence: [.memory/logs/regression_phase5b1.md](.memory/logs/regression_phase5b1.md).
- Notes: the index-ready + no-dangling-delta-base completeness gate was traced end-to-end and intentionally remains inert until the separate capability-negotiation/cascade phase is wired.

## Regression phase 5b.2 status
- Regression validation completed for the Phase 5b.2 implementation slice.
- Verdict: PASS_WITH_NOTES.
- Recorded evidence: [.memory/logs/regression_phase5b2.md](.memory/logs/regression_phase5b2.md).
- Notes: verified-complete-have capability gating, thin->non-thin->standard fail-safe cascade, and catch-path decode cleanup were traced end-to-end; cleanup is limited to catchable decode/resolve failures and does not conflict with crash/timeout resume behavior.

## Regression phase 6 status
- Regression validation completed for the Phase 6 implementation slice.
- Verdict: PASS_WITH_NOTES.
- Recorded evidence: [.memory/logs/regression_phase6.md](.memory/logs/regression_phase6.md).
- Notes: the two new secondary indexes are additive and correctly targeted; the resumable_decode bulk-prefetch fix preserves the original per-object WHERE semantics and is guarded against an empty FOR ALL ENTRIES driving table; the new cache-admin class/report is confirmed unreachable from any hot path and its enqueue lock is released on every exit path.

## Regression phase 7 status
- Final, whole-system regression + performance gate completed after all implementation phases.
- Verdict: PASS_WITH_NOTES (regression) / no fixable bottleneck found (performance, static audit only).
- Recorded evidence: [.memory/logs/regression_phase7.md](.memory/logs/regression_phase7.md), [.memory/logs/performance_phase7.md](.memory/logs/performance_phase7.md).
- Notes: all 4 formal Phase 7 success criteria checked off; two report inaccuracies found via independent spot-checking and corrected in place (a missing stale-tip-fallback test, since added, and an overstated claim about the six-state model actively driving deletion decisions, which is actually still inert/decorative in production - the invariant holds via the older coarser fallback mechanism instead). No performance bottleneck found; the empirical large-repo latency benchmark remains the one item that needs a live environment to fully close out. See the "Phase 7 complete" bullet above for the full ATC follow-up, including a genuine standard-code bug found and fixed (`zcl_abapgit_git_transport=>upload_pack_by_commit` missing exception handling around the Ortec cascade call).

## Delta resolver multi-pass fixpoint fix + SYSTEM_NO_ROLL crash (2026-07-15)
- Michael hit a live `SYSTEM_NO_ROLL` crash in IT8 (kernel `CL_ABAP_GZIP=>DECOMPRESS_BINARY`
  requesting ~431MB). Call stack traced via `mcp_arc-12_SAPDiagnose` (confirmed in THIS session
  `mcp_arc-12_*` = IT8, `mcp_arc-1_*` = ES6 - the opposite of what `.vscode/mcp.json`'s naming
  would suggest; always verify via a `dumps` list call before trusting which system a given
  `mcp_arc-N_*` tool targets) showed `ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK` had fallen back to
  standard `ZCL_ABAPGIT_GIT_PACK=>DECODE` on the exact same pack bytes that had just failed Ortec
  decode - this is the isolation-violation fallback flagged (but not yet fixed) in the 2026-07-14
  discovery entry above. **Fixed**: removed the inline standard-decode fallback in
  `zcl_abapgit_ortec_fastpath=>upload_pack`; a decode failure now re-raises `zcx_abapgit_ortec_git`
  so the EXISTING thin -> non-thin -> standard-via-`zcl_abapgit_git_transport`-catch cascade
  handles escalation instead, which always re-negotiates a fresh, capability-appropriate pack
  rather than reusing bytes that just failed (standard decode cannot parse `OBJ_OFS_DELTA` at all,
  and feeding it a thin/desynced stream is what almost certainly caused the kernel to attempt an
  unbounded allocation on corrupt/misaligned input).
- Investigating what made `decode_and_persist` fail in the first place surfaced the underlying
  crash trigger, reported by Michael as a NEW exception after the fallback removal: "Delta base
  not found in pack/store (1037 missing) - retry with full/deepen fetch" raised from
  `zcl_abapgit_ortec_pack_dec=>resumable_decode`'s pre-scan. Root cause: `resolve_all` walked the
  pack in a **single ascending pass**, which cannot resolve a REF_DELTA chained onto ANOTHER
  unresolved REF_DELTA positioned LATER in the pack - REF_DELTA carries no ordering guarantee, and
  delta-on-delta chains are ordinary in real packs (this is a DIFFERENT failure mode than any of
  the four bugs fixed earlier this project: those were all about finding an ALREADY-resolvable
  base; this is about a base that is only resolvable AFTER another delta elsewhere in the pack is
  itself resolved first). `resumable_decode`'s separate pre-scan compounded this: it only counted
  already-non-delta pack objects as "present in this pack", so it flagged the same class of
  perfectly-resolvable-in-pack chains as externally missing, at huge scale (1037) for a
  complete-refresh pack. Initially attempted a "force-resolve an unresolved same-placeholder
  sibling" patch - **this was wrong and reverted**: resolving an unrelated delta only changes ITS
  OWN identity, it can never retroactively produce the specific identity being searched for, since
  a delta's `-sha1` field holds its OWN declared dependency (a placeholder) until IT is resolved,
  never a value some other future resolution could match against.
- **Actual fix**: `resolve_all` now runs a **multi-pass fixpoint** instead of one ascending pass.
  Phase 1: repeated in-pack-only sweeps (new `resolve_one` parameter `iv_allow_thin_fetch =
  abap_false` - no object-store round-trip, no raise on "not found yet", just `RETURN` and let a
  later sweep retry) resolve whatever becomes newly reconstructable each pass; a full sweep making
  zero new progress means fixpoint reached. Phase 2: one final pass with `iv_allow_thin_fetch =
  abap_true` (the pre-existing behavior) does the actual object-store fetch and raises the precise
  per-object "Delta base not found" for whatever is left - genuinely external/missing only. Bounded
  by the pack's actual max delta-chain depth (`c_max_chain_depth` <= 64), not object count - not
  O(n^2) in practice since real chains are shallow. The now-redundant/unsound missing-base pre-check
  in `resumable_decode` was removed entirely; `resolve_one` is the sole correct authority on
  whether a base is actually missing. `iv_allow_thin_fetch` is threaded through both the REF_DELTA
  and OFS_DELTA recursion paths (OFS bases are always in-pack by format guarantee, so the flag only
  matters there for a nested REF_DELTA dependency, which the shared post-resolution check now
  defers-via-RETURN instead of raising when `iv_allow_thin_fetch = abap_false`). New regression test
  `ltcl_ref_delta=>chain_onto_later_unresolved_delta` (A depends on B's real identity; B, positioned
  after A, is itself an unresolved REF_DELTA depending on a real plain base C) pins exactly this
  scenario - hand-verified byte-for-byte. All three pre-existing `ltcl_ref_delta` tests re-verified
  to still pass unchanged under the new driver (their bases are always either plain from the start
  or resolvable in the first sweep regardless of position, so multi-pass is a no-op for them).
  Committed as `afee6a17` alongside the fastpath fallback removal.
- This directly implements a NARROWER version of the "hybrid A+C" redesign already recommended (but
  marked not-yet-implemented) in the 2026-07-14 discovery entry above and
  [.memory/logs/delta_resolver_redesign.md](.memory/logs/delta_resolver_redesign.md): a non-recursive
  multi-pass/fixpoint driver, but WITHOUT the full HASHED-known-identity-side-index replacement of
  the secondary-key search (kept the existing `LOOP AT ... USING KEY sha` search, since it is
  already correct for finding ANY already-resolved candidate regardless of table position - the
  side-index optimization from `36b5ed2` already handles the O(1) tabix re-derivation piece). The
  broader side-index-for-identity-lookup replacement remains optional/deferred; the multi-pass
  fixpoint alone resolves the reported failure class.
- **Not yet re-verified live**: this fix has not yet been confirmed against a real IT8/ES6
  complete-refresh fetch. Next step once Michael retests: confirm no further "Delta base not
  found"/SYSTEM_NO_ROLL recurrence, and if it does recur, get the exact new error/call stack before
  attempting another fix (per this project's established, repeatedly-validated pattern: escalate to
  design review rather than keep incrementally patching after 2+ recurrences of the same symptom).

## Branch-switch SYSTEM_NO_ROLL crash - confirmed root cause + fix (2026-07-15, commit `11adac3a`)
- After the multi-pass fixpoint fix (`afee6a17`) resolved the initial-fetch "Delta base not found"
  crash, Michael confirmed initial retrieval now works, but reported a NEW `SYSTEM_NO_ROLL` when
  switching to another branch (an incremental/branch-switch fetch, not the initial full deepen).
- Diagnosed via `mcp_arc-12_SAPDiagnose` (IT8) dump `20260715170707T-...-16`: memory request for
  113MB failed, terminating in `zcl_abapgit_ortec_pack_dec=>resumable_decode` at the exact line
  `lv_xstring = iv_data(lv_len)` (the pack-trailer SHA1 check, right after the main decode loop).
  **Confirmed root cause** (not speculative - the crash line itself IS the bug): this line
  duplicated the ENTIRE packfile (minus the 20-byte trailer) into a NAMED local variable just to
  hash it, and that variable stays resident for the rest of the method (never explicitly freed)
  since it's reused elsewhere in the method for an unrelated 4-byte value. By this point in a
  branch-switch fetch, `iv_data` (raw pack) + `rt_objects` (every decompressed object) + the
  object-store thin-fetch cache (`zcl_abapgit_ortec_obj_store` populated while resolving REF_DELTA
  bases against previously-fetched objects) are ALL already resident - this redundant full-pack
  duplicate was the allocation that tipped it over the work process's memory budget. Initial fetch
  worked because it has no thin-fetch cache buildup (nothing previously stored to delta against).
  **Fix**: pass the slice directly as an inline actual parameter to `sha1_raw` instead of assigning
  it to a named variable first - the runtime releases a temporary right after the call instead of
  holding it until the method returns. Purely a peak-memory reduction; validates the exact same
  bytes the exact same way, so it cannot change behavior for either the initial-deepen or the
  branch-switch case (satisfies Michael's explicit "do not break the initial deepen path" ask).
  Already covered by existing `ltcl_pack_decoder=>decode_from_pack`/`decode_populates_all` tests
  (both exercise this validation on the success path via `zcl_abapgit_git_pack=>encode`-built real
  packs) - no new test added for this narrow, behavior-preserving change.
- **Not yet re-verified live.** If a similar crash recurs on an even larger branch-switch delta
  after this fix, the next candidates (in order of likely impact) are the OTHER large buffers that
  are inherently, simultaneously resident during `resumable_decode`: `rt_objects` (holds every
  decompressed object's full data for the whole pack at once, never streamed/freed incrementally)
  and `zcl_abapgit_ortec_obj_store`'s static `mt_cache` (accumulates every thin-fetched base's full
  blob for the whole decode pass, only ever cleared wholesale via `invalidate_cache` at the end of
  a SUCCESSFUL `decode_and_persist` - a crash before that point, being an uncatchable runtime dump
  rather than an ABAP exception, leaves any accumulated cache from the failed attempt resident for
  the rest of the session). These were reviewed and ruled out as the confirmed cause of THIS
  specific crash (the exact termination line was the redundant duplicate, not these), but they
  remain the accurate next-suspects list if the same symptom resurfaces at larger scale.

## Branch-switch SYSTEM_NO_ROLL - CORRECTED root cause (2026-07-15, commit `29514cb7`)
- Michael pushed back on the `11adac3a` fix ("a single copy of iv_data should not cause
  SYSTEM_NO_ROLL, even at hundreds of MB... otherwise the crash will simply reoccur at 2x repo
  size") - **correctly**. Re-examined the SAME crash dump's `kap40` (Information About Memory
  Usage), previously not pulled: **Used Memory 3,798,717,376 bytes (~3.8GB)**, Free Memory
  6,130,240 (~6MB), Largest Free Block 2,294,208 (~2.2MB), against a failed request of only
  ~108MB. A single duplicate cannot explain 3.8GB - the `11adac3a` fix was real but nowhere near
  sufficient, exactly as Michael suspected.
- Tasked `ortec-abapgit-performance` (model override: Claude Opus 4.8) for a read-only memory-flow
  audit across the whole negotiate -> decode -> resolve chain. **Confirmed dominant contributor
  (HIGH confidence, independently verified by the orchestrator directly against the code
  afterward)**: `zcl_abapgit_ortec_fetch_neg=>is_commit_complete` (the have-negotiation
  completeness gate, run once per candidate have-commit, BEFORE the pack is even requested) called
  `zcl_abapgit_ortec_obj_store=>get_reachable_objects`, which unconditionally called
  `populate_cache` - an unbounded `SELECT *` loading **every object ever stored for the repo, full
  blob data included**, into `mt_cache` (`CLASS-DATA`, i.e. static/session-lifetime). That cache is
  only ever cleared via `invalidate_cache()` at the very END of a *successful*
  `decode_and_persist` - never scoped to the verification call that built it - so it coexists with
  the subsequent pack decode's own working set (`iv_data` + `rt_objects`) for the rest of the
  request. Explains the initial-vs-branch-switch asymmetry exactly: `get_have_commits` returns
  empty on the very first fetch (nothing to verify against yet), so this path never fires; every
  fetch AFTER that pays the cost of the ENTIRE accumulated history, scaling with total repo size
  regardless of how small the actual incremental delta is - precisely Michael's "will simply
  recur at 2x" concern.
- **Fix**: added `zcl_abapgit_ortec_obj_store=>get_reachable_sha1s`, a completeness-only
  counterpart to `get_reachable_objects` - walks the identical commit->tree->blob structure and
  raises under the same conditions, but never fetches blob DATA and never preloads the full-repo
  cache: only commit/tree objects (bounded by directory structure, not file content) are fetched
  with data; blob presence is proven via a new `get_present_sha1s` helper (chunked, `SELECT
  obj_sha1` only, never `obj_data`). `is_commit_complete` now uses `get_reachable_sha1s` instead
  of `get_reachable_objects`. Also simplified `get_missing_sha1s` to use the same existence-only
  helper (it was previously loading full rows via `read_object_rows` just to discard everything
  but the SHA1). `get_reachable_objects` itself is UNTOUCHED and still used by its other caller
  (`upload_pack`'s empty-pack cached-objects fallback, which genuinely needs blob content for
  `zcl_abapgit_git_porcelain=>full_tree`) - deliberately NOT touched per the audit's explicit
  recommendation, since changing that return contract would risk a standard-abapGit-adjacent
  change for a much smaller memory win. Added a defensive `invalidate_cache()` at the start of
  `decode_and_persist` as well (independent safety net for any other path that might populate the
  cache). New tests `reachable_sha1s_graph`/`reachable_sha1s_missing_blob`; existing
  `is_commit_complete` tests (`complete_false_no_index`/`complete_true_when_ready`) exercise the
  changed path unchanged and still pass since the method's external contract didn't change.
- **Lesson for this project**: when a memory/performance crash dump is available, always pull the
  "Information About Memory Usage" section (`kap40`) FIRST, not just the termination line/source
  extract - the termination line only tells you which allocation failed, not how much memory was
  already consumed or by what; a superficial "fix the line that failed" patch (`11adac3a`) looked
  plausible and WAS a real, valid improvement, but was not remotely sufficient, and Michael's
  skepticism (rooted in "the reported failure size doesn't match a plausible root cause") caught
  it. Escalating to a large-reasoning subagent for a structured, code-evidenced trace (rather than
  the orchestrator continuing to reason ad hoc against tool-truncated dump snippets) resolved it
  efficiently once the right question was asked.
- **Not yet re-verified live.**

## "Delta base identity mismatch" -> SYSTEM_NO_ROLL - CONFIRMED root cause + fix (2026-07-16, commit `351f0808`)
- Michael reported the multi-pass fixpoint fix (`afee6a17`) still doesn't fully resolve REF_DELTA
  resolution: a NEW `SYSTEM_NO_ROLL` occurred, but call-stack evidence (`mcp_arc-12_SAPDiagnose`)
  showed it originates in `CL_ABAP_GZIP=>DECOMPRESS_BINARY` via **standard**
  `ZCL_ABAPGIT_GIT_TRANSPORT=>UPLOAD_PACK`/`ZCL_ABAPGIT_GIT_PACK=>DECODE` - i.e. the Ortec
  thin+non-thin cascade in `zcl_abapgit_ortec_fastpath` had ALREADY exhausted both attempts and
  fallen through to `zcl_abapgit_git_transport`'s own standard-decode fallback (the OUTER, standard
  3rd tier of the documented cascade), which then crashes for the same reason the INNER Ortec
  fallback used to (standard decode can't handle a thin/large pack). Michael separately reported
  (via SAP GUI debugger, not an ST22 dump - no dump exists for it since it gets caught and
  re-raised) the actual proximate failure: `zcl_abapgit_ortec_delta=>resolve_one`'s "Delta base
  identity mismatch" defensive sanity check (line ~590) still fires, meaning BOTH Ortec attempts
  fail identically on a genuine remaining correctness bug (not an ordering/timing issue this time),
  which is why the outer standard fallback gets reached and crashes.
- **Confirmed root cause** (found via code reading alone, matching an EXACT already-known bug
  pattern - no dump values needed): `zcl_abapgit_ortec_pack_dec=>resumable_decode`'s bulk
  external-delta-base prefetch merge loop (the one-time `SELECT obj_sha1, obj_type, obj_data FROM
  zaog_obj_store ... FOR ALL ENTRIES` block, run once before `resolve_all`, distinct from
  `resolve_one`'s own per-object on-demand thin-fetch) builds each merged `ty_object` via `CLEAR
  ls_object. ls_object-sha1/type/data = ...` - **never sets `-index`**, so every externally-prefetched
  base defaults to `index = 0`. `resolve_all` builds a `HASHED TABLE ... UNIQUE KEY obj_index` to
  translate a found base's stable index into its current primary tabix; with MULTIPLE prefetched
  bases all carrying `index = 0`, only the FIRST one actually registers (`INSERT` into a hashed
  table silently no-ops on a duplicate key, no exception) - every subsequent prefetched base's
  registration is silently dropped. The SHA1-based search that LOCATES a delta's real base still
  works correctly (identity lookup isn't affected), but the follow-up index-to-tabix lookup then
  lands on whichever UNRELATED prefetched base happened to register first for `obj_index=0`,
  instead of the one actually found by the search - producing exactly "Delta base identity
  mismatch: declared X, resolved Y" whenever a pack has 2+ REF_DELTA objects needing DIFFERENT
  external bases in the same decode (the ordinary case for any incremental/branch-switch fetch
  touching more than one previously-fetched file). This is the EXACT SAME bug class already found
  and fixed once for `resolve_one`'s OWN on-demand thin-fetch (`36b5ed2`,
  `two_thin_bases_do_not_collide`) - but that fix never touched THIS separate, earlier bulk-prefetch
  code path in `resumable_decode`, which had the identical missing-index defect independently.
- **Fix**: assign each bulk-prefetched object a real, unique index (`lines( rt_objects ) + 1`)
  before appending, mirroring the exact pattern already proven correct in `resolve_one`. New test
  `ltcl_pack_decoder=>prefetch_bases_do_not_collide` hand-builds a raw pack (PACK header + two
  REF_DELTA entries with real 20-byte raw base-SHA1 headers, real zlib-compressed delta payloads
  via `cl_abap_gzip=>compress_binary`, real trailing pack SHA1) against two distinct, pre-stored
  external bases and exercises `decode_and_persist` END-TO-END (not just `resolve_all` in
  isolation, since this bug is specifically in the bulk-prefetch merge that happens BEFORE
  `resolve_all` runs - a `resolve_all`-level test alone, constructing objects by hand, would
  naturally assign correct indices itself and never exercise the buggy line). Verified the test
  deterministically fails pre-fix regardless of `SELECT` row order (both prefetched bases get
  `index=0`, so BOTH deltas' lookups collide onto whichever one wins registration - at least one of
  the two always mismatches) and passes post-fix.
- Since this was a genuine, deterministic correctness bug (not a timing/ordering issue like the
  earlier multi-pass fix), fixing it should make the Ortec thin (or non-thin) attempt succeed
  outright for the ordinary branch-switch case, meaning the dangerous standard-decode outer
  fallback in `zcl_abapgit_git_transport` should no longer be reached at all for this scenario -
  no separate fix to that standard fallback should be needed. **Not yet re-verified live.**

## Structural memory fix + filtered cold-fetch (2026-07-16, commits `2d55195a`, `11bc9e84`)
- Same-day follow-up crash: old/never-accessed branch, SYSTEM_NO_ROLL, same ~3.8GB used figure.
  Traced (not guessed) via `mcp_arc-12_SAPQuery` against `zaog_obj_store`/`zaog_pack_meta`: real repo
  is ~2.1GB decompressed / 78,526 objects (not "a couple hundred MB" as assumed); the crashing pack
  was ~41,234 objects / 113MB compressed. Root cause of the crash itself: NOT a leak - `rt_objects`
  (every decompressed object of the fetched pack) held simultaneously in memory for the whole
  `resumable_decode`, plus a full second copy (`lt_final_rows`) built for the final persist.
- **Fix 1 (`2d55195a`)**: merged the "promote to resolved" and "update pack-index status" loops in
  `resumable_decode` into one pass, batching the `MODIFY zaog_obj_store` every `iv_commit_interval`
  objects instead of accumulating one full-size `lt_final_rows` copy of every new object before a
  single bulk MODIFY - removes the doubling at the highest-pressure point. Behavior-preserving
  (existing `decode_populates_all`/`decode_from_pack`/`prefetch_bases_do_not_collide` tests cover
  this path unchanged).
- **Fix 2 (`11bc9e84`, filtered cold-fetch)**: Michael challenged why a filtered stage-by-transport
  needs to decode ALL reachable objects at all. Confirmed sound with one hard constraint: git wire
  protocol has no per-path fetch; the only lever is the `filter` capability. Found
  `zcl_abapgit_ortec_obj_index=>rebuild_index` already only needs commit+tree objects to build its
  file-index (type/name derived from PATH, never content) - the actual gap was `ensure_index`/
  `rebuild_index` having NO fallback when commit+trees aren't locally available yet, so any
  never-indexed commit fell straight to a full fetch. Added
  `zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch`: negotiates `filter blob:none` (confirmed
  safe to attempt - this server already advertises `filter`, proven by the existing `fetch_tip_commits`
  using `filter tree:0`) to fetch just commit+trees, persists via existing `decode_and_persist`, then
  lets the EXISTING `get_files_for_filter` (+ its established `zcl_abapgit_ortec_missing_obj=>
  ensure_available` blob top-up) resolve the actual filtered file set. Wired into
  `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage` as an additive attempt before the
  existing full-fetch fallback - never raises, always safe to fall through.
  **Known, documented limitation**: `ensure_available`'s existing blob top-up does a FULL commit
  re-fetch when blobs are missing (not a genuinely per-blob targeted fetch - fetching an arbitrary
  blob SHA1 directly needs server-side `uploadpack.allowReachableSHA1InWant`, not universally
  supported/verified here). So this delivers real savings specifically when the filtered files'
  blobs are already known from another buffered branch (content-addressed reuse, common for
  sibling branches) - not a guarantee of avoiding a full fetch in every case. A genuinely scoped
  single-blob-want mechanism is a tracked, NOT-yet-built future enhancement (higher risk, new wire
  protocol territory, deferred pending confirmed server support).
- Tests: `ltcl_filtered_fetch` (blank-input and already-local short-circuits, no live network calls
  in unit tests per project convention). **Not yet re-verified live.**

## Have-negotiation shared-ancestry fix (2026-07-16, commit `7eac2fd5`)
- Michael asked a conceptual question: shouldn't switching to another branch be cheap, since branches
  typically share most of their object graph as common ancestry, meaning only the blobs between the
  branch root and tip should need fetching (not the whole repo)? Confirmed the assumption is correct
  in principle and that the have-negotiation machinery to exploit it already exists architecturally
  (`get_have_commits`, `collect_ancestor_haves` BFS, `upload_pack`'s deepen-omission-when-haves-exist
  logic) - but it was defeated in practice by two concrete, SQL-confirmed bugs (`mcp_arc-12_SAPQuery`
  against repo `288c81fc1cad`'s `zaog_commit_hist`/`zaog_obj_index`/`zaog_repo_state`):
  - **Bug 1**: `zcl_abapgit_ortec_repo_state=>get_complete_commits` used "commit_hist OR repo_state
    fallback" logic - if `zaog_commit_hist` had ANY row for the repo at all, `zaog_repo_state`'s
    fetch_commit entries for every OTHER branch were silently ignored, hiding legitimate have
    candidates. Fixed by unioning both sources (deduplicated via a hashed seen-set).
  - **Bug 2 (the primary one)**: `zcl_abapgit_ortec_fetch_neg=>is_commit_complete` gated ALL
    completeness on `zcl_abapgit_ortec_obj_index=>is_index_ready` - the STAGE-FILTER index, which is
    only ever built by a filtered Stage/Diff resolution, never by a plain pull/branch-switch. So a
    fully-fetched, fully-valid commit that was never filter-staged could NEVER be offered as a have,
    forcing `get_verified_have_commits` to return empty and every fetch to fall back to an
    unconditional `deepen 1` (full snapshot, ignoring haves) regardless of actual shared history.
    Fixed by removing the `is_index_ready` gate entirely; completeness is now determined solely by
    `get_reachable_sha1s` succeeding (proves every reachable object present) + no dangling delta base.
  - Both fixes are additive/loosening (more candidates now correctly eligible), not stricter, so no
    existing correctness invariant is weakened.
  - Updated the now-stale `complete_false_no_index` test (which had encoded the OLD, buggy behavior
    as "correct") to `complete_false_missing_object` (asserts incompleteness for a genuinely missing
    blob instead), and added `complete_true_without_index` (proves a fully-fetched commit without a
    built stage-filter index is now correctly eligible) and `complete_commits_union_repo_state`
    (proves the Bug 1 union, not "primary-or-fallback").
  - **Known residual limits, not addressed by this fix**: `collect_ancestor_haves` caps its BFS at 50
    levels / 200 results, so very deep/old divergence points may still not be discovered as shared
    ancestry. Whether the remote actually honors haves at all (vs. ignoring them and sending a full
    pack anyway) depends on server-side behavior which was NOT re-verified live in this session -
    Azure DevOps is the current server; GitHub is a planned future migration target and is known to
    restrict `uploadpack.allowReachableSHA1InWant` (arbitrary-blob want) more than some self-hosted/
    Azure DevOps configurations, but ordinary have/want negotiation with `have` lines for previously
    fetched commits is standard git wire-protocol behavior and not GitHub-specific, so this fix's
    approach should be safe to carry into the eventual GitHub migration. **Not yet re-verified live.**

## Full removal of the Ortec-to-standard-decode fallback (2026-07-16, commit `631ebfbc`)
- Michael observed that pretty much every hard-to-diagnose memory crash this session traced back to
  a fall-through to standard `zcl_abapgit_git_pack=>decode` after the Ortec cascade failed, and asked
  to evaluate replacing that fallback entirely with a direct, diagnostic error.
- **Evaluated first, did not implement blindly**: checked the hypothesis against all 6 major crashes
  this session diagnosed - 4 were purely inside Ortec's own code (no fallback involved: `29514cb7`
  `populate_cache`, `2d55195a`/`11adac3a` `resumable_decode`, `afee6a17`'s OWN prior internal
  fallback which was already removed on 2026-07-15), 2 went through the transport-level fallback
  (`351f0808`, and the `58a90001` deepen=0 incident) - so the hypothesis was directionally right for
  the hardest-to-diagnose ones but not literally "pretty much all". Initially recommended a
  size-gated circuit breaker (keep the fallback for small packs, refuse above an object-count
  threshold) specifically because most of this project's Ortec bugs so far were narrow correctness
  issues, not oversized-data issues, and a size gate would have preserved the fallback's safety net
  for that more-common case.
- **Michael corrected this with decisive, previously-unstated context**: standard abapGit's decode
  path (`zcl_abapgit_git_pack=>decode` + `ZCL_ABAPGIT_ZLIB_HUFFMAN` inflate) is NOT actually viable
  for these DevOps-hosted repos AT ALL, regardless of pack size - it is too slow / not memory-capable
  for this environment. So the "resilience" a size-gated fallback would have preserved is illusory
  here: the fallback never actually recovers anything for this environment, it just silently
  discards the real Ortec failure reason and then fails again anyway (OOM or timeout), several stack
  frames away from the true root cause. This directly invalidated the size-gate recommendation -
  full removal is correct, not a resilience/diagnostics trade-off.
- **Fix**: `zcl_abapgit_git_transport=>upload_pack_by_branch`/`upload_pack_by_commit` no longer fall
  through to the standard `find_branch`/`upload_pack` + `zcl_abapgit_git_pack=>decode` path when the
  Ortec fastpath cascade (thin + non-thin) fails for a repo that has `is_active_for_repo = abap_true`.
  Both catch blocks now capture the original exception and immediately
  `zcx_abapgit_exception=>raise_with_text( ix_previous = <caught exception> )`, preserving the real
  Ortec-side failure text (e.g. the combined "thin: X, non-thin: Y" detail from
  `zcl_abapgit_ortec_fastpath`) instead of discarding it. Repos that have NOT opted into the Ortec
  switch are completely unaffected - the standard code path below is unchanged and only reachable
  for them now (previously reachable for both, just usually skipped via `RETURN` for opted-in repos
  on success).
- **Side benefit confirmed by design**: `zcl_abapgit_ortec_missing_obj=>ensure_available` (the
  filtered-stage blob top-up, see the deepen-level fix above) calls
  `zcl_abapgit_git_transport=>upload_pack_by_commit` and already wraps it in
  `CATCH zcx_abapgit_exception` - it will now receive the REAL, detailed Ortec cascade failure text
  in that catch instead of a generic post-fallback message, directly improving the diagnostic value
  of every future incident in that path too, with no separate code change needed there.
- **Scope check performed before implementing**: confirmed via a source-wide search that the ONLY
  fallback sites matching this "Ortec fails -> silently decode via standard `zcl_abapgit_git_pack`"
  pattern were these exact two methods. Other `CATCH zcx_abapgit_ortec_git` sites found in the same
  search (`zcl_abapgit_git_delta=>delta`'s ref-delta base lookup, `zcl_abapgit_git_porcelain=>walk`/
  `walk_tree`'s tree/blob store lookups) are a DIFFERENT, narrower pattern - a single-object lookup
  falling back to a clean, already-informative `zcx_abapgit_exception=>raise('Walk, tree not found')`
  etc., not a whole-pack standard-decode attempt - and were deliberately left untouched (this is
  exactly the still-deferred H4 walk-delegation territory, confirmed unrelated to today's incidents,
  not part of this fix). `src/git/v2/zcl_abapgit_gitv2_porcelain.clas.abap`'s own
  `zcl_abapgit_git_pack=>decode` call was also checked and confirmed to have zero Ortec gating at
  all (a fully separate, non-Ortec git-protocol-v2 code path) - correctly out of scope.
- **Lesson for this project**: my own first-pass recommendation (size-gated fallback) was
  reasonable given the evidence I had, but was wrong for this specific environment because I was
  missing a key operational fact (standard decode's zlib inflate is categorically too slow/incapable
  for DevOps-hosted packs here, not just "usually fine below a size threshold"). When a design
  trade-off hinges on "is the fallback actually useful in practice", ask rather than assume from
  first principles alone - Michael's correction here was decisive and changed the recommendation
  from a conservative middle-ground to full removal being the objectively correct choice.
- **Not yet re-verified live.**

## zcx_abapgit_ortec_git silently losing its own message text (2026-07-16, commit `d29af6b4`)
- Immediate fallout from the fallback-removal fix above: Michael's very next reproduction hit the
  new, intended immediate raise (confirmed the removal itself works - reached via
  `ZCL_ABAPGIT_REPO_ONLINE=>FETCH_REMOTE`/`GET_FILES_REMOTE` -> `pull_by_branch` -> a plain,
  non-filtered Stage page load, a different call path than the `ensure_available` one fixed
  earlier), but the message was **useless**: "Ortec fastpath failed after thin+non-thin retry -
  thin: , non-thin: " - both halves blank.
- **Root cause, confirmed by reading the class**: `zcx_abapgit_ortec_git` stores its message in
  `mv_text` but never populates the T100 message infrastructure (`if_t100_message~t100key` is never
  set in its constructor) and never overrides `get_text( )`. Every `->get_text( )` call on an
  instance of this class - anywhere in the codebase, not just this one call site - therefore fell
  through to the inherited `cx_root` default, which has no knowledge of `mv_text` and returns
  generic/empty text instead. This is a **systemic, previously-invisible bug**: it was undetectable
  in places where a raised exception's default message is shown directly (SAP's error popup already
  has other ways to surface `mv_text`-adjacent info, or nobody looked closely), but it silently
  blanked out EVERY diagnostic message anywhere in the codebase that tries to forward/combine
  another `zcx_abapgit_ortec_git` instance's text - which is exactly the pattern the fallback-removal
  fix above newly relies on to be useful (`ensure_available`'s catches, the cascade-combining raises
  in `zcl_abapgit_ortec_fastpath`, etc.).
- **Fix**: added `METHODS get_text REDEFINITION` returning `mv_text` (falling back to the inherited
  default only if `mv_text` happens to be blank). New regression test
  `ltcl_ortec_git_exception=>get_text_returns_mv_text` proves `get_text( )` returns the exact text
  passed to `raise( iv_text )` instead of a generic/blank default.
- **Important implication for diagnosing the ORIGINAL crash that triggered this**: the underlying
  reason the Ortec thin+non-thin cascade failed in THIS specific incident is still unknown - the
  blank message hid it. With `get_text( )` now fixed, the next reproduction of the same scenario
  will surface the real detail (e.g. the actual HTTP/negotiation/decode error from the thin and
  non-thin attempts) instead of an empty string, which is needed before this specific incident's
  true root cause can be diagnosed and fixed. **Awaiting Michael's next reproduction with this fix
  in place.**
- **Not yet re-verified live.**

## Root cause found: well-formed zero-object pack mishandled (2026-07-16, commit `8245dee2`)
- With `get_text( )` fixed, Michael's next reproduction of the SAME "Ortec fastpath failed after
  thin+non-thin retry" exception (same call path: `ZCL_ABAPGIT_REPO_ONLINE=>FETCH_REMOTE` -> plain
  Stage page load, not the `ensure_available` path) finally showed the real detail on both halves:
  "thin: Ortec decode not applicable for this repo, non-thin: Ortec decode not applicable for this
  repo" - traced to the final, generic fallback raise at the end of
  `zcl_abapgit_ortec_fastpath=>upload_pack`.
- **Root cause**: `upload_pack` only recognized "server has nothing new to send" when the response
  had NO pack section at all (`lv_pack IS INITIAL`), routing that case through existing logic that
  serves already-known-complete objects from the local store. A **well-formed pack** (real
  `PACK`+version header and a valid trailer) that legitimately **declares zero objects** carries the
  exact same meaning - the server confirming, via successful have-negotiation, that the client
  already has everything reachable from the wanted commit(s) - but this shape fell through
  `decode_and_persist` (which correctly decodes zero objects without raising anything, so
  `rt_objects` stays empty) straight to the generic "Ortec decode not applicable for this repo"
  raise, on both the thin AND non-thin attempts identically (explaining why both cascade attempts
  failed with the same message).
- **Why this only started firing today**: have-negotiation almost never had real haves to offer
  before today's `get_complete_commits`/`is_commit_complete` fix (Bug 1 + Bug 2 above), so a real git
  server rarely had reason to reply "you already have everything" at all. Once haves started working
  correctly, this became a live, repeatable failure for any branch/commit that's already fully
  up to date - a direct, previously-latent side effect surfaced by that earlier fix.
- **Fix**: added `zcl_abapgit_ortec_pack_dec=>peek_object_count` - a cheap, decompression-free peek
  at the pack header's declared object count (bytes 8-11, big-endian, mirroring the exact parsing
  `decode_and_persist` already does internally), additionally validating the `PACK` magic before
  trusting the byte layout (returns -1, never 0, for anything too short or missing the magic, so
  callers can distinguish "confirmed empty" from "can't tell yet"). `upload_pack` now checks
  `lv_pack IS INITIAL OR peek_object_count( lv_pack ) = 0` as one combined condition and routes BOTH
  cases through the same logic. Extracted the existing ~60-line "serve from cache" block (previously
  only reachable from the `lv_pack IS INITIAL` branch) into a new shared private method
  `serve_cached_when_nothing_new`, used by both branches - avoided duplicating that logic, and
  cleaned up now-unused local declarations (`lv_repo_key`/`lt_cached`/`lt_reachable`/
  `lt_cached_shas`/`<ls_reachable>`) that only existed for that block. Added
  `ltcl_pack_decoder=>peek_object_count_cases` covering short-data, well-formed-zero-object,
  well-formed-non-zero, and missing-magic inputs.
- **Deliberately NOT changed in this pass** (noted for later, not fixed now to avoid scope creep):
  `serve_cached_when_nothing_new`'s inner `TRY...CATCH zcx_abapgit_ortec_git.` (empty catch body)
  still swallows the more specific "Cached multi-want..."/"Cached objects have incomplete tree..."
  raise texts and always surfaces the generic "Cached objects not available..." message instead -
  the same class of diagnostic-loss pattern as the `get_text( )` bug, just at a different layer
  (message-swallowing via an intentionally empty catch, not a broken accessor). Worth revisiting if
  a future incident needs that specific detail.
- **Not yet re-verified live** - this is now believed to be the actual, complete root cause of the
  live incident chain that started with the H4 evaluation crash, but awaits Michael's confirmation
  that opening the Stage page for an up-to-date branch/repo now succeeds instead of raising.

## Preserve raise call site + fix remaining diagnostic-swallowing (2026-07-16, commit `63e80030`)
- Two follow-up requests once the zero-object-pack fix above was in place:
  1. Fix the deliberately-deferred `serve_cached_when_nothing_new` message-swallowing noted above.
  2. Solve a debugging pain point: by the time Michael catches a `zcx_abapgit_ortec_git` in the
     debugger, the call stack that led to `RAISE EXCEPTION` has already unwound, and `RAISE EXCEPTION
     TYPE zcx_abapgit_ortec_git` always executes inside the class's own static `raise( )` method -
     so the only "source position" previously inspectable pointed at `raise( )`'s own line, never the
     actual calling code that decided to raise.
- **Fix 1**: `serve_cached_when_nothing_new`'s inner `CATCH zcx_abapgit_ortec_git.` now captures the
  exception (`INTO DATA(lx_cache_reason)`) instead of discarding it; the final raise appends its text
  when one was caught, instead of always emitting the same generic "Cached objects not available"
  message regardless of whether the cause was a multi-want limitation or an incomplete cached tree.
- **Fix 2**: replicated `zcx_abapgit_exception=>save_callstack`'s exact, already-proven pattern onto
  `zcx_abapgit_ortec_git` - added `mt_callstack` (filtered `SYSTEM_CALLSTACK`, with this class's own
  `CONSTRUCTOR`/`SAVE_CALLSTACK`/`RAISE*` frames removed via the same dual `mainprogram CP` +
  `blockname =` check the original uses, which is required for correctness in BOTH the normal
  per-class-pool build AND this project's merged single-file build where `mainprogram` is shared
  across all classes) and `ms_src_info` (reused `zcx_abapgit_exception=>ty_scr_info` as the type
  rather than duplicating the structure), captured once in the constructor. Also added a
  `get_source_position` override reading `mt_callstack[1]`, falling back to the inherited default if
  the callstack ended up empty. Both `mt_callstack` and `ms_src_info` remain inspectable directly on
  the caught exception instance in the debugger long after the original stack has unwound.
- New tests: `ltcl_ortec_git_exception=>source_position_points_to_caller` asserts the filtered
  callstack's top frame is the TEST METHOD itself (not `RAISE`/`CONSTRUCTOR`), and that
  `get_source_position( )` stays consistent with `ms_src_info`. The message-swallowing fix wasn't
  independently unit-tested (`serve_cached_when_nothing_new` is private and only reachable via a live
  HTTP fetch, consistent with this project's no-live-network-in-unit-tests convention) - covered by
  direct code review + `get_errors` only.
- **Not yet re-verified live.**

## Haves-free retry for a wrong "nothing new" claim (2026-07-16, commit `a51e743b`)
- Michael reported the SAME "incomplete tree"/"nothing new" failure recurring for MULTIPLE
  objects, across BOTH the plain pull path (`pull_by_branch` -> `upload_pack_by_branch`) AND the
  filtered-stage top-up path (`ensure_available` -> `upload_pack_by_commit`) in the same run -
  confirming this is a systemic gap, not a one-off.
- **Live evidence gathered via `mcp_arc-12_SAPQuery`** before designing a fix: the specific missing
  blob (`4cc98117d09409d2fb9972b59d57aaf81e1009a8`) does not exist anywhere in `zaog_obj_store` for
  ANY repo, is not referenced in `zaog_pack_idx`, and is not referenced in `zaog_obj_index` for any
  commit - genuinely never fetched, not a query/repo-key mismatch. `zaog_repo_state` showed repo
  `288c81fc1cad` has two tracked branches: `bugfix/O4H-8743-...` (`fetch_commit` blank) and
  `development/6.0.x` (fully verified-complete).
  **Correction from Michael (same day, important):** the branch actually being opened when the
  exceptions fired was a **THIRD branch, never buffered/tracked at all** - it has no row in
  `zaog_repo_state` precisely BECAUSE every attempt to fetch it has hit this exception before ever
  reaching the point where `update_after_fetch` would persist a row for it. The missing blob(s)
  belong to THIS never-before-seen branch, not to `bugfix/O4H-8743-...` (whose own blank
  `fetch_commit` is a separate, coincidental artifact of an earlier, unrelated self-invalidation).
  Corrected understanding: WANT = the brand-new third branch's tip (zero local presence at all) ->
  have-negotiation offers `development/6.0.x`'s verified-complete tip (and/or others) as a "have" ->
  the server (Azure DevOps) replies "nothing new" for that want based on that have - incorrectly,
  since the new branch is genuinely first-time-fetched and evidently shares only partial history/
  files with whatever was offered as a have. This makes the haves-free retry fix below even more
  clearly the right shape of solution: a first-time fetch of an unrelated branch has no business
  being satisfied by a have from a completely different branch, and forcing `deepen 1` with zero
  haves is exactly the correct semantics for "fetch this one never-seen commit's full snapshot,
  unconditionally". The exact server-side reasoning for the wrong "nothing new" wasn't further
  chased (would need wire-level tracing) since the fix is engineered to be correct regardless of the
  precise cause.
- **The core problem**: `serve_cached_when_nothing_new` (added earlier today) correctly detects this
  mismatch and raises cleanly instead of serving broken data - but after today's earlier fallback-
  removal fix, there was no way left to actually recover: the thin and non-thin Ortec attempts send
  the SAME have-set and hit the SAME "nothing new" response, so the whole cascade just fails with no
  successful outcome at all, for any branch caught in this situation.
- **Fix**: `zcx_abapgit_ortec_git` gained `mv_retry_without_haves` (settable via a new
  `raise( iv_retry_without_haves = abap_true )` parameter), set on EVERY failure path inside
  `serve_cached_when_nothing_new` (multi-want, incomplete tree, blank repo key, and the final
  generic case) - regardless of the specific reason, the local cache failing to stand in for a
  "nothing new" response always means the same thing: a haves-free retry is worth attempting.
  `upload_pack_by_branch`/`upload_pack_by_commit` now make ONE additional attempt (fresh client,
  `iv_allow_thin = abap_false`, new `iv_force_full = abap_true`) when either the thin or non-thin
  attempt's caught exception has this flag set, before giving up with the combined 2-tier error.
  New private helper `is_retry_without_haves` centralizes the `INSTANCE OF`+flag check used at both
  cascade catch sites. `upload_pack` gained `iv_force_full`, which skips have-resolution entirely
  (leaving `lt_ortec_haves` empty) so the request is guaranteed to ask for a real, complete pack.
- **Related latent gap closed in the same pass**: `upload_pack`'s own deepen logic previously only
  sent `deepen` when `lt_ortec_haves IS INITIAL AND iv_deepen_level > 0` - meaning an empty have-set
  combined with a zero deepen level (the DEFAULT for `upload_pack_by_commit`) sent NEITHER a deepen
  line NOR any have lines, the exact same "send the complete history from the beginning of the repo"
  danger already fixed today for `zcl_abapgit_ortec_missing_obj=>ensure_available`'s specific call
  site. Now closed for EVERY caller of `upload_pack`: whenever there are no haves at all, at least
  `deepen 1` is always sent (never both omitted simultaneously).
- New tests: `ltcl_ortec_git_exception=>retry_without_haves_flag_set` (default false, settable true,
  correctly readable off the caught exception). The retry cascade itself and the deepen-safety
  change are not independently unit-tested (`upload_pack`/`upload_pack_by_branch`/
  `upload_pack_by_commit` all require a live HTTP client, consistent with this project's no-live-
  network-in-unit-tests convention).
- **Not yet re-verified live.**

## H4 evaluated against a live SYSTEM_NO_ROLL - NOT applicable; real cause found + fixed (2026-07-16, commit `58a90001`)
- Michael reported a NEW SYSTEM_NO_ROLL and explicitly asked whether implementing the deferred H4
  walk-delegation topic would help. Pulled the live dump (`mcp_arc-12_SAPDiagnose`,
  `20260716114827T-...`, `CL_ABAP_GZIP=>DECOMPRESS_BINARY` requesting 1,568,916,060 bytes) and its
  full call stack (`kap11`) rather than guessing from the topic description.
- **Verdict: H4 would NOT have helped.** H4 is scoped entirely to `zcl_abapgit_git_porcelain`'s
  `walk`/`walk_tree` (missing-tree/blob recovery during tree traversal of an already-fetched
  commit) - neither method appears anywhere in this crash's call stack. The actual chain was:
  Stage page open -> `zcl_abapgit_ortec_git_facade=>resolve_filtered_remote` ->
  `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage` ->
  `zcl_abapgit_ortec_obj_index=>get_files_for_filter`/`build_files_from_rows` ->
  `zcl_abapgit_ortec_missing_obj=>ensure_available` (the ALREADY-EXISTING bulk-collect-then-fetch
  mechanism - functionally what H4 would have built, just for a different call site) ->
  `zcl_abapgit_git_transport=>upload_pack_by_commit` -> Ortec thin+non-thin cascade failed and was
  correctly caught -> fell through to the standard, non-incremental `zcl_abapgit_git_pack=>decode`
  (the documented, intentional final fallback tier) -> crashed trying to decompress the full
  response pack in one shot.
- **Real root cause, confirmed by reading `upload_pack_by_commit`'s signature and `upload_pack`'s
  deepen/have logic**: `ensure_available` called `zcl_abapgit_git_transport=>upload_pack_by_commit`
  without an explicit `iv_deepen_level`, defaulting to **0** (unlike `upload_pack_by_branch`'s
  default of 1). `zcl_abapgit_ortec_fastpath=>upload_pack`'s own comment states the exact mechanism
  precisely: "Only send deepen when we have NO cached objects... With deepen, the server ignores
  have lines and sends a full shallow pack. Without deepen (but with haves), the server sends only
  the delta." With deepen=0 AND an empty have-set (near-guaranteed for any repo/commit hitting this
  code path before today's separate have-negotiation fix, and still possible today for any
  never-before-synced repo), the guard `IF lt_ortec_haves IS INITIAL AND iv_deepen_level > 0` is
  FALSE, so **neither** a deepen line **nor** any have lines are sent - standard git wire-protocol
  shorthand for "send the complete reachable history from the beginning of the repo", not "this
  commit's own objects". `ensure_available` only ever needs the ONE target commit's own reachable
  graph to resolve its specific missing blobs, so this was requesting vastly more than needed.
- **Fix (commit `58a90001`)**: `ensure_available` now explicitly passes `iv_deepen_level = 1` to
  `upload_pack_by_commit`, mirroring the same deepen-1 convention already established by
  `zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch`. This bounds the absolute worst case
  (empty haves, e.g. first-ever access) to one commit's full tree/blob set instead of the entire
  repo history. Verified the two OTHER standard (non-Ortec) callers of `upload_pack_by_commit`
  (`zcl_abapgit_git_commit=>get_by_commit`, `zcl_abapgit_git_porcelain=>pull_by_commit`) both
  already pass their OWN caller-controlled `iv_deepen_level` through unchanged, so this fix is
  scoped purely to the Ortec `ensure_available` call site and does not touch standard behavior.
  When verified-complete haves ARE available (the common case once a repo has prior history, and
  more often true now after today's separate have-negotiation fix), `upload_pack` already prefers
  them over deepen and sends only the actual delta - so this is a safety bound for the empty-haves
  case specifically, not a behavior change for the already-working incremental case.
- **H4 backlog status unchanged by this**: still deferred, entry condition unmet, and now
  additionally confirmed NOT relevant to this specific incident class - it remains a `walk`/
  `walk_tree`-specific gap for genuinely-never-cached objects reached via tree traversal, distinct
  from the OBJ_INDEX/filtered-stage top-up path this incident was in. **Not yet re-verified live.**

## Non-negotiable invariants
- Missing local cache data is not remote deletion.
- `confirmed_absent` requires a positively resolved remote branch tip, commit, tree path, and parent tree.
- Ortec logic belongs in `zcl_abapgit_ortec_*`; standard abapGit changes remain small hooks only.
- Correctness outranks performance; performance outranks maintainability.

## ACTIVE INCIDENT (2026-07-17): Phase 4 live crash still SYSTEM_NO_ROLL, root cause not yet found
- Michael hit a live `SYSTEM_NO_ROLL` crash after Phase 4 was deployed and verified via unit tests.
  ST22 dump (`20260717162926T-...-15`) call stack: `UPLOAD_PACK -> DECODE_AND_PERSIST (OLD decoder)
  -> RESUMABLE_DECODE` (crash at line 463, requesting 392MB). This PROVES `zcl_abapgit_ortec_pack_stream=>
  decode_streaming` DID run and DID raise `zcx_abapgit_ortec_git` (caught by `upload_pack`'s new Phase 4
  routing), which then fell back to the OLD, memory-unsafe decoder on the SAME huge pack - which crashed
  exactly as it always did. The real streaming failure reason was never captured anywhere (no persistent
  logging existed for it) - this is the CURRENT top-priority blocker.
- Repo in question: `repo_key = '288c81fc1cad'` (resolve the URL via `zcl_abapgit_ortec_repo_state` if
  needed). `zaog_obj_store` state at investigation time: 439,884 rows status `'P'` (old decoder's own
  crash-resume leftover, LARGE - this repo has clearly crash-looped via the old decoder many times,
  before AND independently of this session's work) + 181,925 rows status `'R'`, ALL with real 40-char-hex
  `obj_sha1` values (verified via `NOT LIKE '<40 underscores>'` returning 0 rows) - i.e. **zero temp-key-
  shaped rows found**, so no direct evidence either way of exactly how far the Phase 2/3 streaming attempt
  got before failing (could be old-decoder history, could be a fully-successful Phase 2 decode with Phase 3
  resolve failing, cannot distinguish from current DB state alone).
- Repo is clearly VERY large (400k+ objects based on the P+R row counts) - a real stress case for the
  multi-pass resolver and for simple per-object DB roundtrip cost during decode; a first hypothesis to test
  once the real error text is known: a genuinely missing/thin delta base at this scale (`resolve_streaming`
  raising "Delta base not found"), OR conceivably some cost/performance issue during hundreds-of-thousands
  of individual `store_object` MODIFY statements (though that would need to manifest as a TIME_OUT/dump,
  not a clean `zcx_abapgit_ortec_git`, to be consistent with the observed fallback trigger - so a genuine
  correctness failure is more likely than a raw performance ceiling, but not yet confirmed).
- **Diagnostic action taken (commit `8e03a191`, TEMPORARY, marked with a TODO for reversal)**: in
  `zcl_abapgit_ortec_fastpath=>upload_pack` ONLY (the exact call site in the crash's own stack trace), the
  `CATCH zcx_abapgit_ortec_git` handler that used to fall back to `decode_and_persist` (old decoder) now
  RE-RAISES the streaming failure directly instead. Rationale: the fallback tier crashes anyway for this
  repo/pack, so this does not make anything worse for Michael - it trades an uninformative hard dump for a
  clean, catchable error message (visible in the abapGit UI) that will actually say WHY streaming failed.
  `try_filtered_commit_fetch`'s own separate fallback was left untouched (not implicated in this crash).
- **Next action**: ask Michael to import commit `8e03a191` and reproduce the SAME pull/fetch on repo
  `288c81fc1cad`. Read the resulting clean error message text (from the abapGit UI, or a fresh, now
  MEANINGFUL dump/exception if one still occurs) to identify the exact `resolve_streaming`/
  `decode_and_persist_streaming` failure point, then implement a real, permanent fix (not just re-enabling
  the fallback) before closing Phase 5. Once the real cause is fixed and reverified, revert the diagnostic
  change (restore the `decode_and_persist` fallback in `upload_pack`) so production behavior still has a
  safety net for genuinely unrelated future failures.
- **Real error captured (2026-07-17)**: "Ortec fastpath failed after thin+non-thin retry - thin: Delta copy
  instruction exceeds base length (offset 0, length 8826, base length 5835) - base type tree, 5835 bytes,
  delta 153 bytes, depth 1". Call stack confirms this came through `pull_by_branch` (H4 mirror) ->
  `upload_pack_by_branch` -> ... -> `resolve_one_meta`'s `apply()` call, on a BRAND-NEW branch never
  buffered before (full decode of a very large pack for this repo). depth=1 (no chain) rules out the
  multi-level-chain scope limitation documented above; this is a single-level resolution picking/producing
  a base that's too short for its delta's own copy instruction.
- **Two real gaps found and fixed (commit `c62b0b70`)**, though NOT yet confirmed as THE root cause (could
  not reproduce locally - only manifests on this specific huge, previously-unbuffered repo/branch):
  1. `resolve_one_meta`'s OFS_DELTA branch was missing the negative-`base_offset` sanity check that the
     original `zcl_abapgit_ortec_delta=>resolve_one` has - restored, with obj_index/pack_offset context.
  2. Unlike REF_DELTA (which cross-checks the resolved base's real sha1 against the delta's declared base
     sha1 and raises immediately on mismatch), OFS_DELTA had NO equivalent safety net - a wrong
     offset->object mapping would silently reach `apply()` with a wrong base, surfacing only as this
     generic "copy exceeds base length" error. There's no content-identity to cross-check for OFS_DELTA
     (unlike REF_DELTA's declared sha1), so instead the `apply()` failure diagnostic now reports BOTH
     sides' full identifying info (delta's obj_index/pack_offset/declared_base_sha1/declared_base_offset,
     AND the resolved base's obj_type/sha1/pack_offset) - enough to tell a wrong-base-picked bug apart from
     a genuinely corrupt/unexpected delta stream on sight next time, without another live round-trip.
  - **Also noted, NOT yet fixed (separate, lower-priority finding)**: `zcl_abapgit_ortec_base_cache`'s
    `find_entry` is an O(n) LINEAR SCAN over every cached entry, called on every `get`/`put`/`touch`. At
    this repo's scale (400k+ objects, many distinct tree/blob bases), this is O(n^2) total cost across a
    full resolve pass - a real latent performance problem (Phase 1's unit tests only ever exercised a
    handful of entries, never enough to stress this). Not implicated in THIS specific wrong-base symptom
    (a slow linear scan returns SLOW-BUT-CORRECT results, not wrong ones), but should be replaced with a
    HASHED TABLE keyed by sha1 before this cache is trusted at production repo scale.
- **Next action**: ask Michael to import commit `c62b0b70` and reproduce the SAME pull on the SAME
  previously-failing branch. The new diagnostic text (both sides' obj_index/pack_offset/sha1) should reveal
  whether this is a genuine wrong-base-picked bug (if the declared vs resolved identifiers don't line up in
  an expected way) or something else entirely (e.g. a bug in `apply()`'s own delta-instruction parsing, or
  a genuinely corrupt delta persisted by Phase 2). Do not close Phase 5 until this is root-caused and fixed
  for real - the current `upload_pack` diagnostic re-raise (commit `8e03a191`) must eventually be reverted
  back to a real fallback once fixed.
- **ROOT CAUSE FOUND AND FIXED (commit `15f9e579`, 2026-07-19)**: the rich diagnostic text got silently
  truncated by an undocumented length limit in abapGit's own error display (Michael could not retrieve any
  more of it), so the real root cause was found by directly querying `zaog_obj_store` for the actual
  temp-key rows written by the in-progress decode of the failing branch, bypassing the UI entirely. Ordering
  those rows by insertion time revealed suffixes like `16470000`, `16480000`, `16490000`, ... instead of the
  intended `00001647`, `00001648`, .... **`|{ lv_uindex WIDTH = 8 PAD = '0' }|` does NOT right-align with
  leading zeros - it pads on the RIGHT with trailing zeros**, so index values 1, 10, 100, 1000, and 10000 all
  produced the IDENTICAL 8-character string `"10000000"`. Since this was the suffix of a supposedly-unique
  temp storage key (`pack_id` + index), any two delta objects whose index shared a common leading digit
  (near-certain at real repo scale) silently collided on the exact same `zaog_obj_store` primary key - the
  later object's write overwrote the earlier one's delta bytes, so resolving the earlier object later read
  back completely unrelated delta content, producing exactly the observed "copy instruction exceeds base
  length" symptom. Fixed in BOTH places this exact pattern existed: `zcl_abapgit_ortec_pack_stream=>
  decode_and_persist_streaming` (HIGH severity - the temp-keyed row is the SOLE authoritative storage for
  the delta's bytes, so a collision directly corrupts the real resolve process) and
  `zcl_abapgit_ortec_pack_dec=>resumable_decode` (the OLD decoder, which this pattern was originally copied
  from - LOWER severity there since it's only used for status='P' crash-resume checkpointing, not the
  authoritative in-memory `rt_objects`, but the same real defect, fixed for consistency).
  **Correction (2026-07-19, Michael's review)**: the fix's FIRST version used `UNPACK`, which is the WRONG
  tool (targets BCD/packed-decimal source fields, not a generic numeric-to-char pad) - even though it
  happened to work empirically for a plain integer. Corrected to the real fix: explicit
  `ALIGN = RIGHT` in the string template (`|{ lv_uindex WIDTH = 8 ALIGN = RIGHT PAD = '0' }|`), commit
  `147e8499`. Full lesson recorded in `/memories/abap-mcp-notes.md`.
- **Second real issue found by Michael via a live SAT trace (2026-07-19), fixed in the same commit
  `147e8499`**: 82%+ of total runtime on a ~100k-object pack was pure DB connection open/close overhead
  from individual `MODIFY`/DELETE statements - one row at a time, both in decode (one `store_object` per
  object) and resolve (one `store_object` + one `DELETE` per resolved delta). Fixed by batching: `zcl_abapgit_
  ortec_obj_store=>store_objects` gained an `iv_status` param (mirroring `store_object`); `decode_and_
  persist_streaming` now accumulates decoded objects into an in-memory batch (new `c_batch_size = 500`
  constant) and flushes via one bulk `store_objects()` call per batch; `resolve_one_meta`/`resolve_streaming`
  batch resolved objects + superseded temp keys into `ct_write_batch`/`ct_delete_batch`, flushed via bulk
  `store_objects()` + a single `DELETE ... WHERE obj_sha1 IN @itab` periodically and once more at the end.
  Since a just-resolved delta's bytes are no longer immediately in the DB (only in the pending batch), a
  CHAINED delta needing that object as its OWN base could no longer find it via `get_object` - fixed by
  warming the Phase 1 LRU base cache with every resolved object's bytes immediately upon resolution
  (`get_base_bytes` already checks the cache before the DB). This increased cache churn also exposed
  `zcl_abapgit_ortec_base_cache`'s `find_entry` as an O(n) linear scan - fixed to O(1) via a secondary
  UNIQUE HASHED KEY on sha1 (SY-TABIX is reliably set to the primary table index even via a secondary-key
  read, so the existing INDEX-based LRU eviction logic needed no changes). Full lessons (batching, LRU
  secondary-key indexing) recorded in `/memories/abap-mcp-notes.md`.
- **Third real issue: live ITAB_DUPLICATE_KEY dump (2026-07-19) after importing `147e8499`**, in
  `zcl_abapgit_ortec_base_cache=>put`'s `APPEND ls_entry TO mt_entries` (ST22 call stack: `PUT` <-
  `ZCL_ABAPGIT_ORTEC_PACK_STREAM=>GET_BASE_BYTES` <- `RESOLVE_ONE_META`). Root cause: `get_base_bytes`
  decided "already cached?" via `get( ) IS NOT INITIAL`, which is WRONG for a genuinely cached 0-byte
  object (a real, valid Git object, e.g. an empty blob) - every request for such an object was treated as
  a permanent cache miss, causing a redundant DB fetch + redundant `put( )` call EVERY time it was
  requested (common, since empty blobs/trees are frequently shared bases across many deltas). Fixed in
  commit `9c3d297a`: (1) added `zcl_abapgit_ortec_base_cache=>has( iv_sha1 ) RETURNING abap_bool` (pure
  existence check via `find_entry( ) > 0`, independent of the cached value's length); `get_base_bytes` now
  uses `has( )` instead of the broken `IS NOT INITIAL` check. (2) Defense in depth regardless of root
  cause: `put( )`/`touch( )` now use `INSERT ls_entry INTO TABLE mt_entries` instead of `APPEND ... TO
  mt_entries` - for a table with a UNIQUE secondary key, `APPEND` raises the uncatchable
  `ITAB_DUPLICATE_KEY` runtime error on a duplicate, whereas `INSERT INTO TABLE` degrades gracefully to
  `sy-subrc <> 0`; for a STANDARD table with an EMPTY primary key this still inserts at the end (identical
  position to APPEND), so LRU ordering is unaffected. Added two regression tests to `ltcl_base_cache`
  (`zero_byte_blob_is_a_hit`, `re_put_same_sha1_no_dump`). Full lessons recorded in
  `/memories/abap-mcp-notes.md`.
- **This fix was NOT sufficient - the crash recurred (commit `19bdbe40`, 2026-07-19)**. Confirmed directly
  via live IT8 ST22 dumps (`SAPDiagnose(action="dumps")`, connection to system IT8) rather than waiting for
  Michael to paste them: four dumps between 09:45-10:11 that day, all `PUT` <- `GET_BASE_BYTES` <-
  `RESOLVE_ONE_META`, all showing `SY-TABIX=0` for the crashing SHA1 (i.e. `find_entry( )` itself reported
  "not found" in the SAME statement sequence where the INSERT was rejected as a duplicate of that exact
  SHA1) - and critically, the crashing SHA1 (`c262c5c9...`) was a REAL, non-empty tree object (visible
  tree-entry bytes in the dump), not the 0-byte edge case the `has( )` fix targeted. Root cause: `put( )`/
  `touch( )` implemented LRU "move to newest" via DELETE + re-APPEND/INSERT of the SAME row - repeating
  this delete+reinsert cycle for the SAME hot/shared base SHA1s many times over a ~100k-object pack is
  what actually triggers ITAB_DUPLICATE_KEY against a UNIQUE secondary key, regardless of any
  find-then-delete guard immediately beforehand. **Real fix (commit `19bdbe40`)**: stop deleting/
  re-inserting rows for existing keys, at all. `ty_entry` gained a monotonic `seq` field with its own
  UNIQUE SORTED secondary key `by_seq`; `put( )`/`touch( )` now `MODIFY mt_entries INDEX idx FROM ls_entry`
  in place (row is never removed and re-added); `remove_oldest( )` finds/deletes the true oldest via
  `READ/DELETE ... INDEX 1 USING KEY by_seq` (O(1), independent of physical table position). The one
  remaining `INSERT ls_entry INTO TABLE mt_entries` (for genuinely brand-new keys only) is now wrapped in
  `TRY ... CATCH cx_sy_itab_duplicate_key.` (empty handler) as a final safety net - always safe for a
  content-addressed cache. Verified via a live syntax dry-run against the real IT8 system
  (`SAPDiagnose(action="syntax")`) before pushing. Full lesson recorded in `/memories/abap-mcp-notes.md`.
- **Fourth issue: "Delta base not found" (a caught, non-crashing error this time), commit `69365888`**:
  the thin/non-thin retry only toggled the ofs-delta/thin-pack capability flag - it never changed which
  "haves" (verified-complete commits) got sent, so a stale/wrong locally-tracked have could make the
  server omit the SAME object in both attempts. Confirmed via direct DB query that the reported SHA1 did
  not exist anywhere in `zaog_obj_store`. Fixed by setting `iv_retry_without_haves = abap_true` on both
  "Delta base not found" raise sites in `zcl_abapgit_ortec_pack_stream` (`get_base_bytes`,
  `resolve_one_meta`'s REF_DELTA external-base branch) - this flag already propagated correctly through
  the existing re-raise chain, so `upload_pack_by_branch`/`upload_pack_by_commit`'s existing
  `is_retry_without_haves` check now correctly triggers the pre-existing third "full" (`iv_force_full`,
  no-haves) retry tier for this failure class too.
- **Fifth issue, the ACTUAL root cause (2026-07-20, commit `1ff5070a`)**: even the "full" retry tier
  STILL failed with the IDENTICAL missing base, on BOTH a large repo and a plain branch switch on the
  SMALL abapGit repo - ruling out stale haves (force_full already clears them; a non-thin+no-haves pack
  should be fully self-contained). Root cause found in `build_upload_pack_buffer`
  (`zcl_abapgit_ortec_fastpath`): the "deepen N" line (a SHALLOW-clone request, only the last N commits)
  was only skipped for the shallow/have LINES when `iv_force_full` was set - the deepen line's own `IF`
  condition only checked `it_ortec_haves IS INITIAL`, never `iv_force_full`. So the "full" retry still
  requested a SHALLOW fetch bounded to the caller's original incremental depth (typically 1), not the
  complete/unbounded history "force_full" is supposed to guarantee. Fixed: the deepen line is now ALSO
  skipped when `iv_force_full = abap_true` (omitting "deepen" entirely = standard git wire-protocol
  shorthand for "send the complete history"). Added regression test `buffer_skips_deepen_forced`. Also
  added rich diagnostics (obj_index/pack_offset/pack size/unresolved count via a new
  `count_unresolved( )` helper) to the "Delta base not found" raise in `resolve_one_meta`, in case this
  exact failure ever recurs and still needs distinguishing a real in-pack resolver bug from a genuine
  external-base miss. Verified via live syntax dry-runs against the real IT8 system before pushing.
- **Next action**: ask Michael to import commit `1ff5070a` (pushed; on top of `69365888`/`19bdbe40`/
  `9c3d297a`/`147e8499`/his own `ebd8153e`) and retry BOTH previously-failing repros (large repo stage by
  transport, and the abapGit repo branch switch) once more. If both now succeed cleanly, restore the
  `decode_and_persist` fallback in `upload_pack` (revert the temporary diagnostic re-raise from commit
  `8e03a191`) and close Phase 5.
- **Sixth issue (2026-07-20): `1ff5070a`'s "force_full = truly unbounded" was itself the wrong fix.**
  Michael retried `1ff5070a` and got the IDENTICAL "Delta base not found" on the full retry tier again, on
  the abapGit repo itself. Orchestrator delegated a fresh, independent investigation to
  `ortec-abapgit-protocol-persistence` (Claude Opus, full findings in
  [.memory/logs/protocol_persistence.md](.memory/logs/protocol_persistence.md)) - it re-verified attempts
  1-3 are ALL independently correct (no bugs found in decoder cursor math, two-pass resolver convergence,
  object-store caching, or retry-cascade plumbing), but could not close one gap: why the force_full tier
  (zero haves/shallow/deepen) still fails identically. The orchestrator closed that gap directly via
  GitHub's API: SHA1 `35fcfcb0379260fd21602953a666827b64d931f3` is a REAL commit from 2017 ("Fix issue
  793"), NOT garbage-collected, and is exactly **4737 commits** behind the current branch tip
  (`mkaesemann/abapGit`, confirmed via `/repos/.../compare/{sha}...{branch}` - `ahead_by: 4737,
  behind_by: 0`). So `1ff5070a`'s fix (force_full = omit `deepen` entirely = request the COMPLETE
  ~4737-commit history in one shot) is not a viable recovery strategy for a repo with substantial real
  history - it's very likely to fail on its own terms (timeout/memory/practically enormous pack) rather
  than the original "too shallow" problem, landing on the same visible symptom for a different reason.
  Michael confirmed this reframing and gave explicit direction (see below): PROGRESSIVE/incremental
  deepening (grow the fetch depth step by step until enough history is present), not one unbounded jump -
  which is what he asked for from the very start of this design. Michael also supplied a full independent
  external architecture/code review (`abapgit-ortec-deep-review-findings.md`, saved as
  [.memory/logs/external_review_2026-07-20.md](.memory/logs/external_review_2026-07-20.md)) with 18
  correctness findings (F-01..F-18), 12 performance findings (P-01..P-12), and 6 hardening findings
  (H-01..H-06) - see the new "Architecture hardening plan" section below for how these are being
  sequenced into implementation phases.

## Architecture hardening plan (external review + Michael's direction, 2026-07-20)

Michael's explicit direction, superseding commit `1ff5070a`'s "force_full = unbounded" approach:
**progressive/incremental deepening - widen the fetch depth step by step until the minimum sufficient
history is present, never jump straight to "everything" - balanced against avoiding too many small DB
accesses (current implementation is still too slow per Michael).** This was his original intent for the
design from the start. Full external review archived at
[.memory/logs/external_review_2026-07-20.md](.memory/logs/external_review_2026-07-20.md) (18 correctness
findings F-01..F-18, 12 performance findings P-01..P-12, 6 hardening findings H-01..H-06).

Sequenced into 6 implementation phases below, each dispatched to `ortec-abapgit-implementation` one at a
time with `ortec-abapgit-regression` validation in between (per standing operating rules - no phase starts
until the prior one is verified). Findings are mapped to phases; some findings are flagged **[RECONCILE
FIRST]** because this session's own work this week may have already partially/fully addressed them, or the
current streaming decoder (`ty_meta`) already uses a different data model than what the finding describes
(often written against the OLDER `zcl_abapgit_ortec_delta`/`ty_object`-based resolver) - the implementation
agent must re-verify against the CURRENT source before treating these as still-open defects, not blindly
re-implement.

### Phase 1 - Progressive deepening negotiation (HIGHEST PRIORITY - unblocks live Phase 5 validation)
Findings: F-02, F-03, F-12, (slim slice of F-01).

**Design** (bounded, deliberately NOT the full `commit_materialization` state machine from F-01 - that is
deferred to Phase 5/6 as a bigger schema change; this phase reuses existing `is_commit_complete`/
`get_verified_have_commits` machinery which the review confirms already exists):

1. **Rename retry-tier semantics** (F-02) in `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`/
   `upload_pack_by_commit`: tier 1 = `INCREMENTAL_THIN` (current thin, deepen = caller's/repo's current
   baseline depth), tier 2 = `INCREMENTAL_SELF_CONTAINED` (same depth, non-thin) - both unchanged in
   behavior, just named/logged explicitly per-tier (effective flags + SHA counts) so a future incident
   doesn't need a fresh investigation to know what each tier actually sent.
2. **Replace tier 3 ("full") with `RECOVERY_PROGRESSIVE`**: instead of `iv_force_full` skipping `deepen`
   entirely (revert that specific behavior from `1ff5070a` - the skip-`shallow`/skip-`have` part of that
   commit was correct and stays), loop with a WIDENING `deepen N`: start at
   `max( 4 * current_baseline_deepen, 50 )`, and on each `resolve_streaming`/`decode_streaming` failure
   widen by a factor of 4 (or similar), capped at a hard ceiling (proposed: 2000 - large enough to recover
   real gaps, small enough to bound worst-case pack size/DB writes for a single recovery attempt) and a
   capped number of widening steps (proposed: 5). Each step is still non-thin and sends NO haves (a
   stale/wrong have is exactly what got us here) but DOES send a `deepen N` line (this is the actual
   "incremental retrieval of additional information" Michael asked for). Stop and return as soon as a step
   succeeds; only raise the final combined error if the ceiling is reached.
3. **Persist the last-successful deepen level per repo/branch** in `zaog_repo_state` (the `DEEPEN_LVL`
   column already exists and is already populated per the investigation - just needs its value to flow
   back in as next time's STARTING baseline for tier 1/2, instead of every fetch restarting from a fixed
   default of 1). This directly reduces future recovery-tier triggers for repos that have already proven
   they need more depth (e.g. abapGit's own repo), which is also a performance win (fewer thin/non-thin
   round-trips that are doomed to fail before reaching the progressive tier).
4. **[RECONCILE FIRST] F-01/F-12 slim slice**: audit every call site of `zcl_abapgit_ortec_fetch_neg`
   for uses of the unverified `get_have_commits` (vs. the verified `get_verified_have_commits`) and either
   remove the unverified path from any production caller or gate it behind the same completeness check -
   the review's core complaint (F-01) is that these two paths coexist and only one is safe. Do NOT build
   the full `commit_materialization` table/state-machine in this phase - that's Phase 5/6 territory given
   its schema/migration weight and Michael's performance concern about new DB structures.

**Explicitly deferred out of Phase 1** (do not attempt): F-01's full state machine, F-06 cache rescoping,
F-07 repo_key propagation cleanup, F-11 attempt-scoped isolation - these are real but not what's blocking
today's live validation; sequenced into Phases 2/5/6 below.

**Regression tests required**: a widening-step unit test (monotonic growth, ceiling respected, stops on
first success - can stub/mock the decode outcome); a protocol test asserting `RECOVERY_PROGRESSIVE`'s wire
buffer has no `have`/`shallow` lines but DOES have a `deepen N` line with the CURRENT widened N (replaces/
updates the existing `buffer_skips_deepen_forced` test from `1ff5070a`, which will need to change since
deepen is no longer unconditionally omitted); a repo_state test confirming the baseline deepen level
persists and is reused as next fetch's starting point.

### Phase 2 - Delta dependency model reconciliation
Findings: F-04 [RECONCILE FIRST - `resolve_streaming`'s Pass 1/Pass 2 structure already implements most of
this design; verify the "bulk-resolve-then-fixpoint-again" refinement is actually missing before adding
it], F-08/F-09/F-10 [RECONCILE FIRST - `ty_meta` in the NEW streaming decoder already separates unresolved
metadata (temp_key/delta_base/base_offset) from resolved identity (sha1/obj_type) per F-08's own
recommended design; these findings read as targeting the OLDER `zcl_abapgit_ortec_delta`/`ty_object`
resolver, which is currently bypassed by the temporary diagnostic re-raise in `upload_pack` (see
`8e03a191` TODO) - confirm scope with Michael before spending effort here], F-07 (remove blank repo_key
fallback in production paths - real, concrete, no reconciliation needed), F-06 (scope base cache by
repo_key + clear between retry tiers - real, concrete).

### Phase 3 - Bulk access / N+1 elimination (performance, addresses Michael's "still too slow")
Findings: F-05 (bulk external-base prefetch - directly enables the biggest win), P-01, P-04, P-05, P-06,
P-09, P-10, P-11.

### Phase 4 - Protocol hardening
Findings: F-13, F-14, F-15, F-16, H-01, H-02, H-03, H-04, H-05.

### Phase 5 - Transaction/staging model overhaul (bigger refactor - do once Phases 1-4 are stable/proven live)
Findings: P-07 (single transaction owner - orchestrator commits, low-level methods never do), P-08
(separate staging table instead of status-flagged rows in the main table), F-11 (attempt-scoped isolation
built on top of the Phase 1 progressive-retry loop), F-01's full `commit_materialization` state machine (if
still warranted once Phase 1's slim slice is live and proven).

### Phase 6 - Misc hardening / API contracts
Findings: F-17 (typed sparse decode result instead of overloading `ty_objects_tt`), F-18 (re-hash
verification on read), H-06 (exception classification: protocol-incompatibility vs. transient-network vs.
local-corruption, instead of one broad catch-and-fallback).

**Next action**: dispatch Phase 1 to `ortec-abapgit-implementation` now (highest priority, unblocks live
validation). Regression-validate before Phase 2. Re-sequence phases 2-6 if Michael wants a different order
after seeing Phase 1 land.

- **Phase 1 implemented directly by orchestrator (commit `d8545d53`, pushed), not via
  `ortec-abapgit-implementation`**: that subagent refused TWICE with a bizarre false-positive "avoid
  mirroring public-code" response on legitimate work in our own fork, even after a trimmed, code-block-free
  prompt on the second attempt. Orchestrator implemented Phase 1 directly instead (progressive
  deepen widening replacing `1ff5070a`'s unbounded fetch, baseline-depth persistence, verified-haves on
  both thin/non-thin tiers), verified via local `get_errors` + a live IT8 syntax dry-run, then pushed.
  **Flagged for follow-up**: investigate why `ortec-abapgit-implementation` refuses this class of task
  before relying on it for Phases 2-6.
- **Phase 1 follow-up (2026-07-20, commit `295ccd4c`, pushed): progressive deepening ALSO plateaus - real
  fix is thin-pack completion, not more widening.** Michael retried Phase 1 on a different abapGit branch;
  got the identical error shape, but now showing the widening actually working PARTIALLY: `deepen 50` hit
  one missing SHA1, `deepen 200` resolved it but hit a DIFFERENT one (`34a40760d2a97004f808ec1cc8afb184e5
  3c166b`), and `deepen 800`/`2000` reproduced that SAME second SHA1 with ZERO further change - a plateau,
  not "just needs more depth". Orchestrator confirmed via GitHub's blob API that this SHA1 is a real,
  valid, stable blob (an abapGit DDIC XML file) - genuinely absent from `zaog_obj_store`. Conclusion:
  GitHub's shallow/deepen pack generation is not always guaranteed self-contained regardless of requested
  depth (a real, if under-documented, git-protocol/GitHub-implementation limitation) - widening further is
  not a reliable fix. Michael's direction: **implement thin-pack completion** (the standard git-client
  remedy - `git fetch`'s own `index-pack --fix-thin` behavior: fetch exactly the missing object via a
  targeted follow-up request instead of asking for more history). Implemented in `295ccd4c`:
  `zcl_abapgit_ortec_fastpath=>complete_missing_object` (minimal want-by-SHA1 request, relies on
  `allow-reachable-sha1-in-want`) called from `zcl_abapgit_ortec_pack_stream`'s two external-base-lookup
  sites (`get_base_bytes`, `resolve_one_meta`'s REF_DELTA branch) before raising "not found", with the
  original lookup retried once on success. Bounded by a shared `c_max_completion_attempts = 20` budget per
  top-level fetch (including nested completion fetches). `iv_url` threaded through
  `decode_streaming`/`resolve_streaming`/`resolve_one_meta`/`get_base_bytes` as `OPTIONAL` (backward
  compatible - existing tests/callers unaffected). Known limitation documented in code: a nested
  completion's own `COMMIT WORK` can force an early commit of the outer resolve pass's not-yet-flushed
  batches (not a correctness risk, but weakens the outer's own rollback-on-failure guarantee in a rare
  double-failure case) - tracked for the Phase 5 transaction-ownership rework (P-07).
- **Next action**: ask Michael to import commit `295ccd4c` (on top of `d8545d53`/`1ff5070a`) and retry the
  SAME previously-failing branch/repo. If thin-pack completion resolves it, close out this incident chain
  and resume the phase sequence (Phase 2 next). If it still fails, capture the exact new error text (should
  now say whether completion was attempted at all, per the richer diagnostics already in place) before any
  further investigation.

## Known issues to fix
- `Walk, tree not found` after branch switching or a partially buffered store. **PARTIALLY FIXED (2026-07-12, commit `51c1c52e`)**: Michael reproduced this in ES6 after switching branches, with a slow standard-decoder detour (`zcl_abapgit_zlib_huffman`) beforehand. Full root-cause analysis in [.memory/logs/incident_branch_switch_walk_failure.md](.memory/logs/incident_branch_switch_walk_failure.md). Two concrete pre-existing bugs were fixed: (1) `zcl_abapgit_ortec_obj_store=>set_active_repo_key` now makes the delta-base blank-`iv_repo_key` fallback reliable instead of depending on an accidental side effect; (2) `pull_by_branch`'s self-heal now calls the new `zcl_abapgit_ortec_repo_state=>invalidate_all_history` (repo-wide) instead of the branch-scoped `reset_fetch_commit`, so a retry actually degrades to a full/deepen pack as intended. **STILL OUTSTANDING**: the H4 walk-delegation architecture from `h4_target_architecture_legacy.mmd` (bulk-collect-then-fetch-then-retry for `walk`/`walk_tree`) remains unimplemented - the two fixes above reduce how often the failure surfaces and make the self-heal actually effective, but do not add real recovery for a genuinely-never-cached object on a brand-new branch/commit. That is a larger, separate design/implementation effort. Not yet re-verified live in ES6 - awaiting Michael's next IT8/ES6 test round.
- Wrong delta indicators in filtered stage-by-transport.
- **Wrong "D" (deleted) status badge on abapGit's OWN repo status view after a normal pull, recurring
  (2026-07-17, LIVE, screenshot confirmed)** - the 5 classes just imported for Phase 1/Phase 2 (streaming
  decoder work) show a red "D" badge in the abapGit UI's own repo overview even though the pull/import
  completed successfully and the code runs. This is the SAME symptom class as the H4 walk-delegation bug
  #6 already found and fixed this session (`persist_pull_result` bypass leaving the persistent object
  store stale so the status overview reads pre-pull data) - but that fix was specifically for
  `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s Ortec mirror dispatch, and this current instance may be
  a SEPARATE occurrence (possibly in a different pull/stage code path, or a genuinely different root
  cause that only coincidentally looks the same). **Explicitly DEFERRED per Michael's instruction
  (2026-07-17): "we can fix the badge at the end or if we stumble across the root cause by accident" -
  do NOT proactively investigate this now; only look into it if it surfaces again while working on
  something else, or once all phases are otherwise complete.**
- Large-repo slowdowns caused by full repo/pack/tree processing before the filtered path is available.
  **PARTIALLY ADDRESSED**: filtered cold-fetch (`try_filtered_commit_fetch`, commit `11bc9e84`) avoids
  full decode for filtered stage/diff specifically; have-negotiation shared-ancestry fix (commit
  `7eac2fd5`) should reduce full-snapshot re-fetches on ordinary branch switches. Both **not yet
  re-verified live**.

## Resumable backlog topics
Work intentionally paused pending Michael's direction, an external retest, or a priority call. Each entry
must carry enough context to start or resume cold, without re-reading the whole file history below. Check
this section first - before "Known issues to fix" or picking a new phase - whenever starting/resuming a
session with no other explicit instruction. Trigger via `.github/prompts/resume.prompt.md` (chat: `/resume`)
or the `ortec-abapgit-resume` subagent.

### Topic 1: H4 walk-delegation design + implementation
- **Status (2026-07-17 FINAL): IMPLEMENTED, COMMITTED, AND FULLY VERIFIED on IT8.** All 4
  `LTCL_WALK_PREP` tests pass (`complete_graph_is_noop`, `fetch_blobs_bulk_consumes`,
  `bulk_drains_across_batches`, `oversized_blob_single_batch`), confirmed via a real
  `SAPDiagnose(action="unittest")` run. Only the pre-existing, unrelated
  `LTCL_CACHE_ADMIN::OVERVIEW_AGGREGATES_COUNTS` failure remains (see the separate entry below -
  deferred per Michael's explicit instruction, not part of H4). H4 walk-delegation work is DONE;
  next up is the streaming pack decoder implementation (Topic 2 below), landing as a separate,
  sequential commit per decision (3).
  (27 chars), dispatch added to `src/git/zcl_abapgit_git_porcelain.clas.abap`'s `pull_by_branch`/
  `pull_by_commit`. Delegated to `ortec-abapgit-implementation`, but the FIRST attempt had FIVE
  real, independently-found-and-fixed bugs before this was fit to commit - listed here so future
  regression/review passes know exactly what to scrutinize:
  1. **CRITICAL regression**: the mirror dispatch bypassed the standard `pull_by_branch`'s existing
     `'Walk,'` self-heal retry (invalidate_all_history + one retry) entirely for Ortec-active repos
     - the mirror's own attempt was dead code (`IF ... CS 'Walk,'. RAISE lx_pull. ENDIF. RAISE
     lx_pull.`, both branches identical). Fixed by replicating the exact standard self-heal logic
     inside `zcl_abapgit_ortec_porcelain=>pull_by_branch`.
  2. **CRITICAL design violation**: `pull()` originally pre-fetched ALL blob batches into one merged
     table BEFORE calling `walk`, defeating the entire memory-bounding purpose of H4 batching. Fixed
     by restructuring so `walk` is called ONCE PER BATCH (re-walking the already-warm/cheap tree
     structure each time), serving only that batch's blobs and skipping others, clearing between
     batches - at most one batch's blob bytes resident at a time.
  3. **MAJOR bug**: `fetch_blobs_bulk` fetched full `obj_data` for the ENTIRE remaining SHA1 list
     before applying the byte budget, temporarily spiking memory to the full remaining volume on
     every batch call. Fixed by adding a cheap `obj_sha1, obj_size`-only metadata pre-check to select
     a budget-fitting SHA1 sublist BEFORE fetching actual `obj_data` for only that sublist.
  4. **CRITICAL regression (found on independent re-review after the above 3 fixes)**: `pull()`
     discarded `prewarm`'s own (often-empty, correctly no-op for complete tables) blob list and
     rebuilt it from an unconditional `walk_tree` scan listing EVERY reachable blob regardless of
     whether it was already in `it_objects` - meaning even today's standard, complete full-pull case
     would wastefully (and for a first-ever pull, dangerously) enter the batching path. Fixed
     directly (not delegated) by filtering the manifest-derived blob list down to only blobs NOT
     already present in `it_objects`.
  5. **CRITICAL bug (found alongside #4)**: `fetch_blobs_bulk` silently `CONTINUE`d past any SHA1
     with no object-store metadata row (genuinely missing even after `prewarm`'s topup) WITHOUT
     removing it from `ct_remaining_sha1s` - guaranteed infinite loop in `pull()`'s batching `WHILE`
     for any blob that can never be satisfied. Fixed directly (not delegated) to drain the SHA1 from
     `ct_remaining_sha1s` and raise `zcx_abapgit_ortec_git`, consistent with the "not-buffered !=
     deletion" invariant (never silently drop a file from `rt_files`).
  6. **CRITICAL regression, found LIVE on Michael's own dev repo after import (2026-07-17)**: the
     mirror dispatch bypassed the standard `pull_by_branch`'s post-pull persistence hook
     (`zcl_abapgit_ortec_fastpath=>persist_pull_result`, which writes fetched objects into
     `ZAOG_OBJ_STORE`/updates the index/repo state) - EXACTLY the same class of bug as fix #1
     (self-heal bypass), just a second, previously-missed instance of "dispatch returns before
     reaching standard behavior further down the method." Symptom: after pulling this exact H4
     commit into Michael's own Ortec-active dev repo, the repo status overview showed a wrong "D"
     (deleted-in-remote) badge for the 3 just-pulled, unchanged classes, while a direct diff
     correctly reported "no differences" - proving the live git/file state was fine but the
     Ortec-filtered status overview was reading a STALE persistent-store snapshot that predated the
     pull, because the mirror never told the persistent store about the new files. Fixed by adding
     the identical `persist_pull_result` call (same parameters, same non-critical-failure handling)
     to the end of `zcl_abapgit_ortec_porcelain=>pull_by_branch`, matching the standard method
     exactly. `pull_by_commit` needs no equivalent fix - the standard `pull_by_commit` never had a
     persist hook either, confirmed by direct comparison. **General lesson: any dispatch-at-the-top
     pattern that bypasses a standard method's body must be checked for EVERY side effect in that
     body, not just its return value** - this is now the SECOND time a "returns early, skips
     trailing side effect" bug slipped through independent review; a systematic diff-based check
     (list every side-effecting statement in the original method, confirm each has an equivalent in
     the mirror) would be more reliable than manual reading for any future dispatch-pattern work.
  - **Known, accepted, NOT-yet-optimized cost**: the batch-per-`walk`-call restructuring (fix #2)
    means the (already-warm, in-memory, no-I/O) tree structure gets re-traversed once per batch
    rather than once total - acceptable given trees are cheap and this trades a small CPU cost for
    the memory-boundedness the whole feature exists to provide, but worth remembering if a future
    profiling pass finds tree-recursion overhead significant for repos with very deep/wide trees.
  - **New test coverage**: `ltcl_walk_prep` (complete-graph no-op, bulk-fetch draining, oversized
    single blob, and multi-batch draining with an oversized-then-small-blob sequence). **NOT yet
    covered**: no test exercises `zcl_abapgit_ortec_porcelain=>pull`/`walk` end-to-end with a
    genuinely sparse `it_objects` and multiple sequential blob batches (the actual DR-004 acceptance
    criterion - "no per-object SELECT loop" - is still unverified by an automated test). Also not
    yet verified on a real system at all (local `get_errors` clean on all 4 touched files, nothing
    imported/run on IT8 yet).
  - **Next action**: run `ortec-abapgit-regression` before proceeding to the streaming pack decoder
    implementation, given how many real bugs the first implementation pass had - do not assume this
    is correct just because it is now committed and locally clean.
- **Status (2026-07-16 UPDATE): PROMOTED FROM DEFERRED TO MANDATORY.** Michael's verdict (verbatim):
  "A performance regression of thousands of select singles is unacceptable and would render the
  application unusable. Incorporate the H4 walk-delegation as a requirement into the fix." This was
  triggered by the streaming-decoder design (see Topic 2/"Live crash confirms..." below): bringing
  plain-pull into streaming-decoder scope means `pull`/`walk` receive a SPARSE `it_objects`, and
  `walk`'s existing per-object `get_object` fallback would degrade into one `SELECT SINGLE` per
  missing tree/blob - unacceptable at scale. H4 is no longer optional/efficiency-only; it is now a
  required, first-class component. The OLD entry-condition gate below ("Michael confirms the IT8/ES6
  retest of `51c1c52e` passed") is SUPERSEDED - do not wait on it.
- **Original problem statement** (still accurate): give `zcl_abapgit_git_porcelain`'s `walk`/
  `walk_tree` a real bulk-collect-then-fetch-then-persist-then-retry capability for missing tree/blob
  objects, mirroring `zcl_abapgit_ortec_missing_obj=>ensure_available`. This is the H4 box in
  [.memory/diagrams/h4_target_architecture_legacy.mmd](.memory/diagrams/h4_target_architecture_legacy.mmd).
- **Design report (`ortec-abapgit-design`, 2026-07-16, no code written) - the previously-open design
  question is now RESOLVED**: "how does `walk`/`walk_tree` obtain `iv_url`/root commit for a
  negotiated top-up" -> root commit needs NO new threading (pre-warm runs ONCE at the `pull()` level,
  before `walk()` is invoked, where `iv_commit` is already in scope - not inside the recursion at
  all); `iv_url` CANNOT be reverse-resolved from `iv_repo_key` (confirmed by reading
  `zcl_abapgit_ortec_repo_state`: `ZAOG_REPO_STATE` persists only a one-way `url_hash`, never the
  plaintext URL, and no `get_url_for_repo_key` exists or can be built without a schema change) - so
  the fix is ONE new OPTIONAL `iv_url` parameter added to `pull()` (default blank), threaded from its
  ONLY TWO callers (`pull_by_branch`, `pull_by_commit`, both already have `iv_url` in scope; `pull()`
  has NO external callers - confirmed via scope search; `push` calls `walk` directly and never
  `pull`, so it is unaffected).
  - **Two-pass mechanism**: Pass 1 (new class `zcl_abapgit_ortec_walk_prep`, `prewarm` method) does a
    fixpoint TREE-ONLY BFS from the root tree (bulk `get_objects(iv_bulk_fetch)` per level, bounded
    by tree DEPTH not object count - trees are small/cheap), then enumerates every reachable BLOB sha
    from the now-fully-decoded tree graph, then does ONE set-based `get_missing_sha1s` check; any
    genuinely-missing blobs are escalated to `zcl_abapgit_ortec_missing_obj=>ensure_available`
    UNCHANGED/reused-as-is (one negotiated `deepen 1` fetch + persist + retry - `ensure_available`'s
    existing contract is an exact fit for this residual set). Only TREES get merged into `it_objects`
    afterward - Pass 2 (`walk`'s existing recursion, modified with a small in-place batched-blob
    hook) reads blobs via a new `fetch_blobs_bulk` helper in BOUNDED BATCHES (not one at a time, and
    deliberately NOT merged wholesale into `it_objects`, since `rt_files` already holds every blob's
    data once - merging blobs into `it_objects` too would DOUBLE that residency and reintroduce the
    exact `rt_objects`-class memory ceiling the streaming decoder exists to remove).
  - **`ensure_available` confirmed reusable as-is** for the remote top-up layer, but NOT sufficient
    alone (it proves/ensures store PRESENCE, it doesn't SERVE objects into a buffer for `walk`, and
    its fetch granularity is whole-commit `deepen 1`, not per-blob) - H4 = new local bulk-warm engine
    (trees + blob batching, the part that actually kills the SELECT-SINGLE storm) PLUS
    `ensure_available` reused for the residual genuinely-missing case. Mirrors the already-proven
    `get_files_for_filter` + `ensure_available` pairing on the filtered Stage/Diff path, applied to
    plain-pull.
  - **New class**: `zcl_abapgit_ortec_walk_prep` (27 chars, verified under the 30-char limit) with
    `prewarm` (public), `warm_trees`/`collect_blob_shas`/`topup_missing_blobs` (private), and
    `fetch_blobs_bulk` (public, thin wrapper over `get_objects(iv_bulk_fetch)` for `walk`'s new
    batched-blob hook). Reuses `zcl_abapgit_ortec_obj_store=>get_objects`/`get_missing_sha1s` and
    `zcl_abapgit_ortec_missing_obj=>ensure_available` UNCHANGED.
  - **Regression safety, explicitly verified/designed for**: self-gating (a COMPLETE `it_objects` -
    today's standard full pull - makes `prewarm` a pure no-op, zero added I/O, byte-identical
    behavior); double-gated remote top-up (skipped on blank `iv_url`, AND `ensure_available`'s own
    `is_active_for_repo` check); bounded fetches (at most one negotiated fetch + retry via
    `ensure_available`; `walk`'s residual fallback stays store-only, never networks); the
    `NOT_BUFFERED != deletion` invariant preserved (H4 only ever raises on unresolved objects, never
    signals deletion); the EXISTING `pull_by_branch` `'Walk,'` self-heal (invalidate-all-history +
    full deepen re-fetch) remains the ultimate correctness backstop ABOVE H4, unchanged - if
    `prewarm`/top-up fails, it propagates so `walk` still raises and self-heal still fires exactly as
    today; standard-code touch is minimal (one optional `pull()` param + one small in-place batching
    hook in `walk` - if design-review judges the `walk` hook too invasive for the D7 minimal-touch
    budget, route it via an Ortec mirror `zcl_abapgit_ortec_porcelain` instead, per the established
    D7 fallback).
  - **Effort: Medium** (smaller than the streaming decoder; dominated by the tree fixpoint BFS, the
    `walk` batching hook, and regression porting; no new schema, no new wire protocol). **Impact:
    High** - eliminates the SELECT-SINGLE-storm risk for BOTH the streaming decoder's sparse
    `it_objects` case AND today's existing partially-cached branch-switch scenario (a standalone win
    even before the streaming decoder exists). **Recommended sequencing: land H4 FIRST, standalone,
    before/alongside the streaming decoder** - it helps branch-switches today and is the prerequisite
    that makes it SAFE to answer the streaming decoder's "should plain-pull be in v1 scope" question
    with "yes."
  - **Open questions - DECIDED by Michael (2026-07-16)**, superseding the original list:
    (1) **Accepted as designed** - `fetch_blobs_bulk` batch size is byte-budgeted. **Oversize-object
    rule (DR-003 revision, 2026-07-17):** a byte-budgeted blob batch must always make progress even
    when a single reachable blob is larger than the whole batch budget - such a blob is fetched and
    served ALONE as its own one-object batch (loaded, copied into `rt_files`, then freed before the
    next batch), never skipped, never looped on, and never allowed to silently push the batch over
    budget for other blobs. If even one oversize blob cannot be held solo within available memory, H4
    fails CLEANLY and propagates to the streaming-fallback/self-heal backstop (DR-001), leaving
    persistent state isolated as defined there rather than blowing the budget or spinning.
    (2) **Accepted as designed** - trees merged into `it_objects`, blobs read in bounded batches
    (never merged wholesale). **Mirror `walk` batching invariant (DR-004 revision, 2026-07-17),
    specified precisely enough to prove ALL reachable blobs are served via byte-budgeted batches for
    a repo with more distinct blobs than fit in one batch:** pass 1 (tree-only BFS in
    `zcl_abapgit_ortec_walk_prep`) enumerates the COMPLETE set of reachable blob SHA1s. Pass 2 (the
    mirror's `walk`/`walk_tree`) MUST iterate that ENTIRE enumerated blob-SHA set in byte-budgeted
    batches via `fetch_blobs_bulk`: fill one batch (bounded by the byte budget, or a single oversize
    blob per DR-003), serve EVERY blob in that batch from the WARM in-memory batch while building
    `rt_files`, CLEAR the batch before fetching the next, and continue until the enumerated set is
    exhausted - so a repo with more distinct blobs than fit in one batch is handled by MULTIPLE
    sequential warm batches, not by degrading to per-object reads. The old per-object
    `zcl_abapgit_ortec_obj_store=>get_object` fallback inside `walk` is retained ONLY as an
    EXCEPTIONAL correctness backstop (e.g. a blob somehow absent from pass-1 enumeration), never the
    expected/normal path for a sparse `it_objects`. Acceptance criterion: a plain-pull over a sparse
    `it_objects` must NOT exhibit a SELECT-SINGLE-per-blob pattern - object-store read count must be
    O(number of byte-budgeted batches), not O(number of blobs); the implementation acceptance test
    fails if a per-object SELECT loop appears in the hot walk path.
    (3) **Land ALONGSIDE the streaming decoder, but as TWO SEPARATE, SEQUENTIAL COMMITS** (not
    combined into one, and not with a large standalone gap beforehand either) - a refinement of the
    agent's "standalone first" recommendation: same overall timeframe, still two independently
    reviewable/revertable commits, one after the other.
    (4) **Accepted as a known limit for now.** Michael's reasoning (verbatim intent): this should not
    hurt in practice, since the initial retrieval already worked correctly (even if slow), and the
    project's design goal was always "first access to a never-buffered branch/commit can be slower,
    but every subsequent access benefits from buffering and is fast" - `rt_files`'s one-time
    materialization cost on first access is consistent with that intent, not a new problem.
    (5) **Route the `walk` batched-blob hook through an Ortec mirror (`zcl_abapgit_ortec_porcelain`)
    to retain strong separation** - Michael explicitly chose the mirror over the "small in-place
    hook" recommendation, prioritizing isolation of Ortec logic from standard abapGit code even at
    the cost of a bit more implementation surface (a new mirror class rather than a tiny edit to
    `zcl_abapgit_git_porcelain`). This means `pull`/`walk`/`walk_tree`'s Ortec-routed behavior will
    live in a NEW `zcl_abapgit_ortec_porcelain` class, not as edits to the standard class - exact
    routing mechanism (how callers choose mirror vs standard) is an implementation-phase detail to
    resolve, consistent with the D7 minimal-touch invariant's own documented mirror fallback pattern.
    **Mirror-dispatch contract (DR-005 revision, 2026-07-17) - recorded now so the H4-first commit is
    independently shippable and reversible; this supersedes "routing is an implementation-phase
    detail":**
    - **Non-Ortec repos: ZERO behavior change.** They keep calling standard `zcl_abapgit_git_porcelain`
      exactly as today - the mirror is never on their call path.
    - **Dispatch point:** mirror-vs-standard is chosen at the `pull_by_branch`/`pull_by_commit` entry
      points (the only two callers of `pull()`, both already holding `iv_url`), gated by the existing
      Ortec-active predicate. Ortec-active -> call `zcl_abapgit_ortec_porcelain=>pull_by_branch`/
      `pull_by_commit` (which carry the new optional `iv_url` param through to `pull`); not
      Ortec-active -> unchanged standard call. No standard->Ortec->standard ping-pong: dispatch
      happens ONCE at the top and the mirror owns the whole `pull`/`walk`/`walk_tree` sub-tree for
      that call.
    - **Push is unaffected** (`push` calls `walk` directly, never `pull`); it stays on standard
      porcelain unless/until explicitly routed by a later, separate change.
    - **Full-table no-op equivalence:** given a COMPLETE (non-sparse) `it_objects`, the mirror must
      behave IDENTICALLY to today's standard porcelain - `prewarm` is a pure no-op, no warm-tree or
      blob-batch path activates, byte-identical output. The new warm-tree / byte-budgeted blob-batch
      paths activate ONLY for the intended sparse `it_objects` case.
    - **Independently shippable/reversible:** because dispatch is a single top-level gated branch and
      the full-table case is a no-op, the H4-first commit can ship alone and be reverted by removing
      the dispatch branch, with no residual coupling into standard porcelain.
    - **H4-alone validation:** validate H4 both with a COMPLETE `it_objects` (must be a byte-identical
      no-op vs standard porcelain) AND with a deliberately SPARSE `it_objects` (must serve all
      trees/blobs via warm byte-budgeted batches with no per-object SELECT loop, per DR-004).
    (6) **Accepted as designed** (`deepen 1`-of-root-commit top-up granularity is correct) - see the
    separate, detailed explanation given directly to Michael in this session for his follow-up
    question about whether `deepen > 1` could increase blob reuse: it would not - blob reuse/dedup
    is entirely the job of have-negotiation (content-addressed objects are only ever sent once per
    response regardless of depth), not of `deepen`, which purely controls how much ADDITIONAL,
    older commit history gets pulled in (strictly more data for `ensure_available`'s narrow
    single-commit top-up purpose, not less) - `deepen 1` remains the correct minimal choice for this
    specific use case.
    (7) **Accepted as designed** - `prewarm`/top-up failures propagate to the existing self-heal
    backstop; H4 does not attempt its own separate recovery.
- **Status: DESIGN FULLY DECIDED, NOT YET APPROVED FOR IMPLEMENTATION.** All 7 open questions
  answered. **Revised 2026-07-17** to address the "Streaming Decoder + H4 Walk-Delegation Review"
  (verdict REVISE_AND_REVIEW_ONCE): DR-003 oversize-blob rule added to decision (1), DR-004 mirror
  `walk` batching invariant added to decision (2), DR-005 mirror-dispatch contract added to decision
  (5). **Re-reviewed 2026-07-17: APPROVE_WITH_MINOR_REVISIONS - see the top "APPROVED FOR
  IMPLEMENTATION" status line above.** Sequenced/committed alongside the streaming decoder per
  decision (3) above, landing FIRST as its own commit.

### Topic 2: Shallow-clone protocol correctness (Option A) vs full-clone-once (Option B)
- **Status**: DEFERRED / AWAITING MICHAEL'S DECISION. Do not implement either option until Michael
  explicitly agrees to a direction - he has explicitly asked to hold off.
- **What**: today's `a51e743b` fix (haves-free force-full retry) is a working but wasteful band-aid
  over a real root cause - every fetch this system performs is shallow (`deepen 1`), but the `have
  <sha>` line sent to the server carries no information about that shallow boundary, so the server
  assumes full ancestry is present and can wrongly conclude "nothing new" for a want that shares
  ancestry with a shallow have (confirmed live: a release branch cut from `development/6.0.x` with
  zero new commits triggered exactly this). Michael is concerned the current band-aid means a "full"
  re-fetch on every such branch switch (10+ seconds on his large repo) and asked for a design
  comparison between (A) implementing correct git `shallow`/`unshallow` wire-protocol lines so the
  server never gets this wrong in the first place, vs (B) doing one full (non-shallow) clone per repo
  on first access and building the incremental delta system entirely on top of genuinely complete
  local data.
- **Orchestrator's own read + `ortec-abapgit-design` third opinion (2026-07-16, consulted, no code
  written)**: both independently converged on the SAME recommendation: **Option A is correct;
  Option B should be rejected as currently framed.** Full design report evaluated the exact
  mechanism precisely: `is_commit_complete`/`get_reachable_sha1s` verifies snapshot-completeness
  (commit->tree->blobs for that ONE commit only, never parent history), while a `have <sha>` line
  means "I have everything reachable from this, including ancestry" - so advertising a shallow
  commit as a have is a **mis-advertisement**, not a server bug. Option A (send `shallow <sha>` for
  every shallow have, parse the server's `shallow`/`unshallow` echo lines, currently unparsed) fixes
  this at the correct protocol granularity and should reduce the repro case to ONE round-trip with a
  small incremental delta (not a full re-fetch) - **contingent on confirming, via a live wire-trace
  against Azure DevOps, that AzDO actually honors `shallow`+`have` together and excludes shared blobs
  rather than ignoring haves entirely once `deepen`/`shallow` is present (the existing code comment
  "with deepen, the server ignores have lines" is exactly the open unknown here)**.
- **Option B rejected**: its safety premise ("resumable_decode is memory-safe because it persists
  incrementally") does not hold - `rt_objects` holds every decompressed object of a pack resident
  in memory SIMULTANEOUSLY (never streamed/freed per-object; confirmed via the `29514cb7`/`11adac3a`/
  `2d55195a` incident notes above), so a full clone of this repo (~2.1GB decompressed, ~78,526
  objects) would put ~2.1GB in `rt_objects` alone - squarely in the same crash territory as today's
  incidents, just bigger. None of today's fixes address this (they removed a SECOND copy, a
  negotiation-time preload, and an unbounded-request shape - not the primary accumulator). Option B
  would need a genuine streaming decoder (persist-and-free per object) as a large, separate,
  high-risk prerequisite before it could even run without crashing, on top of a multi-minute/2.1GB
  first-touch cost per repo. Not attractive as primary OR fallback; revisit only if a streaming
  decoder is independently built for other reasons.
- **Recommended phasing** (from the design report, not yet approved by Michael):
  Phase 0 (done): keep `a51e743b`'s force-full retry as the last-resort fallback regardless of what
  else is decided (needed for servers that won't honor shallow boundaries).
  Phase 1 (proposed, NOT started): implement Option A, gated behind a BLOCKING live wire-protocol
  spike against Azure DevOps FIRST (observe actual `want`+`have`+`shallow`+`deepen` server behavior
  before committing to a request shape) - v1 can treat ALL haves as shallow (since `is_shallow` is
  unconditionally `abap_true` for every fetch today, no new per-commit tracking needed initially).
  Phase 2 (optional, later, low priority): make `is_commit_complete` distinguish snapshot-complete
  from reachability-complete precisely, only useful once/if a genuinely non-shallow fetch path exists.
  Also flagged, NOT recommended now: "Option C" - on a still-wrong nothing-new, fetch only the
  specific missing blobs by SHA instead of force-full re-fetching the whole want - contingent on
  unverified `uploadpack.allowReachableSHA1InWant` server support (Azure DevOps unconfirmed, GitHub
  known-restricted per earlier notes) - a possible future upgrade to the fallback, not a v1 item.
- **Effort/impact** (from the design report): Option A = Medium effort (request-line emission is
  small; parsing the shallow/unshallow response section without breaking existing pack/side-band
  parsing is the medium part; the live spike carries schedule uncertainty, not the coding itself) /
  High impact (removes the wasted round-trip AND shrinks the repro's second fetch from a full
  snapshot to a small delta, for every branch-switch on a large repo - Michael's exact daily
  workflow). Option B = Large effort (unmet large prerequisite: streaming decode or chunked-history
  fetch, BEFORE the full-clone orchestration itself can even be built) / High correctness but
  net-negative UX+risk (multi-minute first-touch per repo, implemented in the exact code area that
  has crashed repeatedly this session, zero benefit until the clone completes).
- **Open questions/risks needing Michael before any implementation** (from the design report):
  (1) BLOCKING - exact Azure DevOps behavior for `deepen`+`shallow`+`have` combined, needs a live
  trace, cannot be resolved from memory/code alone; (2) GitHub-migration compatibility of `shallow`/
  `deepen` (standard, believed safe, but re-verify when migrating - contrast with Option C's
  `allowReachableSHA1InWant` which is NOT migration-safe); (3) confirm it's acceptable for v1 to
  treat ALL haves as shallow rather than building real per-commit granularity now; (4) `rt_objects`
  full-pack residency is the real ceiling for BOTH options - a legitimately huge divergent-branch
  delta could still OOM even under Option A; a streaming decoder is the one investment that raises
  this ceiling for everything, is it in scope now or accepted as a known bound for now; (5) confirm
  keeping `a51e743b`'s retry as the permanent fallback after Option A lands; (6) if "nothing new"
  responses are STILL found wrong after Option A ships, that is new signal warranting a wire-level
  trace per this project's own "escalate after 2+ recurrences" rule, not another band-aid.
- **Resume steps once Michael decides**: if Option A approved, the BLOCKING live spike (open
  question 1) must run before any code is written - this determines the actual request shape and
  whether the payoff is "small delta" or merely "round-trip elimination" per the design report's
  §2 table. After the spike, proceed through this project's normal design-review -> implementation
  -> regression phases like any other change.
- **Decision (2026-07-16): Michael approved Option A.** Proceed per the phasing above once ready;
  the blocking live wire-protocol spike against Azure DevOps is the next concrete step whenever
  implementation begins.
- **Clarification on Option B (2026-07-16, before approving A, no code changed)**: Michael correctly
  challenged why a full clone would need gigabytes of memory at all, noting real git clients (e.g.
  git.exe) never do. Confirmed: it would NOT, inherently - our specific implementation has TWO
  separate, non-equally-hard bottlenecks, not one unavoidable constraint:
  (1) **HTTP receive layer** (`zcl_abapgit_http_agent` -> `cl_http_client`'s `SEND`/`RECEIVE`/
  `GET_DATA`): `RECEIVE` blocks until the ENTIRE response body has arrived and `GET_DATA` returns it
  as one xstring - there is no streaming/incremental-read mode exposed here, unlike git.exe reading
  directly off a raw socket and writing straight to a temp pack file on disk as bytes arrive. This
  bounds us to "the COMPRESSED pack must fit in memory to receive it" - real, but much smaller than
  bottleneck 2 (compressed size is a fraction of decompressed size), and a genuine ABAP/kernel-level
  constraint given how this client is used, harder to remove than bottleneck 2.
  (2) **Decode layer, the actual dominant cause** (`zcl_abapgit_ortec_pack_dec=>resumable_decode`):
  builds ONE in-memory table (`rt_objects`) holding EVERY object's fully decompressed data for the
  WHOLE pack simultaneously, unlike git's own `index-pack` which decompresses mostly one object at a
  time, uses a BOUNDED delta-base cache (`core.deltaBaseCacheLimit`, default 96MB - not "every object
  ever seen"), and writes/frees each object as it goes. This is a genuine ARCHITECTURAL CHOICE in our
  decoder (built this way so the multi-pass delta resolver can randomly access any prior object as a
  base), not an ABAP platform limitation - it is directly addressable via a real "streaming decoder"
  redesign (persist-and-free per object; bounded LRU or DB-backed base lookup instead of unbounded
  in-memory retention; optionally split a full fetch into multiple smaller requests). This IS exactly
  the "streaming decoder" prerequisite the design report flagged for Option B, confirmed as a
  legitimate, buildable (if large) future investment, not a fundamental impossibility - useful
  context for revisiting Option B (or handling an unusually large legitimate delta under Option A)
  later, but explicitly NOT being built now.
- **Option B full backlog record (2026-07-16) - a THIRD, independent code path, not built now**:
  captured here in full so it survives to whenever this is picked up, without needing to re-derive
  context. Do not start real design/implementation work on this before Option A has shipped and been
  live-verified (Option A is expected to solve the immediate correctness/performance problem; Option
  B is a separate, larger, longer-horizon investment, not an urgent follow-up).
  - **Bottlenecks this must solve** (already recorded above, repeated here for a self-contained
    entry point): (1) `zcl_abapgit_http_agent`'s `cl_http_client` `SEND`/`RECEIVE`/`GET_DATA` pattern
    blocks until the ENTIRE HTTP response body has arrived and returns it as one xstring - no
    incremental/streaming read exposed, unlike git.exe reading directly off a raw socket. (2)
    `zcl_abapgit_ortec_pack_dec=>resumable_decode` builds one in-memory table (`rt_objects`) holding
    EVERY object's fully decompressed data for the whole pack simultaneously, instead of persisting
    and freeing each object as it's resolved with a bounded delta-base cache (git's own
    `core.deltaBaseCacheLimit`-style approach, default 96MB, not "every object ever seen").
  - **Investigation task 1 (Michael's explicit ask)**: investigate if/how a genuinely streaming TCP
    connection could be established from ABAP, comparable to how a real git client reads incrementally
    off a raw socket - explore whether ICM/SAP push channels, or any other lower-level ABAP networking
    API (raw TCP sockets if exposed, `cl_http_client` chunked-transfer partial reads if actually
    supported, or any other incremental-read mechanism), can give true streaming receipt of the HTTP
    response body instead of the current all-or-nothing `RECEIVE`/`GET_DATA`. This directly addresses
    bottleneck 1. Unresolved/unverified as of 2026-07-16 - needs real investigation, not assumed
    impossible NOR assumed straightforward.
  - **Investigation task 2**: even if bottleneck 1 cannot be fully solved (no streaming socket API
    available), bottleneck 2 (the decode-side unbounded `rt_objects` accumulation) is independently
    and definitely addressable regardless: persist each object and free its decompressed buffer as
    soon as it's resolved; use a bounded LRU cache or DB-backed lookup of already-persisted objects
    to serve as delta bases instead of unbounded in-memory retention; optionally split a full sync
    into multiple smaller requests (deepen-walking in increments, or per-branch/subset fetches)
    rather than one giant request, which also mitigates bottleneck 1's "whole compressed pack in one
    response" constraint even without a true streaming socket.
  - **Investigation task 3 / implementation constraint (Michael's explicit ask)**: if/when this is
    built, it must be implemented as a COMPLETELY SEPARATE code path - a third parallel mechanism
    alongside (a) standard abapGit's existing decoder and (b) the current Ortec fastpath - NOT a
    replacement or in-place rewrite of either. This lets all three coexist and be exercised in
    parallel (standard abapGit path, Ortec fastpath, and this new "git-client-compatible"/streaming
    path) until the new path is sufficiently battle-tested, at which point one or more of the older
    paths can be removed via deletion, rather than a risky big-bang cutover. This mirrors exactly how
    the Ortec fastpath itself was introduced alongside standard abapGit rather than replacing it
    in-place - same pattern, one level up.
  - **Why this is being deferred rather than done alongside Option A**: Option A directly targets the
    specific correctness bug Michael is hitting today, is a Medium-effort, well-scoped change within
    the EXISTING protocol/decode architecture, and does not require this larger investment. Option B
    is a genuinely separate, large, three-part investment (streaming transport research + streaming
    decoder redesign + a whole new parallel code path with its own battle-testing period) that is
    valuable for a different reason (removing the `rt_objects` memory ceiling entirely, which is a
    latent bound even under Option A for a pathologically large legitimate delta - see Topic 2's
    open question 4) but is not required to solve today's reported problem.
  - **Resume steps whenever this is picked up**: (1) re-read this entry plus the "Clarification on
    Option B" note above for full context; (2) run investigation tasks 1 and 2 as genuine research
    (subagent-suitable, read-only/spike-only, no productive code) before any design work; (3) if a
    viable streaming transport is found, or if proceeding decode-only (task 2) is judged sufficient
    on its own merits, run this project's normal discovery/design/design-review cycle for the new
    parallel code path, explicitly scoping it as additive per investigation task 3; (4) implementation
    only after design-review reaches APPROVE/APPROVE_WITH_MINOR_REVISIONS, exactly like any other
    phase.

## Option A implemented (2026-07-16, commit `677a3daf`)
- Delegated to `ortec-abapgit-implementation` per Michael's explicit authorization (no separate live
  wire-protocol spike beforehand - implement then verify live, consistent with today's established
  pattern). Scope: `zcl_abapgit_ortec_fastpath=>upload_pack` now emits a `shallow <sha>` line for
  every commit offered as a `have` (v1: ALL haves, since `is_shallow` is unconditionally `abap_true`
  for every fetch today - no new per-commit tracking needed), in the correct git pack-protocol wire
  order (want(s) -> shallow(s) -> at most one deepen -> flush -> have(s) -> done). `parse` now also
  recognizes plain (non-side-band) `shallow`/`unshallow` response lines a server may send before the
  packfile, exposed via new optional `et_shallow`/`et_unshallow` output params for diagnostics
  (best-effort - malformed lines are skipped, never fatal). The existing `a51e743b` haves-free retry
  fallback is unchanged and remains the last-resort path for servers that don't honor shallow
  boundaries. New `ltcl_fastpath_protocol` test class (4 tests: shallow-line emission + ordering,
  shallow-line suppression under `iv_force_full`/no-haves, shallow/unshallow response collection,
  malformed-line tolerance).
- **Independent spot-check found and fixed THREE real problems in the subagent's initial submission
  before trusting/committing it** (consistent with this project's established "always independently
  verify subagent output" pattern - see the Phase 7 ATC precedent):
  1. **Unintended, unrelated `abaplint.json` overwrite** (840-line diff replacing the project's real
     lint config with a generic default, presumably from running some lint/init tool) - completely
     out of scope for this task, reverted via `git checkout -- abaplint.json` before anything else.
  2. **A real regression in `parse`'s exception handling**: the original `CATCH cx_sy_range_out_of_
     bounds INTO DATA(lx_range). zcx_abapgit_ortec_git=>raise(...)` safety net (protects against a
     malformed/truncated pkt-line causing a raw ABAP runtime range error) was SILENTLY DELETED and
     replaced with a pointless `CATCH zcx_abapgit_ortec_git ... RAISE EXCEPTION` no-op plus an
     overly-broad `CATCH zcx_abapgit_exception` that would have ALSO swallowed unrelated failures
     (e.g. from `length_utf8_hex`) that were always meant to propagate to the caller. Fixed by
     restoring the exact original `CATCH cx_sy_range_out_of_bounds` handler (the new shallow/
     unshallow text-decode step already has its own correctly-scoped INNER try/catch for just that
     step, which was fine and left as-is).
  3. **A structural syntax bug that local `get_errors` did NOT catch**: an erroneous duplicate/
     premature `ENDCLASS.` was inserted partway through the PRIVATE SECTION (when moving
     `build_upload_pack_buffer`/`parse` to PUBLIC), which orphaned the existing
     `serve_cached_when_nothing_new`/`is_retry_without_haves` private method declarations OUTSIDE
     any class definition entirely. Confirmed via direct grep for `^ENDCLASS\.`/`^PRIVATE SECTION\.`
     occurrence counts (found 2 `ENDCLASS.` where there should be 1) - local tooling's `get_errors`
     reported clean throughout, another confirmed instance (like the 30-char-name and inline-`@DATA()`
     issues earlier this project) of local tooling missing something the real SAP compiler would
     have caught. Fixed by removing the erroneous premature `ENDCLASS.`.
  4. **A visibility issue local tooling also missed**: the new tests call `build_upload_pack_buffer`
     and `parse` directly, but both were declared PRIVATE with no `FRIENDS` declaration anywhere in
     `src/ortec/` (confirmed via grep) - and `git log` confirmed `parse` had genuinely never been
     directly unit-tested before (this is a real, new gap, not a pre-existing accepted pattern).
     Fixed by moving both to PUBLIC, with a doc-comment explaining why (mirrors the exact precedent
     already set by `peek_object_count` earlier today - a pure, stateless helper made public
     specifically for direct unit testability).
- All four fixes verified via `get_errors` (clean) plus manual structural re-inspection (grep-counted
  section/ENDCLASS occurrences, confirmed method-declaration-to-implementation 1:1 correspondence)
  given local tooling's demonstrated blind spots for this exact class of error today.
- **Not yet re-verified live** - this is Option A's actual implementation; the real-world validation
  (does Azure DevOps honor `shallow`+`have`+`deepen` the way expected, per Topic 2's blocking open
  question) still needs to happen against the live environment, per Michael's explicit choice to
  verify live rather than do an isolated spike first.

## Live crash confirms the flagged residual risk: rt_objects ceiling (2026-07-16)
- Immediately after Option A landed, Michael hit a NEW SYSTEM_NO_ROLL - but this is NOT a regression
  from Option A, it is the EXACT residual risk explicitly flagged in the Option A/B design report's
  open question 4 ("a genuinely large legitimate delta could still OOM even under Option A, because
  rt_objects holds the whole decoded pack").
- **Live evidence** (`mcp_arc-12_SAPDiagnose`, dump `20260716161421T-...`): memory request for
  392,289,080 bytes (392MB) failed; Used Memory 3,766,258,064 (~3.77GB), Free Memory only 4,891,328
  (~4.9MB) at crash time. A `SESSIONMEM_QUOTA_WARNING` fired 11 seconds earlier (same program),
  confirming memory climbed steadily (a large pack being decoded) rather than one sudden runaway
  request. Crash site: `zcl_abapgit_ortec_pack_dec=>resumable_decode`'s pack-trailer SHA1 check
  (`lv_xstring = iv_data(lv_len). lv_sha1 = zcl_abapgit_hash=>sha1_raw( lv_xstring ). CLEAR
  lv_xstring.`) - `lv_len = xstrlen(iv_data) - 20`, i.e. `lv_xstring` briefly duplicates nearly the
  entire pack, on top of `iv_data` itself and the already-fully-materialised `rt_objects` (every
  decompressed object of the pack, held resident simultaneously - the real root cause; the trailer
  duplicate is just the line that tipped an already-critical memory state over the edge). Call chain:
  Stage page (filtered) -> `ensure_available` (blob top-up for a genuinely new, never-before-fetched
  release branch) -> `upload_pack_by_commit` -> `upload_pack` -> `decode_and_persist` ->
  `resumable_decode` -> crash. This confirms Option A is working as intended (server now sends a
  real, correct delta instead of a wrong "nothing new") - the delta itself is just large enough to
  hit the pre-existing, already-known `rt_objects` ceiling.
- **Streaming-hash research (delegated to a small/fast model, `MAI-Code-1-Flash`, read-only,
  2026-07-16)**: Michael asked to confirm upfront whether `CL_ABAP_MESSAGE_DIGEST` (used by
  `zcl_abapgit_hash=>sha1_raw`) has a working incremental/streaming hash API before implementing
  anything, to avoid speculative work. **Result: NOT confirmed** - no evidence anywhere (this
  codebase, general SAP/ABAP knowledge, or documentation search) of an instance-based/incremental
  update-then-finalize API on this class; only one-shot `CALCULATE_HASH_FOR_RAW`/
  `CALCULATE_HASH_FOR_CHAR` requiring the full payload already in memory. **Decision: do NOT
  implement a streaming-hash micro-fix for the trailer-check duplicate** - per Michael's own
  explicit framing, unconfirmed-feasibility work is skipped. The SAME research pass did confirm
  `zcl_abapgit_ortec_pack_dec` already has a PROVEN, working streaming pattern for a different
  operation - decompression (`decode_commits_only`'s use of `cl_abap_ungzip_binary_stream` +
  `if_abap_ungzip_binary_handler` + `decompress_binary_stream_git`, fed successive input slices,
  receiving output incrementally via a handler callback) - a real, already-battle-tested-in-this-
  codebase building block for the streaming decoder design below.
- **Streaming decoder design report (`ortec-abapgit-design`, 2026-07-16, no code written)** - the
  "real fix" for the `rt_objects` ceiling, addressing Michael's three explicit requirements
  (investigate streaming TCP, solve both bottlenecks, build as a third parallel path):
  - **Correction to the framing, confirmed by the design agent reading the code first**:
    `resumable_decode` ALREADY persists every object's decompressed bytes to `zaog_obj_store`
    (status `'P'`) inside the decode loop - the persistence substrate already exists. The actual
    problem is objects are ALSO retained in `rt_objects` simultaneously (plus a transient second
    copy for the promote step) - the redesign is "stop ALSO keeping everything in memory", not
    "invent persistence".
  - **Architecture**: keep only lightweight per-object METADATA in memory during decode (index,
    pack_offset, type, sha1/base-ref, size, dec_status - a few MB even for 78k objects, bounded by
    object COUNT not size), write real bytes to `zaog_obj_store` and free the local immediately, and
    add a new byte-budgeted LRU delta-base cache (git's `core.deltaBaseCacheLimit`-equivalent, e.g.
    96MB) so recently-touched bases don't need a re-read for a dependent delta in a later pass. The
    EXISTING non-recursive multi-pass fixpoint resolver logic is kept (proven correct), just
    re-driven over metadata+DB+LRU instead of a shared mutable `ct_objects` table - explicitly
    preserves all four historical delta-correctness fixes by construction (no shared-mutable-table
    hazard exists anymore once resolution reads immutable DB bytes and writes independent rows).
    `zcl_abapgit_ortec_delta=>apply`/`get_offset`/`skip_size_header` stay UNCHANGED and reused as-is
    (pure functions, no `ct_objects` dependency). Per-pass BULK base prefetch (reusing the existing
    "targeted delta-base prefetch" `FOR ALL ENTRIES` pattern) keeps DB round-trips per-pass, not
    per-object. **Delta-base prefetch is byte-bounded at the byte-LOADING layer, not only at the SQL
    round-trip layer (DR-002 revision, 2026-07-17)** - it is a TWO-LAYER design so bulk SQL cannot
    re-create the memory ceiling in a different internal table:
    - **Layer 1 - set-based DISCOVERY (cheap, COUNT-bounded, no base bytes read):** resolve the
      distinct set of base SHA1s/pack-offsets a pass needs purely from the in-memory metadata table.
      This produces only identifiers, never bytes.
    - **Layer 2 - byte-budgeted LOADING/APPLY (BYTE-bounded):** fetch and apply those bases in
      BATCHES sized to the active byte budget (the same 256MB LRU budget bounds how many base bytes
      may be resident at once). Each batch reads its base SHA1s via ONE bulk `FOR ALL ENTRIES`
      round-trip (preserving the bulk-SQL win - never per-object SELECTs), applies their dependents,
      frees the batch bytes, then fetches the next batch.
    A single pass NEVER materializes all of its distinct base bytes into one internal table unless
    the total byte estimate is within the active budget; when the estimate exceeds the budget the
    pass is split into multiple byte-budgeted `FOR ALL ENTRIES` batches. Bulk SQL is preserved by
    fetching batches, not by loading the whole pass at once. (A single base that alone exceeds one
    batch budget follows the DR-003 oversize solo-processing rule below.)
  - **Streaming-TCP/HTTP-receive findings (Michael's investigation task 1), evidence-based**: true
    socket-level streaming receipt is NOT achievable on a standard on-premise ABAP/NetWeaver stack -
    `cl_http_client`'s `receive()`/`get_data()` blocks until the ICM has buffered the ENTIRE response
    and returns it as one xstring (no chunk callback, no partial-read variant, even with chunked
    transfer-encoding on the wire - the ICM reassembles fully first); there is no supported raw-TCP
    socket API for application ABAP; APC/AMC push channels are WebSocket-only and cannot receive
    ordinary HTTP(S) git smart-HTTP traffic. **Recommendation**: accept "the compressed pack must fit
    in memory to receive it" as a fixed platform constraint (compressed size is a fraction of
    decompressed size, so this alone moves the ceiling far out once bottleneck 2 is fixed), and
    optionally mitigate further via REQUEST-SPLITTING at the protocol level (incremental `deepen`
    walking for a first-ever large fetch: deepen K, persist, re-fetch with the now-known tip as a
    `have`, repeat) rather than a true streaming socket - this is the one lever that also helps
    bottleneck 1 without new low-level APIs.
  - **Third parallel path, per Michael's explicit requirement**: two NEW classes proposed -
    `ZCL_ABAPGIT_ORTEC_PACK_STREAM` (29 chars - the streaming decoder driver,
    `decode_and_persist_streaming`) and `ZCL_ABAPGIT_ORTEC_BASE_CACHE` (28 chars - the LRU) - both
    within the `zcl_abapgit_ortec_*` naming convention (both verified under the 30-char ABAP global
    object name limit per the standing user-memory lesson).
    **SUPERSEDED (2026-07-17) - the routing/scope description immediately below this point is
    STALE and kept only for archaeology; do NOT implement from it.** It originally proposed a
    DEFAULT-OFF switch with automatic SIZE-based selection scoped to store-backed entry points
    only, with plain-pull explicitly OUT of v1 scope. Both are superseded by the later, final
    decisions recorded under "Open questions - DECIDED by Michael" below: routing is PURE PER-REPO
    OPT-IN (decision 2, no size threshold), streaming is the NEW DEFAULT for any Ortec-active repo
    including plain-pull (decision 6, old fastpath becomes the fallback tier), and plain-pull IS
    in v1 scope specifically because H4 (Topic 1) makes it safe (see "Open question 1 now
    RESOLVED"). Stale text follows for historical record only:
    ~~Routing: a new, DEFAULT-OFF switch
    predicate (e.g. `zcl_abapgit_ortec_git_switch=>is_streaming_active_for_repo`) plus automatic
    size-based selection at the STORE-BACKED entry points only (`ensure_available` and
    `try_filtered_commit_fetch` - exactly the live-crash call chain), using the already-available
    cheap signals `peek_object_count`/`xstrlen`. Plain-pull (`et_objects` fully returned to standard
    abapGit callers) is explicitly OUT of v1 scope, since the streaming decoder's whole point is to
    NOT return a full in-memory table - deferred until/unless that return contract is separately
    refactored.~~ Zero standard-abapGit files touched; existing fastpath/negotiation/Option-A logic
    completely unchanged, only the decode-and-persist STEP is swapped for routed, store-backed
    callers.
  - **Phasing (riskiest first)**: Phase 0 spikes (BLOCKING Spike A: prove a DB-fetched delta base
    produces byte-identical `apply()` results vs an in-memory one - the whole premise rests on this;
    Spike B: per-pass bulk-SELECT cost/LRU hit-rate sanity check; Spike C: re-confirm the bottleneck-1
    negative finding on the actual target release; Spike D: confirm `stream_decompress` works
    correctly on a slice mid-pack, already true in `decode_commits_only`, low risk) -> Phase 1 LRU
    cache (isolated, low risk) -> Phase 2 decode-and-free loop + metadata table -> Phase 3 (bulk of
    the effort) streaming multi-pass resolver, porting ALL existing delta regression scenarios (REF
    chains, delta-on-later-delta, OFS, two-thin-bases-no-collision, SHA1-order != pack-order) -> Phase
    4 routing at the two store-backed entry points only -> Phase 5 live validation on the actual
    crashing IT8 repo, measure peak memory, confirm the SYSTEM_NO_ROLL is gone.
    **Phase 0/Spike A: DONE, PASSED (see Spike A entry above).**
    **Phase 1: IMPLEMENTED, committed `df5def0a`, VERIFIED LIVE on IT8 (2026-07-17) - all 5
    `ltcl_base_cache` tests pass** (`put_get_round_trip`, `get_missing_returns_initial`,
    `lru_eviction`, `oversize_blob_is_not_cached`, `clear_removes_entries`). New standalone class
    `zcl_abapgit_ortec_base_cache` (28 chars) - a generic, git `core.deltaBaseCacheLimit`-style
    LRU cache for delta-base object bytes keyed by SHA1, hardcoded 256MB budget, oversize objects
    silently not admitted (per DR-003), singleton via `get_instance()` with `clear()` for pass/test
    isolation. NOT yet wired into any decode path - purely isolated per the phasing plan.
    **Phase 2: IMPLEMENTED, committed `bcc91801` (2026-07-17), FULLY VERIFIED LIVE on IT8
    (2026-07-17) - all 3 `ltcl_pack_stream` tests pass** (`delta_free_pack_decodes`,
    `ref_delta_stays_unresolved`, `corrupt_trailer_no_rows`), with the full `zcl_abapgit_ortec_git_tests`
    suite at 73 passed / 1 failed (only the pre-existing, deferred `ltcl_cache_admin::
    overview_aggregates_counts` `CX_SY_OPEN_SQL_DB` failure - unrelated, no new regressions in
    `LTCL_WALK_PREP`/`LTCL_BASE_CACHE` or anything else).
    New class `zcl_abapgit_ortec_pack_stream` (29 chars), method `decode_and_persist_streaming`
    (28 chars). Decodes a pack one object at a time, persists each object's decompressed bytes to
    `zaog_obj_store` immediately under a NEW status `'I'` (incomplete - distinct from the old
    decoder's `'P'`, invisible to every existing read path since those all hardcode `status = 'R'`),
    then frees the local buffer before the next object - never holds more than one object's
    decompressed bytes at a time. Non-delta objects persist under their real SHA1 with
    `is_resolved = abap_true`; REF_DELTA/OFS_DELTA objects persist under a temp key
    (`pack_id` + zero-padded object index, mirroring the old decoder's own temp-key convention) with
    `is_resolved = abap_false`, `delta_base`/`base_offset` captured in the returned `ty_meta_tt` for
    a later phase to resolve - delta resolution/application is explicitly OUT OF SCOPE for Phase 2.
    On full success (incl. trailer SHA1 check, verified byte-for-byte against the proven
    `zcl_abapgit_git_pack=>decode` pattern) all of this run's `'I'` rows are promoted to `'R'` in one
    set-based UPDATE + COMMIT; on any failure all of this run's `'I'` rows are deleted in one
    set-based DELETE (`cleanup_incomplete`, keyed on `repo_key`+`pack_id`+status='I' only - never
    touches other pack_ids, the old decoder's `'P'` rows, or its `zaog_pack_idx`/`zaog_pack_meta`/
    `zaog_raw_pack`/`zaog_fetch_sess` bookkeeping tables, which this streaming path deliberately does
    not use) before re-raising. New tests in `ltcl_pack_stream`: `delta_free_pack_decodes` (2-blob
    happy path via `zcl_abapgit_git_pack=>encode`, checks both metadata rows `is_resolved=abap_true`
    and both objects visible via `exists()` after promotion), `ref_delta_stays_unresolved` (manually
    built single-REF_DELTA pack, checks `is_resolved=abap_false`, blank `sha1`, correct `delta_base`,
    non-blank `temp_key`, and confirms the temp-keyed row was promoted to `'R'`),
    `corrupt_trailer_no_rows` (corrupted trailer byte, expects `zcx_abapgit_ortec_git` raised and
    ZERO `zaog_obj_store` rows left for the test repo_key). **Implementation note**: a first
    subagent-delegated attempt reproduced the OLD decoder's full-object-accumulation +
    full-delta-resolution pattern (wrong status `'P'`, built unwanted `zaog_pack_idx`/`zaog_pack_meta`/
    `zaog_raw_pack`/`zaog_fetch_sess` records) - would have provided ZERO benefit toward the
    `rt_objects` ceiling this whole effort exists to fix. Discarded entirely; this implementation was
    written directly instead.
    **Two real bugs found and fixed only via the live IT8 run** (both independently found by re-diffing
    the whole file, not just re-checking for a clean compile - see the two matching lessons in
    `/memories/abap-mcp-notes.md`):
    1. `store_object`'s `iv_status TYPE c LENGTH 1 DEFAULT 'R'` param is invalid ABAP - `DEFAULT`
       cannot combine with an inline `TYPE c LENGTH n`; local `get_errors`/abaplint missed this
       entirely. Fixed by referencing the DB field type directly: `TYPE zaog_obj_store-status`
       (commit `b1393b07`'s predecessor fix, then Michael's own manual "Syntax Fixes" commit
       `a1c13b63` independently arrived at a similar signature fix but **also silently reverted the
       method body's `ls_row-status = iv_status.` back to the hardcoded `ls_row-status = 'R'.`**
       while doing so - reintroducing the exact bug `iv_status` exists to fix, undetected by any
       compile check since it's a pure logic regression, not a syntax error).
    2. Found and fixed directly from #1's live symptom (`corrupt_trailer_no_rows` failing "no rows may
       remain"): restored `ls_row-status = iv_status.` in `store_object`'s body (commit `b1393b07`).
    3. Separately, `ltcl_pack_stream`'s `setup`/`teardown` used the established-but-latently-unsafe
       `DELETE ... ROLLBACK WORK` pattern (copied from `ltcl_obj_store`'s convention), which only
       cleans up correctly for SUTs that never `COMMIT WORK` themselves - since
       `decode_and_persist_streaming` does commit by design, the `ROLLBACK WORK` was undoing the
       test's own uncommitted `DELETE` instead of anything real, leaking `'R'`-status rows into later
       runs. Fixed to `ROLLBACK WORK` -> `DELETE` -> `COMMIT WORK` (commit `6f404891`).
    method-name-length re-checked (29/28 chars, both under the 30-char limit).
    **Phase 2 CLOSED. Ready to start Phase 3** (streaming multi-pass resolver), which depends on this
    phase's `ty_meta_tt` shape as its input contract.
    **Phase 3: IMPLEMENTED, committed `514b127c` (2026-07-17), NOT YET imported/verified on IT8.**
    New public method `zcl_abapgit_ortec_pack_stream=>resolve_streaming( iv_repo_key, iv_pack_id )
    CHANGING ct_meta`, porting `zcl_abapgit_ortec_delta=>resolve_all`/`resolve_one`'s proven multi-pass/
    chain/thin-fetch algorithm onto the metadata-only contract: a delta's raw bytes are read from
    `zaog_obj_store` (via `temp_key`) only for the duration of one `apply()` call, the resolved result is
    persisted under its real recomputed SHA1 and freed immediately, and the now-superseded temp-keyed row
    is deleted - never more than one resolved delta's bytes held in memory at a time. Pass 1: repeated
    ascending in-pack-only sweeps (mirrors `resolve_all`'s own phase 1) converging on any delta-onto-
    later-delta chain regardless of topological/SHA1/pack order. Pass 2: one final pass allowing external/
    thin bases via `zaog_obj_store`, routed through the Phase 1 LRU base cache
    (`zcl_abapgit_ortec_base_cache`) to avoid re-reading a base shared by multiple deltas, raising on a
    truly missing base. REF_DELTA base lookup uses a sha1->tabix index populated ONLY from already-
    resolved rows - since an unresolved `ty_meta` row's `sha1` field is simply blank (never overloaded as
    a placeholder, unlike the old `ct_objects`'s `-sha1`), this needs none of `resolve_one`'s "skip self /
    skip other unresolved rows" workaround. OFS_DELTA base lookup uses a static pack_offset->tabix index
    (OFS bases are always earlier in the same pack, per format guarantee). Commits once at the very end of
    a fully successful resolve; on any failure the whole call's writes roll back together (nothing commits
    until the end) - callers should `ROLLBACK WORK` on catch. New tests in `ltcl_stream_resolve`
    (constructing metadata rows + `zaog_obj_store` fixtures directly, bypassing full pack-byte
    construction - same technique `ltcl_ref_delta`/`ltcl_ofs_delta` already use for `resolve_all`):
    `ref_chain_resolves`, `ofs_chain_resolves`, `external_thin_base_resolves`,
    `two_thin_bases_do_not_collide`, `missing_base_raises`. `get_errors` clean on both files;
    method/class-name lengths re-checked (all <=29 chars). **A `replace_string_in_file` call while
    inserting these methods initially dropped the `METHOD decode_and_persist_streaming.` line and left a
    stray `ENDCLASS.` mid-file (the exact shared-boundary-line corruption pattern already documented in
    `/memories/abap-mcp-notes.md`) - caught immediately via a `grep_search` structural sanity check
    (`^ENDCLASS\.|^CLASS |^  METHOD `) before any further work, and fixed.** Explicitly out of scope for
    Phase 3: wiring `resolve_streaming` into any pull/fetch entry point (Phase 4), and any change to the
    existing non-streaming `zcl_abapgit_ortec_delta`/`zcl_abapgit_ortec_pack_dec` path.
    **Two more real bugs found via the live import/syntax check (commit `95aa2ebf`, 2026-07-17)**: (1) the
    `decode_and_persist_streaming` corruption-fix edit had also dropped its own `DATA lv_data TYPE
    xstring.` declaration (same shared-boundary-line class of bug, now doubly-documented in
    `/memories/abap-mcp-notes.md`) - restored; (2) `ltcl_stream_resolve::ref_chain_resolves` passed a
    string-concatenation expression (`'486921' && '21'`) directly as `sha1_blob`'s XSTRING parameter - a
    real compiler error local tooling missed - replaced with the plain hex literal `'48692121'`. **Next
    action**: ask Michael to import commit `95aa2ebf`, then live syntax check + `action="unittest"` on
    `ZCL_ABAPGIT_ORTEC_GIT_TESTS`, confirm the 5 new `ltcl_stream_resolve` tests pass and no regression
    elsewhere, then proceed to Phase 4 (routing + fallback cascade).
    **Phase 3 status update (2026-07-17).** After importing `95aa2ebf`,
    the live syntax check surfaced one more real gap: `resolve_one_meta` called `zcl_abapgit_hash=>sha1`
    (RAISES `zcx_abapgit_exception`) outside any TRY/CATCH and undeclared in its own RAISING clause -
    folded into the same TRY as `apply()`, which already converts that exception type. Live unittest then
    ran 4 of 5 `ltcl_stream_resolve` tests green immediately; `ref_chain_resolves` failed with an uncaught
    `zcx_abapgit_ortec_git` - root-caused to the TEST, not `resolve_streaming`: it made base object C purely
    external (store-only, no `ct_meta` row), silently combining two scenarios (in-pack chain + external/
    thin base) that neither the new algorithm NOR the original `resolve_all`/`resolve_one` claims to solve
    together in one single-pass Phase 2 (an EARLIER-positioned dependent visited before a LATER-positioned
    delta has had a chance to resolve its OWN external base in the same pass - a pre-existing scope
    limitation, not a Phase 3 regression). Fixed the test to represent C as an in-pack, already-resolved
    `ct_meta` row exactly like the original `chain_onto_later_unresolved` scenario. Fix committed as
    `9b57999e`, **imported and FULLY VERIFIED LIVE on IT8 (2026-07-17): all 5 `ltcl_stream_resolve` tests
    pass**, full suite at 78 passed / 1 failed (only the pre-existing, deferred `LTCL_CACHE_ADMIN::
    OVERVIEW_AGGREGATES_COUNTS`) - no regressions in `LTCL_WALK_PREP`/`LTCL_BASE_CACHE`/`LTCL_PACK_STREAM`
    or anywhere else.
    **Known, accepted scope limitation carried over from the original algorithm (not fixed, not required
    by the design doc's Phase 3 scenario list)**: a chain where an EARLIER-positioned delta depends on a
    LATER-positioned delta that ITSELF requires a genuinely external/thin base is not guaranteed to resolve
    in one `resolve_streaming`/`resolve_all` call, since Phase 2/pass 2 is a single ascending sweep, not a
    fixpoint - if this ever proves necessary for a real repo, Phase 2 would need to become a repeated sweep
    (like Phase 1) instead of one final pass.
    **Phase 3 CLOSED. Ready to start Phase 4** (routing + fallback cascade: wiring `decode_and_persist_
    streaming` + `resolve_streaming` into the two store-backed entry points, per-repo opt-in, with the old
    fastpath decoder as the fallback tier per the design's decision (6)).
    **Phase 4: IMPLEMENTED, committed `05d093a7` (2026-07-17), NOT YET imported/verified on IT8.** New
    public method `zcl_abapgit_ortec_pack_stream=>decode_streaming( iv_data, iv_repo_key ) RETURNING
    rt_objects` orchestrates `decode_and_persist_streaming` + `resolve_streaming`, then returns ONLY the
    resolved commit object(s) - the sparse contract standard `pull()`/H4 need (trees/blobs stay in
    `zaog_obj_store`, served on demand by H4). `decode_and_persist_streaming` gained an optional
    `EXPORTING ev_pack_id` so callers can thread the same pack_id into `resolve_streaming`
    (backward-compatible). Wired into BOTH store-backed entry points identified by the design as the
    live-crash call chain: `zcl_abapgit_ortec_fastpath=>upload_pack` (also reached via `ensure_available`
    -> `upload_pack_by_commit`) and `=>try_filtered_commit_fetch`. Routing per design decision (6):
    streaming tried FIRST (default for any Ortec-active repo), falling back to the proven non-streaming
    `zcl_abapgit_ortec_pack_dec=>decode_and_persist` on the SAME pack bytes on any `zcx_abapgit_ortec_git`
    failure (DR-001 fallback cascade tier 2) - `upload_pack`'s progress message now names which tier ran
    (streaming vs fallback) so a fallback event is visible, not silently normalized, per the design's
    fallback-telemetry note. `try_filtered_commit_fetch` only ever checked "did anything decode" and never
    reads object content, so `decode_streaming`'s sparse result needed no further adaptation there. New
    test `ltcl_pack_stream=>decode_streaming_is_sparse`: encodes a real commit+tree+blob pack, decodes via
    `decode_streaming`, asserts the returned table has ONLY the commit while tree/blob remain fully
    available via the store. **Not covered by new tests**: the routing/fallback glue inside
    `upload_pack`/`try_filtered_commit_fetch` itself (would need HTTP client mocking beyond current test
    infra) - the decode+resolve+sparse-extract logic those methods now call is fully covered by
    `decode_streaming_is_sparse` + the existing `ltcl_stream_resolve` suite; the fallback branch reuses the
    already-tested `decode_and_persist` unchanged. **Next action**: ask Michael to import commit
    `05d093a7`, then live syntax check + `action="unittest"` on `ZCL_ABAPGIT_ORTEC_GIT_TESTS`, confirm
    `decode_streaming_is_sparse` passes and no regression elsewhere, then proceed to Phase 5 (live
    validation on the actual crashing IT8 repo - measure peak memory, confirm the SYSTEM_NO_ROLL is gone).
    **Phase 4 FULLY VERIFIED LIVE on IT8 (2026-07-17).** Syntax clean on `zcl_abapgit_ortec_pack_stream`
    and `zcl_abapgit_ortec_fastpath` (only pre-existing, unrelated `build_upload_pack_buffer` warnings -
    a method this phase never touched). Unit tests: 79 passed / 1 failed (only the pre-existing, deferred
    `LTCL_CACHE_ADMIN::OVERVIEW_AGGREGATES_COUNTS`) - `decode_streaming_is_sparse` passes, no regressions
    anywhere else (`LTCL_WALK_PREP`/`LTCL_BASE_CACHE`/`LTCL_PACK_STREAM`/`LTCL_STREAM_RESOLVE` all still
    green). **Phase 4 CLOSED. Ready to start Phase 5** (live validation on the actual crashing IT8 repo -
    the ONLY phase that needs a real, large, previously-crashing repository to exercise; measure peak
    memory, confirm the SYSTEM_NO_ROLL is gone, confirm the acceptance metrics from the design's review
    addendum: (a) peak memory bounded, (b) object-store read count O(byte-budgeted batches) not O(distinct
    blobs), (c) no per-object SELECT-SINGLE loop in the hot walk path).
  - **Effort: Large** (comparable in scope to the original fastpath introduction, one level down;
    dominated by Phase 3's delta-correctness porting and Phase 5's live-validation schedule risk),
    cleanly phased and independently shippable. **Impact**: removes the `rt_objects` ceiling for
    store-backed consumers - i.e. the EXACT live-crash path - for repos/entry points that opt in.
    Does NOT solve: bottleneck 1 remains a platform constraint (a pathologically huge single
    compressed response can still OOM at RECEIVE time, before decode - only request-splitting
    mitigates, and only partially); plain-pull stays on the existing fastpath (deferred); non-routed
    repos are unaffected either way (opt-in).
  - **Open questions - DECIDED by Michael (2026-07-16)**, superseding the original list:
    (1) RESOLVED separately - v1 scope includes plain-pull, enabled by mandatory H4 (see Topic 1).
    (2) **Pure per-repo opt-in - NO automatic size-threshold routing.** Combined with decision (6)
    below: once a repo has opted into Ortec, streaming becomes the DEFAULT execution engine for that
    repo (not gated by pack size) - there is no separate "streaming vs fastpath by size" switch.
    (3) **LRU byte budget: hardcoded constant, 256MB** (not git's 96MB default, not configurable for
    now - a plain hardcoded value to start with). **Oversize-object rule (DR-003 revision,
    2026-07-17):** the 256MB budget governs LRU ADMISSION, it is not a hard gate that can block
    progress. If a single base object's decompressed bytes are larger than the whole 256MB cache
    budget, that base is still fetched and applied SOLO (loaded, used for the current delta apply,
    then immediately freed and NOT admitted to the LRU - it bypasses the cache rather than evicting
    the entire cache or exceeding the budget), so resolution always makes forward progress one
    oversize base at a time. The LRU never loops re-trying an object it cannot admit and never
    silently drops a needed base. If even solo load+apply of that one object would exceed available
    roll/heap memory, the pass fails CLEANLY into the streaming-fallback/self-heal path (DR-001),
    leaving persistent state isolated as defined there rather than crashing with partial rows
    exposed.
    (4) **Priority order confirmed: functional correctness first (make it work without OOM), optimize
    performance along the way** - Michael wants confirmation that any performance impact stays
    limited, but is explicit that correctness (no crash) outranks performance for this effort,
    consistent with the project's existing non-negotiable invariant ordering.
    (5) **Accepted as a permanent, unavoidable platform constraint** - "compressed pack must fit in
    memory to receive" cannot be avoided given the ABAP HTTP client APIs available; no request-
    splitting investment planned as part of this effort.
    (6) **Three-path coexistence strategy decided**: standard abapGit remains the proven solution for
    smaller/non-opted-in repos (unchanged). For Ortec-opted-in repos, the NEW STREAMING PATH BECOMES
    THE DEFAULT (not the old `resumable_decode` fastpath) - the intent is to battle-test it in
    production from the start. The OLD fastpath decode becomes an explicit FALLBACK (try streaming
    first; fall back to the old fastpath decode on failure) until streaming bugs are eliminated -
    mirrors the already-proven "try new, fall back to previous tier" cascade shape already used for
    thin->non-thin negotiation elsewhere in this codebase. This is a refinement of the original
    "automatic size-based routing" proposal - there is no size threshold; the new path is simply
    tried first, always, for any Ortec-active repo.
    **Streaming failure/fallback boundary (DR-001 revision, 2026-07-17) - now an explicit, ordered
    step in the fallback cascade, not left undefined:** the streaming decoder persists decoded object
    bytes to `zaog_obj_store` incrementally, so a mid-stream failure can leave PARTIAL rows already
    committed. Those partial rows must NEVER be visible to any downstream read path (the old
    `resumable_decode` fallback, H4 warm/top-up, or delta base resolution) as if they were good
    cache. Chosen mechanism (session/status-scoped isolation):
    - Every row a streaming run writes is tagged with the run's fetch-session key AND written in a
      dedicated in-progress state `CORRUPT_OR_INCOMPLETE` (distinct from the committed `'P'` present
      state).
    - Rows are promoted to `'P'` only when the ENTIRE streaming decode+resolve completes successfully
      - a single set-based promote of that session's rows at the very end (all-or-nothing visibility).
    - EVERY object-store read path (`get_object`/`get_objects`/`get_missing_sha1s`, and by extension
      H4 pass-1/pass-2 and delta base resolution) filters OUT `CORRUPT_OR_INCOMPLETE` rows, so a
      partially-written session is simply invisible - equivalent to "not buffered", never mistaken
      for deletion (preserves the `NOT_BUFFERED != deletion` invariant).
    The fallback cascade is therefore, in STRICT order: (i) streaming decode+persist (session-scoped,
    rows in `CORRUPT_OR_INCOMPLETE`); on failure -> (ii) ATOMICALLY delete/roll back that
    fetch-session's `CORRUPT_OR_INCOMPLETE` rows (one set-based `DELETE` keyed on the session key +
    state, so no partial state survives into the next tier) -> (iii) run the old `resumable_decode`
    fastpath decode on now-clean state -> (iv) if that also fails, the existing `pull_by_branch`
    `'Walk,'` self-heal (invalidate-all-history + full deepen re-fetch) remains the ultimate backstop,
    unchanged. The keyed delete of the session's own isolated rows is idempotent, so the boundary is
    safe to re-run even after a partial-crash resume. This supersedes any prior ambiguity about "what
    happens to already-persisted rows before fallback runs."
    (7) **Prefer retaining the existing `zaog_fetch_sess` resume machinery if possible**; if it
    conflicts with the streaming design, persist-and-free-only (relying on the new approach's
    inherent crash-resilience) is explicitly permitted as a fallback design choice. **Refined by
    DR-001 (2026-07-17):** the session key used to isolate/roll back partial streaming rows is the
    `zaog_fetch_sess` key when that machinery is retained (one concept, reused - no second session
    identifier), or a dedicated per-run streaming session id if `zaog_fetch_sess` is dropped; either
    way the `CORRUPT_OR_INCOMPLETE`-state isolation + keyed cleanup of DR-001 is MANDATORY and does
    not depend on which of the two resume choices is taken.
    (8) **Rephrased and reconfirmed**: this was originally phrased as a blocking gate rather than a
    real question. Rephrased as: "before building the full streaming delta-resolver (the highest-
    risk part of this design), should a small, isolated verification check run FIRST, proving a
    delta's base object produces identical `apply()` results whether read fresh from the database or
    already resident in memory - the assumption the entire resolver redesign depends on?" **Awaiting
    Michael's answer to the rephrased question** (recommended: yes, run it first - cheap, de-risks
    the largest single investment in this design before committing to it).
- **Review revision addendum (2026-07-17, optional improvements from the review, incorporated
  where cheap):**
  - **Sparse `rt_objects` contract for plain-pull:** the streaming decoder's returned table is
    intentionally sparse - at minimum the `commit` object (hard-required by `pull()`'s
    `READ TABLE ... type=commit`), plus any trees H4 chooses to merge into `it_objects`; blob bytes
    are served through H4's bounded byte-budgeted batches and are NOT merged into `rt_objects`, and
    `rt_files` remains the ONLY full file-content materialization (accepted one-time first-access
    cost per H4 decision (4)).
  - **Acceptance metrics (added to the implementation plan / Phase 5 validation):** (a) peak memory
    on the IT8 crash path stays bounded and the SYSTEM_NO_ROLL is gone; (b) object-store read count
    during a sparse plain-pull is O(byte-budgeted batches), NOT O(distinct blobs); (c) proof that no
    per-object SELECT-SINGLE loop appears in the hot walk path (DR-004 acceptance test).
  - **Fallback visibility:** the streaming->old-fastpath fallback event (DR-001 step iii) is logged
    via telemetry so a fallback is VISIBLE and not silently normalized while streaming is being
    battle-tested.
- **Status: APPROVED FOR IMPLEMENTATION (APPROVE_WITH_MINOR_REVISIONS, 2026-07-17).** All open
  questions including the rephrased (8) are now answered/resolved (Spike A run and PASSED - see the
  Spike A entry above). The re-review after the DR-001..DR-005 revision pass returned
  `APPROVE_WITH_MINOR_REVISIONS` with one remaining minor, non-blocking item (stale pre-decision
  "size-based routing / plain-pull out of v1 scope" wording in the architecture bullet above,
  contradicted by the later final decisions) - marked SUPERSEDED in place immediately above rather
  than deleted, to preserve archaeology. Per the project's gatekeeping rule, APPROVE_WITH_MINOR_
  REVISIONS clears implementation to begin; `ortec-abapgit-implementation` may now be invoked for
  the phased build (Phase 0 spikes already satisfied by Spike A; proceed from Phase 1 LRU cache).
  Full revision history: **Revised 2026-07-17** to address the "Streaming Decoder + H4
  Walk-Delegation Review" (verdict REVISE_AND_REVIEW_ONCE): DR-001 streaming failure/fallback
  boundary added to decisions (6)/(7); DR-002 byte-bounded two-layer delta-base prefetch added to
  the architecture bullet; DR-003 oversize-base rule added to decision (3); optional-improvement
  notes (sparse `rt_objects` contract, acceptance metrics, fallback telemetry) added.
- **Correction to the design report's v1 scope (2026-07-16, verified directly against code, no code
  changed)**: Michael asked whether standard abapGit's `pull(...)`/`walk` genuinely require the full
  `it_objects` table back after a streaming decode, since they ultimately still need to return
  objects/files to standard abapGit code. Verified directly in
  `src/git/zcl_abapgit_git_porcelain.clas.abap`: `pull(...)` DOES have one hard requirement - its
  first step is `READ TABLE it_objects WITH KEY type=commit sha1=iv_commit`, which RAISES
  `'Commit/Branch not found.'` immediately if missing, no fallback - so the streaming decoder's
  returned table must include AT LEAST the commit object. BUT `walk`/`walk_tree` (called recursively
  by `pull`) do NOT require the rest: both already have a real, already-battle-tested per-object
  fallback to `zcl_abapgit_ortec_obj_store=>get_object( iv_repo_key, iv_sha1 )` for BOTH trees and
  blobs whenever missing from `it_objects` (dating from the earlier branch-switch walk-failure fix
  this project, not new/speculative code). This means the design report's claim that plain-pull
  "genuinely consumes the full table today" and "needs... refactoring standard consumers" to support
  streaming was NOT fully verified and is likely too conservative: a SPARSE `rt_objects` (commit
  object only, or commit+root-tree) would work CORRECTLY for plain-pull too, with zero signature
  changes to `pull`/`walk`/`walk_tree` and zero standard-abapGit code changes - `walk`'s existing
  fallback transparently pulls each tree/blob from the already-persisted `zaog_obj_store` as needed.
  **The real, previously-unweighed tradeoff**: that fallback is ONE SELECT PER MISSING OBJECT, not
  bulk - for a repo with thousands of files, a near-empty `it_objects` means thousands of individual
  single-row SELECTs during the walk. Correct, but potentially slow - trading the memory problem for
  a possible performance regression on large trees, unless mitigated. This is exactly the gap the
  still-deferred H4 walk-delegation backlog topic (see "### Topic 1" above) was meant to close (give
  `walk`/`walk_tree` a real bulk-collect-then-fetch-then-retry capability instead of one-at-a-time
  DB reads) - **H4 and the streaming decoder are complementary, not competing**: streaming decoder
  solves the memory ceiling, H4-style bulk pre-warming would solve the resulting per-object-SELECT
  performance concern if plain-pull is brought into streaming-decoder scope. This REVISES open
  question 1 above: v1 scope should not automatically default to "plain-pull deferred" - whether to
  include plain-pull (accepting the per-object-SELECT performance tradeoff, or pairing it with H4)
  is now a real option worth Michael's explicit consideration, not a closed question.
- **Open question 1 now RESOLVED (2026-07-16)**: Michael ruled out the per-object-SELECT tradeoff
  outright ("A performance regression of thousands of select singles is unacceptable and would
  render the application unusable") and required H4 as a mandatory component instead of accepting
  the tradeoff. See the fully updated "### Topic 1: H4 walk-delegation" entry above for the resolved
  design (new `zcl_abapgit_ortec_walk_prep` class, one optional `iv_url` param on `pull()`, bulk
  tree-BFS + batched-blob-read mechanism) - H4 is now recommended to land FIRST, standalone, as the
  prerequisite that makes it safe to include plain-pull in the streaming decoder's v1 scope.
- **Spike A built, committed, and RUN on IT8 - PASSED (2026-07-16)**: `LTCL_SPIKE_A_DB_BASE_PARITY`
  `DB_BASE_MATCHES_IN_MEMORY` in `zcl_abapgit_ortec_git_tests.clas.testclasses.abap` (commit
  `554a2e41`) confirms `zcl_abapgit_ortec_delta=>apply()` gives identical results for a DB-fresh-read
  base vs. an in-memory one. This closes rephrased open question (8) and the single blocking
  prerequisite Michael gated Phase 3 (streaming resolver) implementation on. **Getting this result
  required THREE follow-up fix-and-reimport round trips**, all for real, previously-invisible-to-
  local-tooling compiler errors in the SAME test file that had nothing to do with Spike A itself, but
  were blocking the ENTIRE class pool from compiling (ABAP compiles the whole class pool, so ANY
  error anywhere blocks syntax check/unit-test execution for everything, including brand-new,
  correct code):
  1. `ff0ced31` - one pre-existing method name over the 30-char limit (`base_positioned_after_dependent`,
     31 chars, renamed to `base_after_dependent`).
  2. `689b6196` - a PROACTIVE full-file scan (after the first hit) found **11 more** over-length
     method names (12 total) - several introduced by ME earlier this same session, not just old code
     - proving this is a systemic, high-frequency blind spot, not a one-off. All renamed; see
     `/memories/abap-mcp-notes.md` for the reusable PowerShell one-pass scan command.
  3. `6f2ea3e9` - two more real, previously-hidden bugs only reachable once the above stopped masking
     them: (a) `INSERT zaog_commit_hist FROM VALUE #( ... ).` is invalid Open SQL (needs the `@`
     host-expression operator: `FROM @( VALUE #( ... ) )`), 3 occurrences; (b)
     `zcl_abapgit_ortec_fastpath=>parse`'s `et_shallow`/`et_unshallow` are EXPORTING parameters, so
     the test was calling them under the caller's `EXPORTING` clause instead of `IMPORTING` (2 call
     sites). Local `get_errors`/abaplint reported ZERO errors for all of these at every step - every
     one was only found via a live `mcp_arc-12_SAPDiagnose(action="syntax")` check on IT8.
  - **New, generalized lesson for future implementation phases**: do not trust "local tooling clean"
    as proof of correctness for this codebase; run a real-system syntax check as the actual gate
    before considering any ABAP change (test or production) verified. See
    `/memories/abap-mcp-notes.md` for the full, reusable list of this class of error.
- **Three UNRELATED, pre-existing test failures surfaced once the full suite could finally execute
  end-to-end for the first time (2026-07-16) - two now FIXED, one DEFERRED**:
  1. `LTCL_OFS_DELTA::OFFSET_MULTI_BYTE` - **FIXED (`2eb9cae8`)**. Root cause: the test's own `FF7F`
     expected value (16383) was a naive base-128 concat, not git's real OFS_DELTA varint formula
     (`offset = (offset+1)<<7 | byte`) - confirmed by cross-checking every other vector in the same
     test against the same formula. Production `get_offset()` was always correct; only the test
     data was wrong. Corrected to 16511. CONFIRMED PASSING on a subsequent real run.
  2. `LTCL_PACK_DECODER::PREFETCH_BASES_DO_NOT_COLLIDE` - **FIXED (`58108b4b`)**. Root cause: C(hex
     text) -> X(raw bytes) assignment silently produces GARBAGE bytes (not an error) unless the hex
     string is UPPERCASE first - the test computed lowercase hex via `zcl_abapgit_hash=>sha1_raw()`/
     `sha1_blob()` and assigned it directly to `TYPE x LENGTH 20` fields (2 base SHA1s + 1 pack
     trailer) without `to_upper()`. Confirmed via a temporary diagnostic (self-check assertion
     comparing the test's own recomputed SHA1 against the actual appended trailer bytes) that the
     resulting raw bytes were garbage (`0x10` followed by zeros). Fixed by wrapping all three
     conversions in `to_upper(...)`, matching the already-proven-working pattern in
     `zcl_abapgit_git_pack=>encode()` (`lv_sha1 TYPE x LENGTH 20 = to_upper( sha1_raw(...) )`).
     Temporary diagnostics removed once root-caused. See `/memories/abap-mcp-notes.md` for the
     generalized lesson (new, distinct from the earlier offset-notation/30-char/Open-SQL findings).
  3. `LTCL_CACHE_ADMIN::OVERVIEW_AGGREGATES_COUNTS` - **STILL UNRESOLVED, DEFERRED per Michael's
     explicit instruction (2026-07-17): "if you can't fix it now, then move on to the actual
     implementation task and stop trying to fix the broken test."** Exact failing exception class/
     text still NOT captured - three escalating diagnostic attempts (catch `cx_sy_open_sql_db`
     around just `get_overview()`; widened to wrap the entire DB-seed sequence too; finally widened
     to catch `cx_root` with `cl_abap_classdescr=>get_class_name()` to report the real runtime
     class) were all committed (`e3c4ef3c`, `e3cc90e2`) but the LAST one was never actually verified
     against a live run - IT8 connectivity dropped (`ADT network error: fetch failed`) right as this
     final diagnostic was imported and about to be checked. **Unrelated to any of today's work or
     the streaming decoder/H4 effort** - pre-existing, deferred backlog item. The `cx_root` diagnostic
     wrapper is still live in the committed test code (commit `e3cc90e2`) if/when this is picked back
     up - just re-run the unit test once IT8 is reachable again to get the real class name/message.

## Key design targets
- Two-layer Git model: persistent local object store first, remote fetch fallback second.
- Bulk missing-object collection and fetch, then retry from local store.
- Unified status engine for full staging, filtered staging, diff, and patch.
- Explicit object states: `loaded`, `indexed_needs_load`, `not_buffered`, `unknown_needs_fetch`, `confirmed_absent`, `corrupt_or_incomplete`.

## Investigation log
- The standard stage flow now checks the Ortec switch inside `zcl_abapgit_stage_logic=>zif_abapgit_stage_logic~get`.
- The diff page uses the same pattern in `zcl_abapgit_gui_page_diff_base=>get_files_and_status`.
- Both entry points call `zcl_abapgit_ortec_filter_walk` for filtered remote file resolution and fall back to `ii_repo_online->get_files_remote(...)` whenever the Ortec preconditions are not met.
- The filtered helper validates repo state, selected branch, selected commit, and branch-tip consistency before it uses `zcl_abapgit_ortec_obj_index=>get_files_for_filter`.

## Historical fast path findings (superseded by archaeology pass — see below)
- The original fast path was a cache-backed reconstitution layer that aimed to avoid a full remote GET when the persistent store already had reachable objects for the branch tip.
- It stored and re-used state through `ZAOG_OBJ_STORE`, `ZAOG_REPO_STATE`, `ZAOG_OBJ_INDEX`, and the pack/session tables.
- The core risk in the historical design was that a cache miss or an incomplete tree could silently look like a successful fast path and then break later in walk/patch logic.
- **Archaeology refinement (2026-07-10)**: for the Stage/Diff/Patch *read* path specifically (as opposed to the pull/fetch *write* path below), the pre-`e61fcb11` behavior was to call `ZCL_ABAPGIT_ORTEC_FILTER_WALK` **unconditionally** whenever an object filter was present, relying only on its own data-validity checks (repo_key resolvable, `fetch_commit` cached, branch tip unchanged) to decide fast vs. fallback — see `.memory/diagrams/historical_fast_path.mmd` and `.memory/logs/archaeology.md`.

## Current slow/correct path findings
- `zcl_abapgit_git_porcelain=>pull_by_branch` is the main orchestrator for remote pulls. It tries `zcl_abapgit_ortec_fastpath=>pull_by_branch` first, then falls back to `zcl_abapgit_git_transport=>upload_pack_by_branch`, then runs `pull(...)` to materialize files.
- `zcl_abapgit_git_transport=>upload_pack_by_branch` and `upload_pack_by_commit` route to the Ortec fast path when the repo switch is active. Otherwise they use the standard upload-pack flow.
- `zcl_abapgit_git_porcelain=>walk` and `walk_tree` are the places where a missing tree or blob can surface as `Walk, tree not found`, `Walk, blob not found`, or `tree not found`. They consult the persistent object store as a repair path before raising.
- `zcl_abapgit_ortec_repo_state=>reset_fetch_commit` and `invalidate_tip_commit` are the explicit repair hooks used when the cache is incomplete or stale, especially after branch switches or a partial fetch.
- `zcl_abapgit_ortec_fastpath=>persist_pull_result` writes finished fetch results into `ZAOG_OBJ_STORE` and updates `ZAOG_REPO_STATE`/`ZAOG_COMMIT_HIST` so later pulls can reuse the objects.
- The UI hooks for branch switching and virtual stage rendering are `zcl_abapgit_ortec_branch_list`, `zcl_abapgit_ortec_git_stage`, and `zcl_abapgit_ortec_git_patch`.
- **Archaeology refinement (2026-07-10)**: for Stage/Diff/Patch, the *current* fork point is `ZCL_ABAPGIT_ORTEC_GIT_SWITCH=>IS_ACTIVE_FOR_REPO(url)`, checked in `zcl_abapgit_stage_logic~get` and `zcl_abapgit_gui_page_diff_base~get_files_and_status` **before** the filtered walk is even attempted. This flag is a per-user/per-repo persisted opt-in (`zcl_abapgit_persistence_ortec=>get_repo_use_cache`) that defaults to `abap_false` unless a user has explicitly ticked "Use Persistent Object Cache" in Repository Settings. See `.memory/diagrams/current_slow_path.mmd` and `.memory/logs/archaeology.md` for full evidence.

## Target architecture findings
- The intended runtime shape is a thin standard hook layer plus an Ortec fast-path layer that owns persistence, branch-state validation, and filtered object indexing.
- The slow but correct path is still the fallback from `zcl_abapgit_ortec_filter_walk` to `ii_repo_online->get_files_remote(...)`, and from the Ortec fast path to the standard `upload_pack` + `pull` path when the store state is incomplete.

## Changed objects
- `src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap` (Phase 1: removed read-path opt-in gate; Phase 4: passes `iv_url` into `get_files_for_filter`)
- `src/repo/stage/zcl_abapgit_stage_logic.clas.abap` (Phase 1 gate removal + Phase 3 facade/it_remote rewrite, ping-pong removed)
- `src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap` (Phase 1 gate removal + Phase 3 facade/it_remote/it_local rewrite, ping-pong removed)
- `src/git/zcl_abapgit_git_porcelain.clas.abap` (Phase 1: walk_tree repo_key fix)
- `src/git/zcl_abapgit_git_porcelain.clas.testclasses.abap` (Phase 1: new walk_tree_repo_key_isolation regression test)
- `src/repo/zcl_abapgit_repo_status.clas.abap` (Phase 3: new optional `it_remote` seam on `calculate`)
- `src/ortec/git/zcl_abapgit_ortec_git_facade.clas.abap` (Phase 3: NEW - single filtered-remote entry point)
- `src/ortec/git/zcl_abapgit_ortec_git_facade.clas.xml` (Phase 3: NEW class metadata)
- `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap` (Phase 4: new `get_missing_sha1s` set-based bulk-missing check)
- `src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap` (Phase 4: NEW - bulk missing-object collector with write-opt-in safety gate)
- `src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.xml` (Phase 4: NEW class metadata)
- `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap` (Phase 4: `get_files_for_filter`/`build_files_from_rows` gained optional `iv_url`/`iv_commit`, best-effort top-up fetch before existing miss-handling)
- `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap` (Phase 4: new `missing_sha1s_none`/`missing_sha1s_some` tests + new `ltcl_missing_objects` safety-gate tests; Phase 4b: new `object_state_constants`/`absent_strictness_default` constant tests + new `ltcl_obj_index` class with `marker_required_for_ready` self-heal test)
- `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap` (Phase 4b: new `ty_object_state`/`cs_object_state` six-state vocabulary, additive only)
- `src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap` (Phase 5b.1: new `has_dangling_delta_base` chunked set-based completeness check)
- `src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap` (Phase 4b: new `cs_absent_strictness` D4 STRICT/RELAXED switch, additive only)
- `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap` (Phase 4b: fixed a latent `is_index_ready`/`rebuild_index` completeness-marker bug gated on `cs_absent_strictness-mode`; improved `build_files_from_rows` diagnostic to use `CORRUPT_OR_INCOMPLETE`; removed dead `lv_row_count`)
- `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap` (Phase 5b.1: `is_index_ready` moved from PRIVATE to PUBLIC for completeness-gate reuse; visibility-only change)
- `src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap` (Phase 4b: doc-comment-only tie-in to the six-state vocabulary, no functional change)
- `abaplint.json` (unrelated pre-existing tooling bug fixed: stripped invalid `(?i)` inline regex flags blocking all lint runs)
- `src/ortec/git/zcl_abapgit_ortec_delta.clas.abap` (Phase 5a: NEW - unified REF+OFS delta resolver, decode-only/dead code until Phase 5b; commit `b6b8372a`: added defensive `try/catch` wrappers in `get_offset`/`skip_size_header`/`apply` converting unexpected `cx_sy_range_out_of_bounds` to declared `zcx_abapgit_exception`)
- `src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap` (Phase 5a: `get_type` gained `OBJ_OFS_DELTA` support; `resumable_decode` now tracks pack-offset/base-offset and resolves deltas via `zcl_abapgit_ortec_delta=>resolve_all` instead of standard `decode_deltas`; Phase 5b.2: `decode_and_persist` catch path now does full temp/index/meta/raw/session cleanup + `COMMIT WORK` before re-raise)
- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap` (commit `b6b8372a`: additive exception-handling guards around pkt-line capability parsing in `fetch_tip_commits` and `parse`; plus real pkt-line length validation fix for invalid non-flush lengths 1-3; Phase 5b.2: `upload_pack` gained `iv_allow_thin` and verified-have-gated thin/ofs advertisement; branch/commit upload now do thin->non-thin retry and final fallback-triggering re-raise)
- `src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap` (Phase 5b.1: new `is_commit_complete` and `get_verified_have_commits` completeness-gate methods)
- `src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap` (Phase 5a: new `ltcl_ofs_delta` test class; Phase 5b.1: new `ltcl_completeness_gate` with 4 test methods; Phase 5b.2: new `ltcl_pack_decoder=>cleanup_after_decode_failure` regression test asserting zero residual rows in `zaog_obj_store`/`zaog_pack_idx`/`zaog_pack_meta`/`zaog_raw_pack`)

## Tests and regression status
- Phase 1, Phase 3, and Phase 4 implemented, lint-clean (0 new abaplint issues beyond one deliberate style match to pre-existing test-file convention; verified via targeted diffs against baseline).
- New ABAP Unit tests added and syntax-verified: `walk_tree_repo_key_isolation` (Phase 1), `missing_sha1s_none`/`missing_sha1s_some`/`ltcl_missing_objects` safety-gate tests (Phase 4).
- Independent regression subagent validated all three phases: PASS_WITH_NOTES, no hard-stop violations (see regression_phase1.md, regression_phase3.md, regression_phase4.md).
- Phase 1 + Phase 3 changes confirmed compiled successfully on real SAP system IT8 (Michael, 2026-07-11).
- Residual gap: full `npm run unit` ABAP-to-JS transpile+execute harness is blocked by PRE-EXISTING, UNRELATED dependency drift against freshly cloned open-abap-core/open-abap-gui (zcl_abapgit_hash.clas.abap EF_HASHSTRING, zcl_abapgit_cts_integration.clas.abap subtractsecs_to_short, zif_abapgit_cts_api prefetch_descriptions). Confirmed unrelated to all Ortec changes. Not fixed (out of scope). Real-system IT8 compilation now covers Phase 1+3; Phase 4 still awaits the same real-system confirmation.
- Known minor trade-off (non-blocking, documented in regression_phase4.md): `get_files_for_filter`'s stale-index rebuild-and-retry path can invoke the new bulk-fetch top-up a second time in a rare double-failure scenario; bounded at 2 attempts total, never unbounded, never incorrect.
- Phase 4 scope note: implemented the concrete, high-value "bulk missing-object collection" piece for the FILTERED Stage/Diff read path (`zcl_abapgit_ortec_obj_index`). Deliberately left `zcl_abapgit_git_porcelain`'s `walk`/`walk_tree` and their existing `pull_by_branch` self-heal untouched beyond Phase 1's repo_key fix, since that repair path already works correctly and reworking it risked exceeding the D7 minimal-touch budget for no added benefit. The six-state model (`LOADED`/`INDEXED_NEEDS_LOAD`/`NOT_BUFFERED`/`UNKNOWN_NEEDS_FETCH`/`CONFIRMED_ABSENT`/`CORRUPT_OR_INCOMPLETE`) and the D4 `cs_absent_strictness` switch remain deferred to a future phase (not yet needed by any concrete implemented behavior; avoids empty-abstraction scaffolding, consistent with the Phase 3 `zcl_abapgit_ortec_status_engine` deferral decision).
- Phase 4b (2026-07-11): implemented the deferred six-state model (`zcl_abapgit_ortec_obj_store=>cs_object_state`) and the D4 `cs_absent_strictness` STRICT/RELAXED switch (`zcl_abapgit_ortec_git_switch`). While reasoning through the completeness requirements, found and fixed a genuine latent bug: `zcl_abapgit_ortec_obj_index=>is_index_ready` treated "any row exists" as "index complete", which is also true for an index left behind by a rebuild interrupted partway through (the `$IDX/__READY__` marker was previously only written when zero rows were found). Fixed by always writing the marker on a fully successful rebuild and gating the readiness check on STRICT (marker required, default) vs RELAXED (old behavior, benchmark-only, never ships as default). New tests: `object_state_constants`, `absent_strictness_default`, and `ltcl_obj_index=>marker_required_for_ready` (builds a real commit/tree/blob graph and proves STRICT mode self-heals a corrupted/marker-less index). Lint-verified via before/after abaplint diff on just the touched files (per-rule occurrence counts, not raw line diff): only pre-existing SQL-style-convention deltas remain, plus two genuinely new findings (`unused_variables`, `use_new`) that were found and fixed. No standard abapGit code touched. Full record: [.memory/handoffs/implementation_phase4b.md](.memory/handoffs/implementation_phase4b.md).
- Phase 5a (2026-07-11): implemented decode-only `OBJ_OFS_DELTA` support in Ortec code (`zcl_abapgit_ortec_delta` + `zcl_abapgit_ortec_pack_dec` integration) plus new `ltcl_ofs_delta` tests; independently regression-validated PASS_WITH_NOTES with offset-varint math, dependency-ordered chain resolution, fail-safe error handling, and dead-code reachability traced and confirmed (see [.memory/logs/regression_phase5a.md](.memory/logs/regression_phase5a.md)).
- Phase 5b.2 (2026-07-12): implemented capability negotiation and fail-safe cascade wiring plus decode-failure cleanup (`zcl_abapgit_ortec_fastpath` + `zcl_abapgit_ortec_pack_dec`) and added `cleanup_after_decode_failure` regression coverage in `ltcl_pack_decoder`; independently regression-validated PASS_WITH_NOTES with the expected 3-tier fallback chain (thin Ortec -> non-thin Ortec -> standard), verified-complete-have advertisement gate, and catch-scoped cleanup semantics confirmed (see [.memory/logs/regression_phase5b2.md](.memory/logs/regression_phase5b2.md)).

## Open questions for Michael
- Which branch-tip states should be treated as `confirmed_absent` versus `unknown_needs_fetch` in the filtered stage/diff flow?
- Should the repair path reuse `ZAOG_OBJ_INDEX` entries even when the object store is incomplete for a single commit?
- Phase 5 direction: implement OBJ_OFS_DELTA decode + enable thin-pack negotiation (larger, higher-risk, real bandwidth win) vs. skip thin-pack for now and do a smaller/safer slice instead (Phase 6 admin report, the deferred unified status engine, or a targeted fix to the existing missing-delta-base full-store-reload fallback)? See [.memory/logs/phase5_findings.md](.memory/logs/phase5_findings.md).
- ATC follow-up (updated 2026-07-12): **Done.** Ran a full ATC sweep (default check variant) across all 20 `zcl_abapgit_ortec_*`/`zcx_abapgit_ortec_git` classes plus the new `ZABAPGIT_ORTEC_CACHE_ADMIN` report, now that arc-1 connectivity is confirmed working. See "Phase 7" bullet above for the full outcome, including a genuine bug the first delegated ATC pass missed (`zcl_abapgit_git_transport=>upload_pack_by_commit` had no exception handling around the Ortec cascade call at all) that I found via independent spot-checking and fixed directly.
- **Last open item (2026-07-12):** all planned implementation phases (1 through 7) are complete. The only thing left to fully close out the project is an empirical large-repo (>=5k objects) filtered Stage/Diff latency benchmark on a real system, to confirm the Phase 7 target (>=50% reduction for Phase 1, or >=2x for the full Phase 1-6 rollout) - this needs Michael's live environment, since no such benchmark harness is available in this session.

## Design Review Status
- Last review file: `.memory/reviews/design_review.md`
- Verdict: APPROVE_WITH_MINOR_REVISIONS
- Auto-iteration used: no
- Blocking issues: none at design-decision level; D7 moved into Phase 1
- Next action: begin Phase 1 implementation

## Archaeology phase findings (2026-07-10)

### Repo/commit topology
- `ortec/abapgit_1_133-opt-rework` HEAD == `ortec/abapgit_1_133-optimized` HEAD ==
  `origin/ortec/abapgit_1_133-optimized` == baseline `b4f41e38372a0fe9f67483f71e968b1885b594c1`.
- Working tree has no uncommitted ABAP source diffs. **No commit-level or working-tree
  diff exists between "historical" and "current"** — the regression had to be found by
  tracing conditional fast/fallback branches inside the current source, not by diffing
  two states (per task instructions on missing commit deltas).
- One unrelated stash exists (`stash@{0}` "Prefetch in Parallel Processing", based on
  `8d3de207`) — serializer prefetch work, out of scope for this regression, left
  untouched.

### Direct answer to "what changed to make performance gains disappear"
Commit `e61fcb11` ("Attach Filtered Tree Walk Optimization to Fastpath Switch",
2026-06-23 08:31 UTC) and its sibling `64d4e9e1` ("Add Filtered Walk in Base Diff
Calculation", 2026-06-23 11:07 UTC) — both already ancestors of, and included in, the
current HEAD/baseline `b4f41e38` — made the read-only, non-mutating, self-validating
sparse remote-file lookup (`ZCL_ABAPGIT_ORTEC_FILTER_WALK`, used by Stage/Diff/Patch)
conditional on `ZCL_ABAPGIT_ORTEC_GIT_SWITCH=>IS_ACTIVE_FOR_REPO` — the same per-user/
per-repo opt-in flag used to gate the unrelated, *mutating* write-side pull/persist
cache (`ZCL_ABAPGIT_ORTEC_FASTPATH`). That flag defaults to `abap_false` for any
repo/user without an explicit "Use Persistent Object Cache" opt-in, and silently
resolves to `abap_false` on any persistence-read exception. Before this change, the
filtered walk was attempted unconditionally and had its own correct data-validity
fallback (repo_key resolvable → fetch_commit cached → branch tip unchanged → sparse
index lookup, else full retrieval). After this change, the vast majority of real usage
(no explicit opt-in) takes the exact same full `get_files_remote()` path that existed
before any Ortec optimization work — silently, with no error or log signal. Full
evidence chain, code excerpts, and file/line references: `.memory/logs/archaeology.md`.

### Classification summary (full table with evidence in archaeology.md)
- Required, must be preserved: write-side `is_active_for_repo` gate on
  `zcl_abapgit_ortec_fastpath` (3e964850/81345469); `zcl_abapgit_ortec_filter_walk`'s
  own data-validity fallback chain; `fe1e94a8` DEVC path-filtering / sub-package /
  multi-filter correctness fixes.
- Cached/sparse retrieval replaced by standard full retrieval + accidental performance
  regression: `e61fcb11`'s new `is_active_for_repo` guard placed first inside
  `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`.
- Cache invalidation/bypass regression: `e61fcb11`/`64d4e9e1` wrapping the
  `CALL METHOD ('ZCL_ABAPGIT_ORTEC_FILTER_WALK')` call sites in `zcl_abapgit_stage_logic`
  and `zcl_abapgit_gui_page_diff_base` with the same opt-in-defaulting-to-off flag.
- Workaround needing narrower fix: conflating the read-only rendering optimization with
  the write-side persistent-cache opt-in switch under one flag; narrower fix is a
  separate flag (or none) for the read-only filtered walk.
- Unrelated: `b4f41e38` itself (pure client-side JS/DOM rendering for the Patch page,
  confirmed via full diff vs. parent); the stashed "Prefetch in Parallel Processing"
  work (different subsystem — local object serialization).

### Residual uncertainty / assumptions
- Assumption: the "current slow version" Michael observes is this same checked-out
  source running with the opt-in flag unset/unreadable for the affected repo(s) —
  consistent with all source evidence, but not confirmed via a live system trace
  (`SAPRead`/ADT connectivity returned a network error when probed in this session) or
  by directly asking whether "Use Persistent Object Cache" is ticked for the affected
  repositories.
- Not verified: whether any specific repository in the live environment currently has a
  persisted `use_cache = 'X'` row (that repo/user combination would still see the fast
  path today).
- Next action for a future phase: confirm the opt-in flag's state for the affected
  repositories on the live system, then decide the narrow fix (decouple the read-only
  filter-walk gate from the write-side fastpath switch) as a design-phase proposal —
  no ABAP change made in this archaeology pass.

## Design phase plan & risk (2026-07-10, model: Claude Opus 4.8)

Full design: `.memory/logs/target_design.md`. Target diagram:
`.memory/diagrams/h4_target_architecture_legacy.mmd`. Open decisions:
`.memory/decisions/h4_design_decisions_d1_d7.md`.

### Core design decision
Split **policy** from **data availability**. The read-only filtered walk (Stage/Diff/Patch)
is governed ONLY by data validity (repo_key → fetch_commit → tip match → index READY), never
by the write-side `is_active_for_repo` opt-in. Protocol-altering writes (have suppression,
thin packs, delta-only fetch) stay behind the conservative opt-in.

### Object/path states (boolean found/not-found abolished)
`LOADED`, `INDEXED_NEEDS_LOAD`, `NOT_BUFFERED`, `UNKNOWN_NEEDS_FETCH`, `CONFIRMED_ABSENT`,
`CORRUPT_OR_INCOMPLETE`. Deleted classification allowed ONLY on `CONFIRMED_ABSENT`
(= remote tip + commit + parent tree + path all positively resolved).

### New Ortec entry points
`zcl_abapgit_ortec_git_facade` (single filtered-remote entry, no ping-pong),
`zcl_abapgit_ortec_status_engine` (unified stage/diff/patch classification),
`zcl_abapgit_ortec_missing_obj` (bulk collect → 1 fetch → persist → 1 retry).
`zcl_abapgit_ortec_porcelain` (D7 mirror, **conditional** — only if standard-porcelain
minimal-touch budget is exceeded). `zcl_abapgit_ortec_cache_admin` (D5, Phase 6 — backing class
for admin report `ZABAPGIT_ORTEC_CACHE_ADMIN`, off the hot path).
Refactor: `filter_walk` (drop inner gate), `git_switch` (split flags + D4 `cs_absent_strictness`
and D6 `cs_tip_validation` benchmark constants), `obj_index`/`obj_store`/`fetch_neg` (extend).
Standard code stays tiny hooks only; one new seam: optional `it_remote` on
`zcl_abapgit_repo_status=>calculate`.

### Schema implications
Reuse `$IDX/__READY__` marker as per-commit completeness signal. NEW: delta-base edges
(cols on `ZAOG_PACK_IDX` or new `ZAOG_DELTA`) for thin-pack safety + eviction safety;
verify secondary index on `ZAOG_OBJ_INDEX(repo_key,commit_sha1,obj_type,obj_name)`;
retention metadata (`last_used_ts`) for eviction. No `ZAOG_REPO_STATE` semantic change.

### Phased plan (correctness-first, smallest-diff-first)
0. Confirm runtime hypothesis on live system (no code).
1. Decouple read gate = the regression fix (no schema change). **MVP.** Fix `walk_tree`
  repo_key at the same time because it is a correctness bug.
2. Safe self-population of store stays behind the opt-in (D1 = Option B, strict separation).
3. Facade + status engine + remove set/refresh ping-pong.
4. Explicit states + bulk missing-object collect/retry.
5. Delta-base completeness + protocol hardening (thin packs, have/want).
6. Large-repo index/schema optimisation + eviction.
7. Regression (agent 05) then performance (agent 06) validation, with explicit success criteria
  from the design doc.

### Top risks
- Read decoupled but store empty → no visible gain for non-opted-in repos (accepted under
  D1 Option B; gains apply to repos that opted into the fastpath; revisit ungated population
  only after production-proven).
- Store growth on huge repos (mitigate: D5 = manual clear default + optional admin report
  `ZABAPGIT_ORTEC_CACHE_ADMIN`; any compaction is delta-base-safe).
- Thin-pack without complete bases → `Walk,` corruption (mitigate: D3 delta index).
- Standard `zcl_abapgit_git_porcelain` churn / upstream-rebase conflicts (mitigate: D7
  minimal-touch budget, else `zcl_abapgit_ortec_porcelain` mirror).
- **Misclassifying `NOT_BUFFERED` as deletion = data-loss-grade bug** (mitigate: hard rule,
  Deleted only on `CONFIRMED_ABSENT`; D4 broadens the completeness guard to all statuses,
  enforced in status engine + collector).

### Blocking decisions for Michael
**All of D1–D7 are resolved by the owner (2026-07-10); none remain blocking.**
- D1 = Option B (strict opt-in): store read/written only when the fastpath opt-in is ON.
- D3 = implement the delta-base index (thin-pack + eviction safety).
- D4 = two runtime modes via a switch constant (`STRICT` default / `RELAXED` benchmark) +
  broad stale-data protection across **all** statuses (not just remote-Deleted).
- D5 = manual clear default + optional admin report `ZABAPGIT_ORTEC_CACHE_ADMIN` (Phase 6).
- D6 = per-op tip validation default + switchable short-TTL mode (`cs_tip_validation`) for
  latency benchmarking.
- D7 = minimal-touch standard `zcl_abapgit_git_porcelain` first; if the change exceeds the
  §8.0 budget, route through the Ortec mirror `zcl_abapgit_ortec_porcelain`.
The plan is implementation-ready; per the design-mode gate, coding begins only on Michael's
explicit go-ahead. Recommended path: Phase 0+1 first (safe regression fix), then Phases 2–6.

### Design review status
- Verdict: APPROVE_WITH_MINOR_REVISIONS.
- No productive ABAP changed. No transports created.

### Phase 1 implementation update (2026-07-11)
- Removed the read-path opt-in gate from filtered remote retrieval in `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`; fallback behavior to `get_files_remote` remains unchanged.
- Removed standard-stage and diff-page pre-gates (`is_active_for_repo`) so filtered walk is always attempted first with safe fallback on any error.
- Fixed `zcl_abapgit_git_porcelain=>walk_tree` wrong-store risk by passing `iv_repo_key` through the method signature/recursive call and using it in `zcl_abapgit_ortec_obj_store=>get_object`.
- This stays within D7 minimal-touch intent: no broad control-flow rewrite and no new Ortec mirror routing yet.
