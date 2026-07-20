# Fastpath isolation audit — ORTEC vs. standard abapGit crossover points

- Date: 2026-07-14
- Scope: read-only discovery of the "fastpath active" call path (pull/fetch and filtered
  Stage/Diff read), validating three isolation assumptions:
  - (a) Git protocol/communication should stay in `zcl_abapgit_ortec_*` except low-level HTTP
  - (b) `ZAOG_*` buffering-table reads/writes should stay in `zcl_abapgit_ortec_*`
  - (c) pack/object decode and processing should stay in `zcl_abapgit_ortec_*`
- Companion diagram: [.memory/diagrams/fastpath_active_call_trace.mmd](../diagrams/fastpath_active_call_trace.mmd)
- Produced by: `ortec-abapgit-discovery` subagent (initial pass) + orchestrator direct
  source verification (exact line numbers, exact control flow, and severity assessment).

## #1 — CONFIRMED VIOLATION (highest priority): silent fallback to standard pack decode inside `upload_pack`

- File/method: `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`, `METHOD upload_pack` (L590-779).
- Exact mechanism (verified against source, both `iv_allow_thin = abap_true` and
  `iv_allow_thin = abap_false` attempts run this *identical* logic):
  1. Pack bytes are fetched over HTTP (`io_client->send_receive_close`) — this part is fine
     per assumption (a), it is genuinely low-level transport.
  2. `IF zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_true` and a repo
     key resolves, the method calls `zcl_abapgit_ortec_pack_dec=>decode_and_persist(...)`
     inside a `TRY ... CATCH zcx_abapgit_exception.` block. **The CATCH body is empty** — no
     re-raise, no `MESSAGE`, no log entry, nothing. If `decode_and_persist` raises for *any*
     reason, execution simply falls through past the `ENDTRY.`.
  3. Immediately after that `TRY` block (L775-779), with **no guard at all**:
     ```abap
     zcl_abapgit_ortec_obj_store=>set_active_repo_key( lv_ortec_rk ).
     rt_objects = zcl_abapgit_git_pack=>decode( lv_pack ).
     ```
     This unconditionally calls the **standard** `zcl_abapgit_git_pack=>decode` on the exact
     same pack bytes, whenever step 2 didn't already `RETURN` with Ortec-decoded objects.
- Classification: **isolation violation of assumption (c)**. Decode/processing of the fetched
  pack is not staying inside `zcl_abapgit_ortec_*`; it silently reverts to the standard,
  slower decoder (`zcl_abapgit_git_pack` → `zcl_abapgit_zlib` for the 0x7801 zlib-header case)
  while the fastpath switch is reported as active, with **no visible error, warning, or log**.
- Why this matches the live SAT-trace observation: when the fallback fires and the pack
  objects use the `0x7801` zlib header (common with some git servers; `cl_abap_gzip` only
  reliably handles `0x789C`), `zcl_abapgit_git_pack=>decode` cannot use the fast kernel path
  and must invoke the slow, pure-ABAP `zcl_abapgit_zlib=>decompress`/`decode_loop_fast` for
  **every single object in the pack**. This exactly matches the SAT trace evidence (1,068
  `ZLIB_DECOMPRESS`/`DECODE_LOOP_FAST` calls dominating the 264-second gross runtime), and it
  explains why the call originated directly from `ZCL_ABAPGIT_ORTEC_FASTPATH` rather than from
  `zcl_abapgit_git_transport`'s documented standard-fallback tier.
- Important nuance vs. the documented Phase 5b.2 "3-tier cascade" (thin Ortec → non-thin
  Ortec → standard): the design intended the *third* tier ("standard") to be a genuinely
  separate re-negotiation reached only after **both** Ortec attempts raise
  `zcx_abapgit_ortec_git`, caught by `zcl_abapgit_git_transport`'s outer
  `CATCH zcx_abapgit_ortec_git`. What is actually implemented is different: **both** of the
  first two tiers (thin attempt and non-thin retry) each have their *own* embedded,
  undocumented standard-decode-of-the-same-bytes sub-fallback baked directly into
  `upload_pack`. When that sub-fallback happens to succeed (i.e. the pack turns out to be
  fully self-contained / non-ofs), the method returns normally with **no exception at all**,
  so the outer `upload_pack_by_branch`/`upload_pack_by_commit` retry logic and
  `zcl_abapgit_git_transport`'s standard-fallback catch never even see that anything went
  wrong — from their perspective, "the fastpath succeeded".
- **Root-cause note — the real severity is unknown without further instrumentation.** Because
  step 2's `CATCH zcx_abapgit_exception.` is silent, there is currently no way to distinguish,
  from logs, between:
  - an **expected** case (the thin/ofs attempt's pack genuinely can't be parsed by
    `decode_and_persist` for a benign, protocol-related reason and the plain standard decode
    of the same bytes then happens to succeed because the pack wasn't actually thin), vs.
  - a **genuine bug** in `zcl_abapgit_ortec_pack_dec=>decode_and_persist` (e.g. a delta-base
    resolution error, a lock/DB issue in `acquire_repo_lock`, an OFS chain-depth or resolver
    defect) that is being silently masked every time, causing the ORTEC fastpath to
    systematically degrade to the slow standard decoder far more often than intended.
  Recommend adding a diagnostic (e.g. log `lx_error->get_text( )` before falling through) as
  the safe, minimal first step before deciding on a permanent fix — the correct fix depends on
  *why* `decode_and_persist` is failing in the live system.

## #2 — By-design fallback: filtered Stage/Diff read

- File/method: `src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap`,
  `get_remote_files_for_stage`.
- Behavior: when repo-key resolution, cached branch-tip validation, or the index lookup
  fails, the method falls back to `ii_repo_online->get_files_remote( ii_obj_filter )`
  (standard).
- Classification: **intentional, documented correctness fallback** — governed purely by data
  validity (per the target design's "never treat missing/stale local data as a hard failure"
  rule), not a switch/policy decision. Not a violation.

## #3 — By-design cascade: transport-layer fastpath-to-standard fallback

- File/method: `src/git/zcl_abapgit_git_transport.clas.abap`,
  `upload_pack_by_branch`/`upload_pack_by_commit`.
- Behavior: `IF is_active_for_repo(...) TRY zcl_abapgit_ortec_fastpath=>upload_pack_by_branch/
  commit(...) CATCH zcx_abapgit_ortec_git` → standard upload_pack + standard
  `zcl_abapgit_git_pack=>decode`.
- Classification: **intentional, documented fallback tier** (the genuine "tier 3" of the
  Phase 5b.2 cascade). Correctly scoped to `zcx_abapgit_ortec_git` only, matching the design.
  Not itself a violation — but see #1: this tier is effectively *starved* because `upload_pack`
  rarely lets a genuine `zcx_abapgit_ortec_git`/`zcx_abapgit_exception` propagate all the way
  out to it (its own thin/non-thin sub-fallback usually absorbs the failure first).

## #4 — Reverse-direction crossover (standard code calling into Ortec for DB read)

- File/method: `src/git/zcl_abapgit_git_porcelain.clas.abap`, `walk`/`walk_tree`, and
  `src/git/zcl_abapgit_git_delta.clas.abap`, `delta` (ref-delta base fallback).
- Behavior: standard porcelain/delta code calls `zcl_abapgit_ortec_obj_store=>get_object(...)`
  directly when a tree/blob/delta-base is missing from the in-memory pack objects.
- Classification: **not a violation of (a)/(c)** — this is standard code calling *into*
  Ortec for a `ZAOG_*` DB read, the opposite direction from the audited assumption. It is a
  deliberate, already-documented repair path (see `.memory/logs/incident_branch_switch_walk_failure.md`).
  Flagged here for completeness only.

## Summary for the orchestrator

1. **The one real, confirmed isolation violation** is #1 above: `zcl_abapgit_ortec_fastpath=>
   upload_pack` silently falls back to standard `zcl_abapgit_git_pack=>decode` on every
   `decode_and_persist` failure, with no logging, and this happens on *both* cascade tiers
   before the documented standard-fallback tier is ever reached. This is almost certainly the
   direct cause of the live-observed standard ZLIB decode.
2. Everything else found (#2, #3, #4) is an intentional, already-documented fallback/guardrail
   and not a violation.
3. Recommended next step (not yet done — this is discovery only): add temporary diagnostic
   logging around the swallowed `CATCH zcx_abapgit_exception` in `upload_pack` to capture the
   real failure reason from `decode_and_persist` on the next live run, before deciding whether
   the fix is (i) a bug fix inside `decode_and_persist`, or (ii) tightening the fallback so it
   only silently substitutes standard decode for the specific "pack wasn't actually thin"
   case and re-raises (visibly) for anything else.
