# Phase 5 review required — OFS_DELTA decode + thin-pack negotiation

- **Phase:** design-detail (ortec-abapgit-design, Claude Opus 4.8)
- **Date:** 2026-07-11
- **Status:** ALL DECISIONS RESOLVED (2026-07-11) — Michael accepted all seven design recommendations (D-P5-1 through D-P5-7). Implementation may proceed starting with Phase 5a (decode-only) per the strict rollout order; Phase 5b (capability negotiation) may not begin until Phase 5a's unit tests are green.
- **Companion:** `.memory/logs/target_design_phase5.md`, `.memory/diagrams/phase5_ofs_delta_flow.mmd`
- **Ground truth (do not re-derive):** `.memory/logs/phase5_findings.md`
- **Prior decisions still in force:** D1 = Option B strict opt-in; D3 = delta-base index used to
  allow thin only when base completeness verified; D7 = minimal-touch standard porcelain else
  Ortec mirror. (See `design_review_required.md`.)

---

## D-P5-1 — OFS type constant location + standard error message *(low stakes)*
**Question:** Put the git type-6 constant on the shared `zif_abapgit_git_definitions=>c_type`
(`ofs_d`), or keep it Ortec-local on `zcl_abapgit_ortec_delta` (`c_type_ofs_d`)?
- **Context:** ofs is a live path only on the Ortec opt-in path (D1 Option B); standard code
  never needs it.
- **Recommendation:** **Ortec-local** — touches zero standard files (strongest D7 outcome).
  Optionally reword the standard `Todo, unknown git pack type` message (L688 pack / L1253 Ortec)
  to name ofs-delta for diagnosability; cosmetic, no behaviour change.
- **Correctness stakes:** none.
- **Michael decision:** ACCEPTED — Use an Ortec-local constant with zero standard files touched; leave optional cosmetic error message rewording as-is for now.

## D-P5-2 — Completeness gate strength before advertising thin-pack *(HIGH stakes)*
**Question:** Is the per-commit `$IDX/__READY__` marker (Phase 4b) sufficient to declare a
`have` commit safe as a thin-pack base source, or must we **also** scan for dangling
`ZAOG_PACK_IDX.DELTA_BASE` among the commit's reachable objects (i.e. every stored delta's base
is itself present as `status='R'`) before advertising thin?
- **Context:** A thin pack lets the server delta the response against objects we claim to
  `have`. If any of those bases is actually missing/incomplete locally, the response's deltas
  become unresolvable → `Walk,`-class corruption. The marker proves the *index/tree* is
  complete; it does **not** by itself prove every *delta base* is present.
- **Recommendation:** **Both** — marker (condition 5.1.2) **and** a set-based
  no-dangling-delta-base scan (5.1.3, `zcl_abapgit_ortec_obj_store=>has_dangling_delta_base`).
  Correctness-first; the scan is one set-based SELECT, off the hot path (only runs when building
  the have set for a thin fetch).
- **Correctness stakes:** HIGH — marker-only could advertise an incomplete base and corrupt.
- **Michael decision:** ACCEPTED — Require both the marker and the no-dangling-delta-base scan before advertising thin-pack.

## D-P5-3 — Byte-apply reuse: expose public standard `apply` vs Ortec-owned copy *(medium)*
**Question:** Reuse the byte copy/insert algorithm by exposing a public
`zcl_abapgit_git_delta=>apply( iv_base, iv_delta )` (small standard refactor, less duplication),
or copy the ~20-line loop into `zcl_abapgit_ortec_delta` (zero standard touch, ~20 duplicated
lines)?
- **Context:** `delta()` is private and coupled to `ct_objects` + SHA lookup — not a clean
  primitive. The algorithm is tiny and spec-stable.
- **Recommendation:** **Ortec-owned copy** (per D7 minimal-touch; unit-pinned by test T2 which
  also exercises the standard path, so drift is caught).
- **Correctness stakes:** low (both are pinned by identical exact vectors).
- **Michael decision:** ACCEPTED — Use an Ortec-owned copy of the byte-apply algorithm rather than a standard refactor.

## D-P5-4 — Fail-safe cascade shape: 3-try vs 2-try *(minor)*
**Question:** On a thin/ofs decode failure, retry **thin → non-thin Ortec → standard** (3 tries,
keeps the Ortec have/deepen bandwidth win on the retry), or **thin → standard** (2 tries,
simpler)?
- **Recommendation:** **3-try** — the non-thin Ortec retry preserves the have-based bandwidth
  optimisation and only falls all the way back to standard on a second failure.
- **Correctness stakes:** none (all paths end at a safe self-contained decode).
- **Michael decision:** ACCEPTED — Use the 3-try cascade thin/ofs Ortec to non-thin Ortec to standard.

## D-P5-5 — Advertise `ofs-delta` without `thin-pack`? *(scope boundary)*
**Question:** Should `ofs-delta` also be advertised on non-thin Ortec fetches (e.g. the first
`deepen` fetch and the `filter tree:0` commits-only path), giving intra-pack compactness even
without thin, or keep ofs strictly coupled to thin for now?
- **Context:** ofs-delta alone (no thin) still lets the server use compact intra-pack deltas.
  But it also makes ofs a live path on more code paths, each needing the resolver + cascade.
- **Recommendation:** **Keep coupled to thin for now** (advertise `thin-pack ofs-delta`
  together, only on `upload_pack`). Treat ofs-without-thin as a separate future optimisation
  once thin+ofs is production-proven. Explicitly out of Phase 5 scope.
- **Correctness stakes:** low (scope, not correctness).
- **Michael decision:** ACCEPTED — Keep ofs-delta coupled to thin-pack for now and do not advertise ofs alone on other Ortec paths.

## D-P5-6 — Delta-chain depth cap value + exceed behaviour *(minor)*
**Question:** What recursion/chain-depth cap should the unified resolver enforce, and what
happens on exceed?
- **Context:** git's default `pack.depth` is 50; a malformed/hostile pack could present a longer
  or cyclic chain.
- **Recommendation:** cap at **64** (comfortable margin over git's 50); on exceed (or a detected
  offset cycle) raise `zcx_abapgit_ortec_git` → fail-safe cascade (§6). Never unbounded
  recursion.
- **Correctness stakes:** low-medium (DoS/robustness, not silent corruption — a mis-decode is
  still caught by SHA recompute).
- **Michael decision:** ACCEPTED — Enforce a chain-depth cap of 64 and raise on exceed.

## D-P5-7 — `ofs-delta → ref-delta → thin base` case *(HIGH stakes)*
**Question:** Confirm the design handles an ofs-delta whose base (by offset) is itself a
**ref-delta** whose base is a **thin** object (outside the pack, in `obj_store`). This is the
subtlest legal thin-pack shape.
- **Context:** git may reference a reconstructed ref-delta (thin base) by offset from a later
  object. Resolving the ofs entry then requires resolving the ref entry first, which requires the
  thin base from `obj_store`. The unified resolver's dependency-ordered recursion + `obj_store`
  ref-base fallback is designed to cover this; test **T5** pins it. The completeness gate
  (D-P5-2) is what guarantees the thin base is actually present before we ever advertise thin.
- **Recommendation:** Require test **T5** to pass **and** require the D-P5-2 "both" gate. Do not
  ship 5b without T5 green.
- **Correctness stakes:** HIGH — a gap here is silent wrong-data on a legal server response.
- **Michael decision:** ACCEPTED — Require test T5 to pass and require the D-P5-2 both gate before shipping Phase 5b.

---

## Gatekeeping
All seven decisions (**D-P5-1** through **D-P5-7**) are resolved and accepted. Implementation may proceed starting with Phase 5a only. Rollout order is fixed regardless of the
above: **5a decode-only + unit tests green → 5b enable capability/gate/cascade → 5c
persistence/eviction tie-in.** Do not enable `thin-pack` until OFS_DELTA decode is unit-proven
(5a green).
