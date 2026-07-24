# Design review required — open decisions for Michael

- **Phase:** design (ortec-abapgit-design, Claude Opus 4.8)
- **Date:** 2026-07-10 (updated 2026-07-10 with owner decisions on D6/D7 + admin report)
- **Status:** DECISIONS RESOLVED — all of D1–D7 decided by the owner (see each decision).
  Plan is implementation-ready; per the design-mode gate, coding still starts only on Michael's
  explicit go-ahead. No decision remains blocking; residual items are implementation-time
  confirmations noted inline.
- **Companion:** `.memory/logs/target_design.md`, `.memory/diagrams/h4_target_architecture_legacy.mmd`

The design separates a **safe read-only sparse lookup** (data-validity gated) from the
**mutating/protocol-altering persistent-cache opt-in**. The decisions below are the points where
correctness and performance genuinely trade off, or where a default must be chosen by the owner.

---

## D1 — Default-on side-effect cache population? *(central decision, blocks Phase 2)*
**Question:** Should `ZAOG_OBJ_STORE` populate as a harmless side effect of every normal full
pull (no protocol change), so the read fast-path helps **all** repos — or stay strictly opt-in?

- **Context:** Removing the read gate (Phase 1) only speeds up repos whose store is **already
  populated**. Population today happens via the write-side fastpath, which is opt-in. So without
  D1, the regression fix helps only repos that opted in at least once.
- **Option A (recommended):** Populate on normal pull regardless of the protocol opt-in.
  Side-effect writes only; negotiation unchanged. Universal speedup.
  - *Cost:* `ZAOG_*` growth + write time on every pull; needs D5 retention.
- **Option B:** Keep population opt-in.
  - *Cost:* Read decoupling benefits few repos; "gains" stay mostly invisible.
- **Trade-off:** performance/coverage (A) vs. storage & write cost (B).
- **Design recommendation:** A, gated by a lightweight session/repo flag defaulting ON, plus D5.
- **Michael decision:** Option **B** for now. Keep the strict separation: if fastpath opt-in is
  off, do not use or write the object store. The default abapGit path remains the fallback.
  Revisit ungated population only after the new fastpath is proven stable in production.

## D2 — Read fast-path gating mechanism
**Question:** Fully ungate the read path (data-validity only) or add a new read-only flag
defaulting ON?
- **Recommendation:** Data-validity only (no policy flag on the read path). The fallback chain
  already guarantees correctness; a flag re-introduces the conflation that caused the regression.
- **Decision needed:** confirm no per-repo read opt-out is required for support/debugging.

## D3 — Thin-pack acceptance policy
**Question:** Always request non-thin packs (simplest, safe, more bandwidth) or accept thin
packs only when delta-base completeness is verified (faster, needs the new delta index)?
- **Trade-off:** bandwidth/latency vs. schema addition + verification complexity.
- **Recommendation:** Add the delta-base index and accept thin only when complete; non-thin
  fallback otherwise. (Enables Phase 5; without it, force non-thin everywhere.)
- **Michael decision:** Follow recommendation. Implement the delta-base index and use it to
  allow thin packs only when base completeness is verified.

## D4 — `CONFIRMED_ABSENT` strictness
**Question:** Require **all four** positive resolutions (remote tip + commit + parent tree +
path) before ever classifying a file as remote-Deleted — even though that costs an extra remote
ref/commit resolution when the store is incomplete?
- **Recommendation:** Yes (correctness over speed). This is the non-negotiable guard against
  "missing local data → remote deletion". Confirm acceptance of the extra resolution cost.
- **Michael implementation direction:**
  1. Implement both modes in code and control them via a constant in the switch class so speed
    can be benchmarked side-by-side.
  2. Ensure the stale-data protection is broader than remote-Deleted only: avoid false
    local:modified, remote:modified, and remote:added due to incomplete/stale acquisition.
  3. Treat remote-Deleted as one status outcome of a broader completeness/consistency model.
- **Michael decision:** Accepted with the above conditions. The model must not treat
  remote-Deleted as a special isolated path; completeness/consistency checks must protect all
  status outcomes.

## D5 — Retention / eviction policy for `ZAOG_*` on very large repos
**Question:** What retention keeps the cache bounded without breaking correctness?
- **Constraints:** never evict a delta base still referenced by a stored delta; keep the current
  tip complete.
- **Options:** keep last N commits per branch / LRU by `last_used_ts` / size cap per repo /
  manual clear only (exists today via `clear_repo_cache`).
- **Decision needed:** policy + limits. Prerequisite for D1 Option A.
- **Timing note:** Michael must choose the D5 policy before Phase 2 implementation begins.
- **Michael decision:** DB space is not a primary concern for now; use **manual clear** as the
  default retention approach.
- **Michael follow-up request:** add an admin report/program (separate from the main flow) that
  can show current store size and counts, run manual clear, and where feasible offer optional
  maintenance actions (for example compacting and cleanup of outdated/unused refs or branches).
  If advanced maintenance is not feasible in ABAP/DB context, keep simple manual cleanup.
- **Design response — admin report concrete scope (Phase 6, optional, off the hot path):**
  - **Object:** executable report `ZABAPGIT_ORTEC_CACHE_ADMIN` (program), placed under
    `src/ortec/` (Ortec namespace), *not* wired into the `ZABAPGIT` transaction/UI so it never
    touches the Stage/Diff/Patch hot path. Optionally reachable via its own transaction
    `ZABAPGIT_ORTEC_ADMIN`. All logic delegates to a thin class `zcl_abapgit_ortec_cache_admin`
    (report = selection screen + ALV only).
  - **Shows (read-only overview, ALV per repo_key):** remote_url + branch (`ZAOG_REPO_STATE`),
    object count and total `obj_size` (MB) (`ZAOG_OBJ_STORE`), pack count + raw pack bytes
    (`ZAOG_PACK_META` / `ZAOG_RAW_PACK`), index entries + per-commit `$IDX/__READY__` markers
    (`ZAOG_OBJ_INDEX`), commit count (`ZAOG_COMMIT_HIST`), open fetch sessions
    (`ZAOG_FETCH_SESS`), `curr_commit` / `fetch_commit` / `fetch_ts` / `is_shallow`, and
    `last_used_ts` once the retention column exists.
  - **Actions:** (1) **Manual clear per repo / selected / all** — reuses the existing
    `zcl_abapgit_ortec_git_switch=>clear_repo_cache` (already returns per-table counts via
    `ty_clear_result` + `format_clear_result`). (2) **Optional compact** (delete objects not
    reachable from `curr_commit`/`fetch_commit` and not referenced as a delta base) — enabled
    only when the D3 delta-base index exists. (3) **Ref/branch cleanup** — remove
    `ZAOG_REPO_STATE`/`ZAOG_COMMIT_HIST` rows for branches no longer advertised by the remote.
    (4) **Stale fetch-session cleanup** (`ZAOG_FETCH_SESS`).
  - **Safeguards:** repo-scoped enqueue via `EZAOG_REPO_LOCK` around every delete; **dry-run /
    simulation default ON** (report the delete set before executing); explicit confirmation
    popup before any destructive action; authorization check (`S_DEVELOP` or a custom admin
    auth object); **never** delete an object reachable from the current tip or a delta base
    still referenced by a stored delta; compact stays disabled unless delta-base reachability
    is provably complete.
  - **Fallback if compacting is not feasible** (no delta index yet, or reachability cannot be
    computed safely): hide/disable the compact + ref-cleanup actions and keep only the size
    report + **whole-repo manual clear** (`clear_repo_cache`), which is always safe because the
    read path simply re-populates or falls back to `get_files_remote()`. This matches Michael's
    "keep simple manual cleanup" instruction.

## D6 — Remote-tip validation cost on filtered ops
**Question:** The read path calls `branches(url)` to verify the tip still matches on every
filtered Stage/Diff. Keep per-op validation (always correct, adds latency) or cache the tip with
a short TTL (faster, small staleness window)?
- **Recommendation:** Keep per-op validation by default; offer an optional short TTL if latency
  is measured as a problem (Phase 6).
- **Michael decision:** Keep **per-op validation as the default**, but implement a **switchable
  short-TTL mode** so the latency impact can be benchmarked side-by-side (same pattern as the
  D4 two-mode switch).
- **Switch behavior (exact):**
  - Controlled by a compile-time constant in `zcl_abapgit_ortec_git_switch`, e.g.
    `cs_tip_validation-mode` with values `PER_OP` (default) and `TTL`, plus
    `cs_tip_validation-ttl_seconds` used only in `TTL` mode.
  - **`PER_OP` (default):** every filtered Stage/Diff/Patch calls `branches(url)` and compares
    the tip before serving from Layer 1. Zero staleness window. This is the shipping default.
  - **`TTL`:** the resolved tip is cached per (repo_key, branch) with a timestamp; within
    `ttl_seconds` the cached tip is trusted and the `branches(url)` round-trip is skipped;
    after expiry the next op re-validates. Bounded staleness window = `ttl_seconds`.
  - **Safety:** the TTL cache only *skips the tip round-trip*; it never suppresses the
    downstream data-validity fallback. A tip mismatch discovered on the next validation still
    triggers `invalidate_tip_commit`/fallback, so TTL mode can never classify stale data as a
    change — it only trades a small staleness window for latency. Default remains `PER_OP`;
    `TTL` is opt-in for benchmarking, mirroring D4.
- **Michael TTL-default choice (Option 2):** for `TTL` benchmark mode, derive
  `cs_tip_validation-ttl_seconds` from measured RTT in Phase 6,
  `ttl_seconds = min( 3 * RTT_median_seconds, 30 )`, rounded to whole seconds with floor `2`.
  Keep `PER_OP` as the shipping default.

## D7 — Standard `zcl_abapgit_git_porcelain` change budget (minimal-touch vs. Ortec mirror)
**Question:** How much standard `zcl_abapgit_git_porcelain` churn is acceptable to land the fix
(especially the `walk` / `walk_tree` bulk-collect rework and the `walk_tree` missing-`repo_key`
correction)?
- **Michael decision:** The fix is accepted **only if it avoids major/rippling changes** in the
  standard `zcl_abapgit_git_porcelain`. If a correct fix would require large standard-code churn,
  route the logic through an **Ortec-owned mirror/clone path** instead of heavily editing the
  standard class.
- **Explicit decision rule:**
  1. **Minimal-touch first (default).** In `zcl_abapgit_git_porcelain`, only tiny guarded
     delegation hooks are allowed:
     - `walk` / `walk_tree`: always pass an explicit `repo_key`, and on a missing object
       delegate to `zcl_abapgit_ortec_missing_obj` (one added call + params) — **no**
       restructuring of the walk algorithm and **no** signature change visible to non-Ortec
       callers.
     - `pull_by_branch`: keep the existing fastpath try and move the walk-error self-heal into
       the Ortec repair coordinator (one call) — no new standard branching.
  2. **Threshold for "too big" (route to mirror).** Exceeded if a needed change would
     (a) alter a standard method signature in a way that ripples to non-Ortec callers,
     (b) restructure standard control flow beyond adding a guarded delegation call,
     (c) require touching more than the small enumerable hook set in §8 of the design, or
     (d) create meaningful merge-conflict risk against future upstream abapGit rebases.
  3. **Ortec mirror strategy (fallback).** If the threshold is exceeded, add an Ortec-owned
     `zcl_abapgit_ortec_porcelain` that reimplements/wraps the required clone/pull/walk logic.
     Standard porcelain then keeps only a one-line delegation at the entry (or the Ortec facade
     calls the mirror directly), leaving upstream code effectively untouched and rebase-safe.
  4. **Plan consequence.** Phase 4 must first attempt the walk/walk_tree changes as minimal
     in-place hooks. If the resulting diff exceeds the budget above, Phase 4 switches to the
     mirror strategy **without** changing any earlier phase or decision.
- **Concrete first instance — `walk_tree` missing `repo_key` (latent correctness bug):**
  `zcl_abapgit_git_porcelain=>walk_tree` calls
  `zcl_abapgit_ortec_obj_store=>get_object( iv_sha1 = iv_tree )` **without** `iv_repo_key`,
  relying on the session-cache repo_key — can read the wrong store in a multi-repo session.
  Treated as a bug fix (always pass explicit `repo_key`), pulled into Phase 1 alongside the
  read-path gate removal. This is exactly the kind of minimal-touch change rule (1) permits;
  it stays in-place. Only if the surrounding bulk-collect rework in Phase 4 would breach the
  threshold does that broader work move to the mirror class.

---

## Gatekeeping (per orchestrator)
Implementation may begin only on `APPROVE` or `APPROVE_WITH_MINOR_REVISIONS` from the design
review agent (`03b_design_review`). **All of D1–D7 are now accepted by the owner**
(D1 = Option B strict opt-in; D3 = implement delta-base index; D4 = two runtime modes via
switch constant + broad stale-data protection across all statuses; D5 = manual clear default +
optional admin report; D6 = per-op default + switchable short-TTL benchmark mode; D7 =
minimal-touch standard porcelain, else Ortec mirror). No decision remains blocking; the plan is
implementation-ready pending Michael's go-ahead to code.

## Recommended immediate path
Approve **Phase 0 + Phase 1** now (regression fix, no schema change, lowest risk), then proceed
with the durable rework (Phases 2–6) using manual clear as the default retention policy.
