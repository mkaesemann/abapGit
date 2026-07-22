# Variant B / Package C / Checkpoint C1 — implementation handoff

## Status
- STATUS: `IMPLEMENTED_PENDING_SAP_VALIDATION`
- C0 design gate: CLOSED (`APPROVED_WITH_RESOLVED_REVISIONS`, DR-006 closed this session)
- Scope: certificate-only have policy class + one call-site migration. C2 (porcelain routing, cold-branch orchestration, incremental publication) explicitly NOT implemented.

## Implemented symbols
- NEW `ZCL_ABAPGIT_ORTEC_HAVE_POLICY` (public, final):
  - `CLASSIFY_OPERATION( iv_repo_key, iv_target_commit ) RETURNING rv_class` — pure certificate read, no write/COMMIT/HTTP.
  - `GET_CERTIFIED_HAVES( iv_repo_key, it_want_hashes OPTIONAL, iv_max_haves DEFAULT 50 ) RETURNING rt_haves` — single bulk SELECT on `ZAOG_COMMIT_HIST` (`HIST_LEVEL = FULL_COMPLETE` only), deterministic sort, capped.
  - `TRY_BACKFILL_TARGET( iv_repo_key, iv_target_commit, iv_branch_name OPTIONAL ) RETURNING rv_certified RAISING zcx_abapgit_ortec_git` — bounded, idempotent, local-only certification side effect; only `verify_tree_closure`'s `zcx_abapgit_ortec_git` is caught.
  - Files: `src/ortec/git/zcl_abapgit_ortec_have_policy.clas.abap`, `.clas.xml`, `.clas.testclasses.abap` (16 tests, `ltcl_have_policy`).
- MODIFIED `ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK` (`src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`): have-resolution now resolves `lv_have_repo_key` via `ZCL_ABAPGIT_ORTEC_REPO_STATE=>GET_REPO_KEY_FOR_URL` (read-only, no key creation — preserves prior empty-haves behavior for unknown URLs) then calls `ZCL_ABAPGIT_ORTEC_HAVE_POLICY=>GET_CERTIFIED_HAVES`, gated by the same `incremental_thin` / `incremental_self_contained` mode check as before. `ZCL_ABAPGIT_ORTEC_FETCH_NEG=>GET_VERIFIED_HAVE_COMMITS` is no longer called from this method (definition intentionally retained — Package E scope).
- Design doc closeout: `.memory/logs/variant_b_package_c_design.md` updated to close DR-006 (§1, §3, §3a rewritten for the pure-classify / explicit-backfill API split).

## Invariant / acceptance matrix
- AC-1..AC-6 (see `.memory/reviews/implementation_audit_variant_b_package_c_c1.md`): all PASS, 0 blocking.
- PERF-01..PERF-04 (see `.memory/reviews/performance_scan_variant_b_package_c_c1.md` and `.memory/reviews/perf_scan_variant_b_package_c_c1.md`): all PASS, 0 blocking.
- Regression checklist (see `.memory/logs/regression_variant_b_package_c_c1.md`): PASS after orchestrator override of one false-positive item (grep wording issue, not a real defect — see file's "Orchestrator override" section).

## Validation performed
- Local `get_errors` (ADT diagnostics): 0 errors on all 3 new files + modified fastpath file.
- Method-name length check (≤30 chars): all clear, including test methods.
- Repo-wide grep: `get_verified_have_commits` has no live caller outside `zcl_abapgit_ortec_fetch_neg`'s own definition (Package E scope); `have_policy` is not referenced from `zcl_abapgit_ortec_porcelain` (no C2 scope creep).
- Full-repo `npx abaplint` run: inconclusive/noisy in this sandbox (pre-existing, unrelated "class not found" errors across many ortec files not touched this session, likely caused by the offline deps-clone step) — not treated as a finding against this slice.
- NOT performed (owner action required): IT8 import, activation, ABAP Unit execution, ATC.

## Next action
- Owner: import/activate on IT8, run ABAP Unit (`ltcl_have_policy`, 16 methods) and ATC, report results.
- On SAP_VALIDATED_COMPLETE: proceed to Package C C2 (porcelain routing, cold-branch orchestration, incremental publication) per design §13/§14.
