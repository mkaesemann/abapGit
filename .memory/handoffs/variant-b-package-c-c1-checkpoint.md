# Variant B / Package C / Checkpoint C1 — implementation handoff

### Status

- Status: `SAP_VALIDATED_COMPLETE`
- C0 design gate:
  `CLOSED`
- C1 implementation commit:
  `aeac812652da4d18d46e0ee4aad742baaa179e80`
- C1 SAP/ATC fix commit:
  `b3701afbc812e5379b886fdc6a1e3db134578012`
- Validated C1 HEAD:
  `b3701afbc812e5379b886fdc6a1e3db134578012`
- Scope:
  certificate-only have policy class plus one fastpath have-source migration
- C2 status:
  not implemented
- Remaining C1 blockers:
  none

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

### Validation

#### Static and review gates

- Local ADT diagnostics:
  `PASS`
- Method-name limit:
  `PASS`
- Performance scan:
  `PASS`
- Implementation audit:
  `PASS`
- Static regression:
  `PASS`
- Remaining live caller of
  `ZCL_ABAPGIT_ORTEC_FETCH_NEG=>GET_VERIFIED_HAVE_COMMITS`:
  none in the migrated fastpath
- C2 scope leakage:
  none

#### Owner-executed SAP validation

- IT8 import:
  `PASS`
- Activation:
  `PASS`
- `ZCL_ABAPGIT_ORTEC_HAVE_POLICY` ABAP Unit:
  `PASS`
- Bundled tests in `ZCL_ABAPGIT_ORTEC_GIT_TESTS`:
  `PASS`
- Productive ATC:
  `PASS`
- Working tree after validation:
  clean
- Final SAP validation verdict:
  `SAP_VALIDATED_COMPLETE`

#### SAP correction

The original C1 implementation required a focused syntax/exception-contract
correction in `ZCL_ABAPGIT_ORTEC_FASTPATH=>COMPLETE_MISSING_OBJECT`.

The existing exception translation was extended to cover the request-URI
construction as well, preserving:

- the public `ZCX_ABAPGIT_ORTEC_GIT` contract;
- the original `ZCX_ABAPGIT_EXCEPTION` as `previous`;
- the dormant status of the per-object completion path;
- all C1 have-policy behavior.

Correction commit:

`b3701afbc812e5379b886fdc6a1e3db134578012`

### Next action

Proceed with Package C C2 from validated HEAD
`b3701afbc812e5379b886fdc6a1e3db134578012`.

C2 scope:

- productive warm/incremental/cold routing;
- explicit one-time opportunistic backfill invocation;
- Package B cold-path wiring;
- incremental certification lifecycle;
- verified branch publication.

Do not repeat C0 or C1 and do not begin Package D.
