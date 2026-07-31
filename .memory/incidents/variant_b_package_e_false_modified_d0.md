# Variant B / Package E — False Local+Remote MODIFIED — D0 Reproduction (abapGit-testing, SMALL REPO)

```text
STATUS=SUPERSEDED_INVALID_REPRODUCTION
INVALIDATED_BY_OWNER_EVIDENCE=2026-07-30 (owner ran `git fetch origin` on the
  local abapGit-testing clone used to compare demo-branch and
  demo/demo-branch)
REASON=stale local Git clone; after git fetch origin the compared branches
  had different tips; small repository does not reproduce false MODIFIED
ROOT_CAUSE_ID=RETRACTED
R12=NOT_ESTABLISHED
E2_FIX_AUTHORIZED=NO
```

The successor active incident is the OS4 repository, tracked in
[.memory/incidents/variant_b_package_e_false_modified_os4_d0.md](.memory/incidents/variant_b_package_e_false_modified_os4_d0.md).
See
[.memory/handoffs/variant-b-package-e-e2-os4-diagnostic.md](.memory/handoffs/variant-b-package-e-e2-os4-diagnostic.md)
for the current phase status.

## Claims validity split (post-retraction)

**INVALID — depended on the bad reproduction, do not cite as evidence:**
- The entire D0 reproduction packet below (branches/commits/"same commit"
  claim, the two affected objects as a *false*-MODIFIED example).
- R12 (branch-switch/commit-resolution timing) as a root-cause candidate for
  this repository — it was inferred solely from this now-invalidated
  reproduction.
- The D1 log's root-cause matrix verdicts that depended on D0 being real
  (§6 of the linked D1 log), and its "evidence gap" framing in §5.

**STILL GENERALLY USEFUL — source observations independent of whether this
particular reproduction was real, retained for reuse in the OS4 investigation:**
- The verified call graph itself (§1 of the linked D1 log): `fetch_remote`,
  `select_branch`/`reset_remote`, `pull_by_branch`/`classify_operation`,
  `zcl_abapgit_status_calc=>build_existing`'s early-return-on-match logic,
  `zcl_abapgit_repo_checksums`'s repo-GUID (not branch) scoping, and the live
  (non-cached) `zcl_abapgit_git_transport~branches`/`find_by_name` exact-match
  lookup — these are static-source facts about the current codebase, not
  dependent on the invalid reproduction.
- The finding that `zcl_abapgit_pull_buffer`/`zcl_abapgit_blob_buffer` are
  dead code (calls commented out) — a standing source fact.
- The general lesson that a "same commit" claim from an owner-side git clone
  must be re-verified with a fresh `git fetch origin` before being trusted as
  D0 evidence (this is now a hard requirement for the OS4 D0 packet).

---

Status (historical, INVALID): D0_REPRODUCTION=COMPLETE (owner-supplied + DB-corroborated)
Baseline: CURRENT_HEAD=8b382a5e (E1-PERF-A productive change isolated to
`c_index_write_chunk_size` 1000→30000 in
`zcl_abapgit_ortec_obj_index=>rebuild_index`; no index-content, readiness, or
status-semantic change — independent of this diagnostic).

## Required packet (owner-supplied, IT8)

| Field | Value |
|---|---|
| System | IT8 |
| Remote URL | `https://ORTEC-SAP@dev.azure.com/ORTEC-SAP/ORTEC%20for%20S4HANA/_git/abapGit-testing` |
| Affected branch (incorrect) | `refs/heads/demo/demo-branch` |
| Comparison branch (correct) | `refs/heads/demo-branch` |
| Owner-stated shared commit | `cacac3bb4e9f5505a4c0d24ffe70631b3c0571e5` (both branches "point to the same commit") |
| Affected objects | `TABL ZOR_ABAPGIT_TEST`, `PROG ZOR_ABAPGIT_TESTING` |
| Access mode | Full unfiltered Stage (NOT OBJ_INDEX / Stage-by-Transport) |
| Symptom | `demo-branch` Stage → no changes (correct). `demo/demo-branch` Stage → both objects shown Local MODIFIED **and** Remote MODIFIED, despite owner asserting no real delta. |
| Owner hypothesis | "the state it checks is not of the branch itself, but a previously opened one" |

## Evidence sources

- Owner interview (`vscode_askQuestions`), this session.
- Live read-only DB query (`mcp_arc-12_SAPQuery`) against IT8, this session
  (see D1 log for full result sets).
- Static source trace of the full remote-reconstruction and status-comparison
  call chain (see D1 log).

## Missing fields / not yet independently verified

- **The owner's "same commit" claim has NOT been independently re-verified
  live** (e.g. via a fresh `git ls-remote`/branch listing taken at the exact
  moment of the reported Stage views). This is the single most important
  missing fact — see D1 "First wrong decision or evidence gap".
- Exact timestamp of the reported Stage views (only DB fetch/certification
  timestamps are known, not when Michael actually looked at the two Stage
  pages).
- Whether `demo/demo-branch` was viewed via the standard repo's own
  Stage page after a real branch switch (`zcl_abapgit_ortec_branch_list=>
  perform_switch` / `zif_abapgit_repo_online~select_branch`), or via some
  other UI entry point. Not confirmed with the owner.

## Reproduction reliability

- **Concrete, owner-confirmed, currently reproducible on IT8** (not a
  one-off/historical report). Only one persisted abapGit repo entity exists
  for this URL (repo GUID `000000000004`, `PACKAGE=ZOR_ABAPGIT_TEST`,
  `BRANCH_NAME=refs/heads/demo-branch` persisted, `DESERIALIZED_AT=
  20260722155508`), so `demo/demo-branch` is viewed by switching this same
  repo's selected branch, not via a second repo entity.
- DB evidence (`ZAOG_REPO_STATE`, `ZAOG_COMMIT_HIST`) independently
  corroborates the existence of both branches and a commit history record for
  `cacac3bb...`, but shows an unexplained asymmetry (see D1) that must be
  resolved before ROOT_CAUSE can move from PARTIAL to CONFIRMED.

See [.memory/logs/variant_b_package_e_false_modified_d1.md](.memory/logs/variant_b_package_e_false_modified_d1.md)
for the full call-graph trace and root-cause matrix, and
[.memory/handoffs/variant-b-package-e-e2-diagnostic.md](.memory/handoffs/variant-b-package-e-e2-diagnostic.md)
for the phase verdict and next-evidence request.
