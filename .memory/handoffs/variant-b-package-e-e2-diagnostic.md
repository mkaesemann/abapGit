# Variant B / Package E — E2 Diagnostic Handoff (abapGit-testing, SMALL REPO — D0/D1 only)

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

The active E2 incident is now the OS4 repository — see
[.memory/handoffs/variant-b-package-e-e2-os4-diagnostic.md](.memory/handoffs/variant-b-package-e-e2-os4-diagnostic.md).
This file is retained only as a historical record of the (invalid) small-repo
investigation.

**Update (2026-07-30): the D0 reproduction this handoff was based on is
retracted.** The owner's local git clone used to assert that `demo-branch`
and `demo/demo-branch` pointed to the same commit was stale. After `git
fetch origin`, the two branches were confirmed to point to **different**
commits, and the reported false Local+Remote MODIFIED symptom does not
reproduce on this (small) repository. Consequently:

- The D0 packet and D1 log linked below are marked
  `SUPERSEDED_INVALID_REPRODUCTION` and retained only as historical
  investigation records (they document a real, useful call-graph trace and
  elimination of several candidate mechanisms, but not a valid reproduction).
- **R12 (branch-switch/commit-resolution timing) is retracted** as the
  leading root-cause candidate — it was only ever "leading but unconfirmed",
  and the reproduction it was inferred from is now known to be invalid.
- No root cause is currently established for the originally reported
  symptom. Package E's false-MODIFIED investigation reverts to
  `WAITING_FOR_D0_REPRODUCTION`: a new, git-fetch-verified reproduction
  packet is required before any further D0/D1 work resumes.
- `E2_FIX_AUTHORIZED=NO` remains in force. Do not use any conclusion from the
  superseded artifacts to authorize or design an E2 fix.

---

## Historical record (INVALID reproduction — do not use as current evidence)

The following summarizes the retracted investigation for traceability only.

## Summary

- D0: COMPLETE. Owner-supplied, DB-corroborated reproduction on IT8: repo
  `abapGit-testing` (repo GUID `000000000004`, package `ZOR_ABAPGIT_TEST`),
  branch `demo-branch` shows correct "no changes"; branch `demo/demo-branch`
  (switched to via the same repo entity) shows false Local+Remote MODIFIED for
  `TABL ZOR_ABAPGIT_TEST` and `PROG ZOR_ABAPGIT_TESTING`, via Full unfiltered
  Stage. Full packet:
  [.memory/incidents/variant_b_package_e_false_modified_d0.md](.memory/incidents/variant_b_package_e_false_modified_d0.md).
- D1: Full call-graph trace + read-only IT8 DB verification performed. Full
  detail, comparison-input matrix, and R1-R12 verdict matrix:
  [.memory/logs/variant_b_package_e_false_modified_d1.md](.memory/logs/variant_b_package_e_false_modified_d1.md).
- Eliminated this session (CONTRADICTED / NOT_APPLICABLE, with evidence): R1,
  R2 (OBJ_INDEX not used — unfiltered access), R3 (content-addressed
  reconstruction correct-by-construction), R5 as sole cause (checksums
  baseline is repo-GUID-scoped, not branch-scoped), R9 (confirmed unfiltered
  access), R10's only concrete candidate mechanism (legacy
  `zcl_abapgit_pull_buffer`/`zcl_abapgit_blob_buffer` — confirmed dead code,
  calls commented out in `zcl_abapgit_git_porcelain.clas.abap:557,640`).
- Leading but UNCONFIRMED candidate: R12 (branch-switch/commit-resolution
  timing). The one concrete branch-switch mechanism found
  (`zcl_abapgit_ortec_branch_list=>perform_switch` →
  `zif_abapgit_repo_online~select_branch`) correctly calls `reset_remote()`
  before switching — no defect found in it directly.
- **Blocking evidence gap**: this diagnostic cannot independently confirm,
  without a live branch-tip check taken at reproduction time (outside D0/D1
  read-only-DB/static-source scope) or D2 runtime instrumentation (not
  authorized this phase), whether `demo-branch` and `demo/demo-branch`
  actually resolved to the identical commit SHA1 at the moment each Stage
  view was rendered. If confirmed identical, every mechanism traced this
  session is correct-by-construction, meaning the actual defect (if real)
  lies in a statement not yet localized — ROOT_CAUSE=PARTIAL, not CONFIRMED.

## Standard abapGit involvement

Several standard (non-ORTEC) classes are in the call graph
(`zcl_abapgit_repo`, `zcl_abapgit_repo_online`, `zcl_abapgit_repo_checksums`,
`zcl_abapgit_status_calc`, `zcl_abapgit_git_branch_list`,
`zcl_abapgit_git_transport`). All standard-code paths inspected this session
were confirmed CORRECT by direct source read (branch-scoped cache
invalidation on `select_branch`, exact-match `find_by_name`, repo-GUID-scoped
checksums, early-return-on-SHA1-match in `zcl_abapgit_status_calc`). **No
standard abapGit defect is suspected or was found.** No standard source was
modified. No owner approval request for a standard-source hook is needed at
this time.

## Ownership

Mixed call graph (ORTEC + standard abapGit), but the open question is scoped
to ORTEC's own commit-resolution/branch-switch consistency, not the standard
comparison algorithm — see D1 §7.

## Correctness / protocol / performance review applicability

- Correctness review: N/A this phase (no fix proposed).
- Protocol/persistence review: N/A this phase.
- Performance design gate: N/A this phase (no productive change made).
- Unit tests: none added (read-only diagnostic).
- DDIC/state impact: none. No `.memory/state.md` write performed (per task
  scope). No new DB writes performed (only read-only `SELECT`s via
  `mcp_arc-12_SAPQuery`).

## Next evidence required (before any D0/D1/D2/E2-FIX work resumes)

0. **Superseding this retraction**: obtain a NEW reproduction where the
   owner's local git clone has been freshly fetched (`git fetch origin`)
   immediately before comparing branch tips, to rule out stale-local-clone
   artifacts before any further code-level diagnosis begins.

1. **Owner action**: at/near the exact time of a fresh reproduction attempt,
   capture the live branch list for the `abapGit-testing` URL (the existing
   branch-picker UI already performs a live HTTP call — see D1 §1) and record
   the SHA1s shown for `demo-branch` and `demo/demo-branch` side by side, plus
   the exact Stage-view MODIFIED/unchanged result observed immediately after.
2. If step 1 shows the two branches' live tips genuinely differ at that
   moment: this is very likely a correct result, not a defect — report back
   and this incident can likely be closed without further code changes.
3. If step 1 confirms identical live tips together with a reproduced false
   MODIFIED: request owner authorization for a bounded D2 runtime trace
   (read-only instrumentation only, per Package E ladder rules) capturing the
   actual `lv_target_commit` and per-file `local_sha1`/`remote_sha1` pair used
   by `zcl_abapgit_status_calc` for both Stage renders, to localize the exact
   statement responsible.

## Artifacts

- [.memory/incidents/variant_b_package_e_false_modified_d0.md](.memory/incidents/variant_b_package_e_false_modified_d0.md)
- [.memory/logs/variant_b_package_e_false_modified_d1.md](.memory/logs/variant_b_package_e_false_modified_d1.md)
- This file.

## State

`.memory/state.md` was NOT modified (out of scope for this task). This
handoff is the authoritative record of the D0/D1 outcome until a state update
is explicitly authorized.
