# Variant B Package E — Active-memory audit (M-STATE-01..07)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E0-MEMORY-AUDIT
BASELINE=8eef0b55fb37892c3d6b6428c038886481c73192 (HEAD, verified)
STATUS=AUDIT_COMPLETE
```

## Baseline verification (Phase 1)

```text
git rev-parse HEAD                         = 8eef0b55fb37892c3d6b6428c038886481c73192
git status --short                         = 2 tracked files show as "modified"
                                              with an EMPTY diff (git diff / --numstat
                                              both produce zero output) - confirmed
                                              line-ending/mode noise, not a real change:
                                              src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.testclasses.abap
                                              src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.testclasses.abap
                                              plus one untracked scratch file
                                              (git-client-package_d.txt, repo root,
                                              not a memory or source artifact)
git merge-base --is-ancestor 733bb307 8eef0b55  = YES (exit 0)
git diff --name-status 733bb307..8eef0b55  = 10 files, ALL under .memory/**
                                              (handoffs/incidents/logs/state.md only)
git diff --check 733bb307..8eef0b55        = empty (no whitespace errors)
```

```text
PACKAGE_D2_SAP_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
PACKAGE_D2_MEMORY_CLOSEOUT=8eef0b55fb37892c3d6b6428c038886481c73192
PACKAGE_E_DESIGN_BASELINE=8eef0b55fb37892c3d6b6428c038886481c73192
CURRENT_HEAD_KIND=memory-only
BASELINE_RANGE_CLASSIFICATION=MEMORY_ONLY
```

No unvalidated productive change exists after the D2 validated head. Continuing
with memory normalization and Package E discovery/design is authorized.

## Classification of load-bearing active-memory claims

| ID | Claim (prior `.memory/state.md`) | Classification | Disposition |
| --- | --- | --- | --- |
| M-STATE-01 | Duplicate D1 intermediate + D1 final + D0 + D2 + completed-work + roadmap + current-objective + next-action sections, each restating overlapping facts | CONFIRMED_CURRENT (the duplication itself is real) | Removed from the new compact state. Intermediate D1 implementation matrix (`PACKAGE_D_D1=IMPLEMENTED` block) is superseded by the `SAP_VALIDATED_COMPLETE` block; both remain verbatim in [.memory/handoffs/variant-b-package-d-d1-implementation.md](../handoffs/variant-b-package-d-d1-implementation.md) and [.memory/logs/regression_variant_b_package_d_d1.md](../logs/regression_variant_b_package_d_d1.md) - not deleted, just no longer duplicated in active state. |
| M-STATE-02 | "Repository HEAD is at `73cb519a`" (stated in the D1 closeout section, superseded later in the same file by the D2 closeout's "Repository HEAD is at `733bb307`") | SUPERSEDED (by content already present later in the same file) | Removed. New state carries only the single current productive baseline (`733bb307`, SAP-validated) and the current design/memory HEAD (`8eef0b55`). |
| M-STATE-03 | Baseline separation between Package C historical baseline, D2 SAP-validated baseline, and current repository HEAD | CONFIRMED_CURRENT as a requirement; PARTIALLY_PRESENT in the prior state (D2 closeout section already got this right; earlier sections did not) | New compact state states all three explicitly and never implies `8eef0b55` (memory-only) was itself imported/validated in SAP. |
| M-STATE-04 | `.memory/logs/variant_b_package_e_design.md` (existing draft) describes Package D as "remains unchanged and must be completed" and frames the fix around a "commit-based `deepen 1` fetch" that "fails to materialize every SHA requested" | SUPERSEDED | Confirmed by direct source read: `zcl_abapgit_ortec_missing_obj=>ensure_available` no longer performs a `deepen 1`/commit-scoped fetch at all - it calls `zcl_abapgit_ortec_cold_init=>materialize_missing_batches` (D2 TIME_OUT fix, `17513ba7`/`733bb307`), scoped exactly to the caller's missing blob SHA1 set. The draft's §2/§3 (INV-E-10..13, the adaptive-batching algorithm with 500/50/1000/2x/16 MiB/25 MiB constants) is **byte-for-byte already implemented** in `zcl_abapgit_ortec_cold_init.clas.abap` (`c_batch_rows_initial=500`, `c_batch_rows_min=50`, `c_batch_rows_max=1000`, `c_max_batch_growth=2`, `c_target_response_bytes=16777216`, `c_max_batch_response_bytes=26214400`). The whole draft is rewritten in [variant_b_package_e_design.md](variant_b_package_e_design.md) (new version) rather than patched in place, per instruction; the old content is superseded, not deleted (git history retains it). |
| M-STATE-05 | SAT artifact's branch-relationship/commit-distance claims; false-MODIFIED root cause | CONFIRMED partially UNVERIFIED already in the SAT artifact itself (`COMMIT_DISTANCE=NOT_VERIFIED`, `FILE_DELTA=NOT_VERIFIED`) and in the D1 triage doc (`Classification: PACKAGE_E_CONSUMER_COHERENCE`, explicitly "a static-evidence classification, not a measured reproduction") | UNVERIFIED (kept). New discovery adds further static evidence (see [variant_b_package_e_discovery.md](variant_b_package_e_discovery.md) §E2) that narrows, but does not close, the root cause. Not stated as measured fact anywhere in the new artifacts. |
| M-STATE-06 | "Missing `ZAOG_REPO_STATE`/`ZAOG_COMMIT_HIST` rows are not proof of failed persistence" / F4 omission is administration-only | CONFIRMED_CURRENT, and stronger than previously assumed | Direct source read of `zcl_abapgit_ortec_cache_admin=>get_repo_f4_values` shows it **already** unions three sources (`ZAOG_REPO_STATE`, then `ZAOG_OBJ_STORE` orphans labeled `<orphaned cache>`, then `ZAOG_COMMIT_HIST` orphans) - the F4 discoverability gap the old draft worried about is largely already closed. See discovery §E3. |
| M-STATE-07 | Deferred risk inventory (D2 lock-contention telemetry `AUDIT-M-1`, final large-repo profiling) | CONFIRMED_CURRENT, still open, still non-blocking | Carried into the new compact state's "Deferred topics" section with owner/entry-condition, not folded into Package E scope without evidence. |

## Additional finding not in the original M-STATE list

- The prior draft's §4/§5 "certified consumer decision flow" and "CERTIFIED_BUT_MISSING" repair contract assume no repair mechanism exists yet. Direct source read shows **two independent repair mechanisms already exist and are already SAP-validated**:
  1. Filtered/partial path: `zcl_abapgit_ortec_missing_obj=>ensure_available` (bounded targeted top-up, D2).
  2. Full/unfiltered path: `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `'Walk,'`-error-triggered `invalidate_all_history` + one full re-fetch retry (pre-dates D2, already production code).
  See discovery §E4. This reclassifies most of the draft's proposed new state machine as **already implemented in a different, already-approved shape** rather than a Package E gap.

## Verdict

```text
MEMORY_AUDIT=PASS_WITH_FINDINGS
BLOCKING=0
FINDINGS=M-STATE-01..07 all classified and dispositioned above; one
  additional finding (E4 already-implemented dual repair mechanism)
STATE_ACTION=REPLACE (.memory/state.md rewritten to the compact required
  schema; see the file itself for the new content)
```
