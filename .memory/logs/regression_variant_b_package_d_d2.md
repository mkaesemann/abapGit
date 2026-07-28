# Regression validation: Variant B / Package D2

## Summary

- Task: read-only regression validation for the Variant B Package D2 / D2b1 / D2b2 ORTEC implementation scope.
- Scope: staged delta visibility, attempt-id plumbing, repo-lock ownership, and the fastpath/porcelain publication-unit orchestration paths.
- Validation mode: source inspection plus workspace diagnostics; no live SAP ABAP Unit or activation run was available in this environment.
- Result: PASS_WITH_FINDINGS.

## Files reviewed

- [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap)
- [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.testclasses.abap)
- [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.testclasses.abap)
- [src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap](src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap)
- [src/ortec/git/zcl_abapgit_ortec_porcelain.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_porcelain.clas.testclasses.abap)
- [src/ortec/git/zaog_obj_store.tabl.xml](src/ortec/git/zaog_obj_store.tabl.xml)
- [src/ortec/git/zaog_fetch_sess.tabl.xml](src/ortec/git/zaog_fetch_sess.tabl.xml)
- [src/ortec/git/zaog_pack_meta.tabl.xml](src/ortec/git/zaog_pack_meta.tabl.xml)

## Evidence collected

- Workspace diagnostics reported no errors in the reviewed ABAP sources.
- The relevant handoff and audit notes were reviewed:
  - [.memory/handoffs/variant-b-package-d-d2b1-implementation.md](.memory/handoffs/variant-b-package-d-d2b1-implementation.md)
  - [.memory/handoffs/variant-b-package-d-d2b2-implementation.md](.memory/handoffs/variant-b-package-d-d2b2-implementation.md)
  - [.memory/logs/performance_audit_variant_b_package_d2.md](.memory/logs/performance_audit_variant_b_package_d2.md)

## Scenario matrix

| Scenario | Status | Evidence |
| --- | --- | --- |
| Unresolved REF_DELTA rows are promoted to the intermediate staged status instead of the ready status | PASS | Verified in [src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap) and the corresponding production path in [src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap). |
| Generic object-store reads stay hidden from staged rows until they are explicitly surfaced | PASS | Verified in [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap) and the matching test methods in [src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap). |
| Attempt-id is threaded through object-store persistence and commit-history correlation | PASS | Verified in [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap), [src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap), and the added tests in [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.testclasses.abap) and [src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.testclasses.abap). |
| Repo lock is acquired and released around the intended publication-unit work, without spanning the earlier HTTP phase | PASS | Verified by direct source inspection of [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap) and [src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap](src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap). |
| Performance audit verdict | PASS_WITH_MINOR_FINDINGS | The implementation audit in [.memory/logs/performance_audit_variant_b_package_d2.md](.memory/logs/performance_audit_variant_b_package_d2.md) is PASS_WITH_MINOR_FINDINGS; the only finding is a non-blocking same-repo lock-contention latency consideration that remains structurally reviewed rather than runtime measured. |
| Live SAP activation / ABAP Unit execution | NOT RUN | No connected SAP system or live ABAP Unit runner was available in this environment. |

## Failure analysis

- Failing class/method: none.
- No correctness defect was identified from the static review of the D2 / D2b1 / D2b2 implementation path.

## Corrective proposal

- None required for this validation pass.
- If live SAP validation becomes available, the next step is to import the relevant checkpoint into SAP and run the ABAP Unit / ATC checks to confirm the runtime behavior end to end.

## Final local regression confirmation

- Regression verdict: PASS_WITH_FINDINGS.
- Blocking findings: 0.
- Performance audit verdict: PASS_WITH_MINOR_FINDINGS.
- SAP validation: NOT RUN in this environment.

## SAP validation closeout

```text
STATUS=SAP_VALIDATED_COMPLETE
PACKAGE_D_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
ABAP_UNIT=PASS
ATC=PASS_WITHOUT_SEVERE_FINDINGS
```

Live IT8 validation subsequently confirmed this regression scope: three
incidents surfaced and were resolved (SYSTEM_NO_ROLL, TIME_OUT,
DBSQL_STMNT_TOO_LARGE, all SAP_VALIDATED_RESOLVED), and a follow-up SAT
trace found zero cost contribution from any D2-owned mechanism (see
[.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md](.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md)).
AUDIT-M-1 (same-repo lock-contention latency, documented in
`performance_audit_variant_b_package_d2.md`) remains a non-blocking,
structurally-reviewed-only finding - no live contention was observed or
measured in any of the three incident retests or the SAT trace.
