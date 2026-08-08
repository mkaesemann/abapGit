# SER-SLICE-5 — ranked backlog (evidence-supported only)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_5_RANKED_BACKLOG
STATUS=FINAL
```

```text
1. OWNER_ACTION: rerun IT8 (SER-SLICE-4 &sect;9 SAT comparison, or a fresh Normal/
   Batch trace pair) WITH the SLICE5-001 fix
   (zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap) in place.
   Highest priority by far - this single fix is the prerequisite for
   measuring whether EIGHT already-built, already-unit-tested provider
   families (DOMA/DTEL/CLAS/INTF/MSAG/TABL/PROG/FUGR) and the WAPA batch-path
   replacement deliver any real production benefit at all. No further FUGR/
   DDLS/WAPA design work should be prioritized ahead of this.

2. FUGR: after the IT8 retest, confirm `provider_hit=1` and reduced native
   ENLFDIR/AREAT SQL for the 358 FUGR objects (category B, discovery log).
   REPAIR_EXISTING_PROVIDER_COVERAGE is expected to close automatically once
   #1 is validated - no new code change anticipated.

3. Tail-latency re-check: compare WAIT ASYNC's 66.26% share before/after #1 -
   if it drops materially, no scheduling change is needed; if not, escalate to
   a dedicated `ZCL_ABAPGIT_ORTEC_SER_PLANNER`/`SER_COST` performance review.

4. DDLS MEASURE_FIRST: after #1's IT8 result is known, decide whether a 9th
   provider family is worth designing - do not start design work before then.

5. WAPA relaxation MEASURE_FIRST: requires #1's IT8 retest to include a real
   WAPA-in-batch object, PLUS a dedicated WAPA-heavy-repository trace (owner's
   own motivating case) before any bounded-multi-WAPA design is authorized.

6. FUGR Option C (source/include provider): remains MEASURE_FIRST/deferred,
   unchanged from SER-SLICE-4's own approved design - no new evidence this
   slice moves it forward.

7. Bulk Exists / Object store: separate performance areas, no action, not
   mixed into serializer work.
```
