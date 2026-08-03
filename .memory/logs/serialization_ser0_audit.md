# SER-0 — Current-State Audit and Evidence Map (consolidated)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_PERFORMANCE_SER0_AUDIT
STATUS=INTERNALLY_CONSISTENT
SOURCES=.memory/logs/serialization_ser0_audit_standard.md (standard path, 24 citations),
        .memory/logs/serialization_ser0_audit_ortec.md (ORTEC path, 47 citations),
        .memory/logs/abapgit_git_serialization_perf_discovery.md +
        abapgit_git_serialization_perf_backlog.md (prior, Git-side-focused pass,
        cross-checked, not restated)
HEAD=8b56ffb802fca0a4f619088657d84c35949714c4 (branch ortec/abapgit_1_133-opt-rework)
```

This file is the single navigable synthesis. Detailed per-file citations live in
the two sub-audits above; do not duplicate them here — link and summarize only.

## 1. Exact standard serialization call flow (see standard sub-audit §1-§5)

```text
zcl_abapgit_serialize=>serialize()                         [.abap:749-860]
  -> filter_unsupported_objects / filter_ignored_objects on lt_tadir (already K/N per caller)
  -> IF is_serial_prefetch_active(): ser_pref/_ext/_oo=>prepare(lt_tadir)  [RUN-SCOPED, once]
  -> LOOP AT lt_tadir:
       IF lv_max=1 OR mv_parallel_broken OR is_no_parallel(object):
         run_sequential(row)  -> zcl_abapgit_objects=>serialize() in-process
       ELSE:
         run_parallel(row, task=|{sy-tabix}|)                 [L635-707]
           -> CALL FUNCTION 'Z_ABAPGIT_SERIALIZE_PARALLEL' STARTING NEW TASK
              DESTINATION IN GROUP mv_group CALLING on_end_of_task ON END OF TASK
           -> WAIT UNTIL mv_free > 0 UP TO 120 SECONDS   (throttle: mv_free counts
              free worker slots, decremented on dispatch, incremented in
              on_end_of_task)
  -> WAIT UNTIL mv_free = lv_max UP TO 120 SECONDS (drain)
  -> CLEANUP/finally: ser_pref/_ext/_oo=>clear()
```

`Z_ABAPGIT_SERIALIZE_PARALLEL` (function group `zabapgit_parallel`) is
STRICTLY single-object: one `IS_TADIR` row in, one `EV_RESULT` xstring out,
`EV_PATH`, exceptions `ERROR`. It already accepts
`IV_PREFETCH_BUFFER`/`_EXT`/`_OO` (per-object slices, exported by the main
process via `extract_for_object` and injected in the worker via
`inject_from_buffer`). `on_end_of_task` imports the result, and on RFC
communication/system failure (`sy-subrc <> 0` on `RECEIVE`) logs a warning and
sets `mv_parallel_broken`-style degradation for the remainder of the run
(exact flag verified in standard sub-audit §1.5) — **no object is silently
dropped on RFC failure today; it degrades to sequential for the rest of the
run**, which is the existing fallback baseline any new design must not
regress below.

Worker count comes from `determine_max_processes` -> `get_environment(
)->init_parallel_processing(mv_group)`, capped at 50, exit-hook adjustable.
There is **no existing per-object cost estimation or ordering** in the
standard path (`SOURCE_CONFIRMED=NO`, standard sub-audit §5) — objects are
processed in TADIR primary-key order.

`zcl_abapgit_tadir=>select_objects` issues one whole-package-tree
`SELECT ... FOR ALL ENTRIES IN et_packages` (N-scoped); `it_filter` is
applied in memory afterwards; `check_exists` calls the ORTEC bulk-exists
hook when active, otherwise a per-row `zcl_abapgit_objects=>exists()` loop.

**No ORTEC call sites exist in `zcl_abapgit_objects.clas.abap` itself** — the
object-dispatch class is unmodified; all ORTEC hooks live in
`zcl_abapgit_serialize`, `zcl_abapgit_tadir`, `zcl_abapgit_objects_super`,
and the per-object-type classes (DTEL/ENHS/FUGR/MSAG/PROG/SMIM/TOBJ/TRAN/
CLAS-via-OO-base/WAPA).

## 2. Exact current ORTEC existence/prefetch/WAPA flow (see ORTEC sub-audit)

```text
ZCL_ABAPGIT_ORTEC_BULK_EXISTS         one static filter_existing(it_tadir),
                                       CASE-per-type FOR ALL ENTRIES, silent
                                       per-type fallback to exists_standard()
                                       on SQL failure or unsupported type.
                                       Types ALREADY covered: DOMA (always),
                                       DSYS, FUGR, MSAG, PROG, SHLP, SMIM,
                                       TOBJ, TRAN, TTYP (always), TABL/DTEL/
                                       CLAS/INTF (switch-gated, DEFAULT ON).
ZCL_ABAPGIT_ORTEC_SER_PREF            MSAG (T100/T100A/T100T) + DOKIL
                                       (documentation), RUN-scoped CLASS-DATA,
                                       clear()/prepare()/extract_for_object()/
                                       inject_from_buffer(), miss -> standard
                                       per-object SELECT.
ZCL_ABAPGIT_ORTEC_SER_PREF_EXT        10 caches: DTEL, ENHS, FUGR (areat/
                                       enlfdir/func_meta), PROG langs, SMIM
                                       (loio/phf), TOBJ, TRAN. Same lifecycle
                                       shape as SER_PREF. NO DOMA cache exists
                                       today (gap, see SER-3).
ZCL_ABAPGIT_ORTEC_SER_PREF_OO         CLAS/INTF description caches
                                       (classtx/compotx/subcotx), same
                                       lifecycle shape.
ZCL_ABAPGIT_ORTEC_WAPA                Direct O2APPL/O2PAGDIR*/O2PAGCON reads
                                       replacing cl_o2_api_*; page metadata is
                                       already FOR ALL ENTRIES bulk (per
                                       application); O2PAGCON content itself
                                       is a per-page cluster-table IMPORT
                                       (type-inherent, not bulk-able the same
                                       way). ZERO ABAP Unit tests exist.
```

All three `ser_pref*` classes already implement the exact "clear before
insert on inject" defensive pattern to prevent stale pooled-RFC-worker data
(cross-referenced, not re-litigated — this is the E2/OS4-adjacent fix already
recorded as fixed in `.memory/logs/variant_b_package_e_false_modified_os4_d1.md`).

## 3. Active feature-switch and fallback matrix

See ORTEC sub-audit §7 for the full table (`zcl_abapgit_ortec_git_switch`):
`mv_bulk_exists_active`, `mv_serial_prefetch_active`, `mv_avoid_timeout_active`,
plus per-type constants `cs_bulk_exists-{tabl,dtel,clas,intf}_active`, all
default `abap_true`. Every switch has a clean OFF path (falls back to
unmodified standard behavior) — **no switch currently gates a mechanism that
would need a NEW OFF path**; new SER-2/SER-3 work needs exactly one new
switch (`is_ser_batch_active`, see performance design).

## 4. Object-class modifications and hook inventory (already exists — do not
re-add)

```text
zcl_abapgit_serialize          run_parallel/run_sequential/serialize() —
                                prepare/clear hooks (L770-857)
zcl_abapgit_tadir               check_exists() — bulk-exists hook (L212)
zabapgit_parallel FM             inject_from_buffer x3 (worker-side)
zcl_abapgit_object_dtel/enhs/    per-object get_*_data() prefetch reads,
  fugr/msag/prog/smim/tobj/tran  each gated by is_serial_prefetch_active()
zcl_abapgit_oo_base              read_descriptions_class/compo/subco —
                                  ser_pref_oo hook, miss -> standard SELECT
zcl_abapgit_objects_super        serialize_longtexts() — ser_pref get_dokil()
zcl_abapgit_object_wapa          exists()/serialize() — wapa hook, gated by
                                  is_wapa_active()
```

## 5. Provider/object-type/table/API coverage matrix

| Type family | Existence bulk | Serialize prefetch | Gap |
|---|---|---|---|
| CLAS/INTF | YES (switch, default ON) | YES (ser_pref_oo: descriptions only; OO source/includes/text-pool/DOKIL/attrs still per-object via CL_OO_* APIs and D010TINF/DOKHL/SEOCOMPODF/VSEOEXTEND SELECTs, see standard sub-audit §3.3) | No batch-RFC grouping; still 1 RFC task/object |
| DTEL | YES (switch, default ON) | YES (ser_pref_ext mt_dtel) | No batch-RFC grouping |
| DOMA | YES (always) | **NO dedicated prefetch cache** | New provider needed (SER-3) |
| MSAG/PROG/SMIM/TOBJ/TRAN/FUGR/ENHS | YES/mostly | YES | No batch-RFC grouping |
| WAPA | Existence via wapa=>exists (direct) | YES (own class, not under ser_pref lifecycle) | No tests; not under provider contract |
| Everything else | Standard per-object | Standard per-object | Generic batch path only (SER-2) |

## 6. Verified trace interpretation (see attachments; values are
MEASURED from the supplied SAT exports, not re-run this session)

```text
classes-serial-main.txt          459 objects, sequential, ~27.4s in
                                  ZCL_ABAPGIT_SERIALIZE=>SERIALIZE,
                                  ~25.0s inside CLAS serialize (458 objects).
                                  Dominant: REPOSRC/CL_OO_CLIF_SOURCE include
                                  reads, DOKIL/longtexts, DWINACTIV/TMDIR/
                                  SEOCOMPODF/D010TINF/VSEOEXTEND, enqueue/
                                  dequeue of CS includes, XML build.
classes-parallel-main.txt        459 RUN_PARALLEL calls, 459 async RFC
                                  dispatches, central SERIALIZE only ~4.16s
                                  (workers absorb the rest) -> parallel
                                  already ~6.6x faster than sequential for
                                  this CLAS-only sample. MUST preserve this
                                  parallelism, not replace it with a
                                  sequential bulk step.
classes-parallel-worker2/3.txt   Individual CLAS costs vary materially
                                  (worker2 total ~567ms/object trace overhead
                                  incl. runtime-analysis tax vs worker3
                                  ~238ms) — object TYPE alone is not a
                                  sufficient weight predictor (confirms
                                  brief's own claim).
classes-parallel-worker1.txt     NOT a serialization worker trace (ADT/
                                  Gateway error-log REST request) — EXCLUDED
                                  per brief's own instruction, confirmed by
                                  content inspection (SADT_REST_RFC_ENDPOINT/
                                  error-log feed provider calls, zero
                                  Z_ABAPGIT_SERIALIZE_PARALLEL references).
SAT-WarmToColdBranchWithMinChanges.txt  17,148 RUN_PARALLEL calls / 18,547
                                  SPBT_PARALLEL_PROCESSING calls at a much
                                  larger repo scale. SPBT/task-dispatch
                                  machinery itself (SPBT_GET_CURR_RESOURCE_
                                  INFO, CHECK_SRV_STILL_ACTIVE, TH_ARFC_
                                  REQUESTS, ThSysInfo, GET_SERVER_PBT_
                                  RESOURCES) totals roughly 12-15% of GROSS
                                  time EACH at this scale — this is the
                                  strongest evidence in the whole dataset
                                  for "fewer, larger RFC tasks" as the
                                  primary lever, independent of per-object
                                  ABAP cost. This single fact is the main
                                  quantitative justification for SER-2.
SAT-ColdBranch.txt                Attached but not separately re-summarized
                                  here (Git-side cold-branch path, already
                                  covered by Package B/C/D evidence,
                                  cross-referenced not re-derived).
```

## 7. Known correctness, memory, lifecycle, observability gaps

```text
G-1  ser_pref*/wapa are RUN-scoped CLASS-DATA, not batch-scoped — acceptable
     today because they are sized to the caller's already-filtered lt_tadir,
     but the new batch orchestrator must not silently assume this changes;
     SER-3 keeps prepare() run-scoped and only makes *extraction* per-batch.
G-2  No DOMA prefetch cache exists — DOMA serialization pays the standard
     per-object DD01L/DD01V/DD01T/DD07L/DD07T reads today (SER-3 gap).
G-3  ZCL_ABAPGIT_ORTEC_WAPA has ZERO ABAP Unit tests (real regression-safety
     gap, flagged as a mandatory prerequisite in SER-5, independent of any
     architecture change).
G-4  The existing single-object RFC contract re-transmits repo-wide-constant
     settings (iv_abap_language_vers, iv_language, iv_path, iv_main_
     language_only, iv_suppress_po_comments, it_translation_langs, iv_use_
     lxe) on EVERY task — these never vary per object within one serialize()
     call. Hoisting them to a batch header is a free simplification, not
     just a batching side-effect.
G-5  No existing per-batch/per-object timeout or duplicate/late-callback
     guard is visible in the standard on_end_of_task path beyond the blanket
     120-second WAIT and mv_parallel_broken degradation — SER-2 must add
     its own bounded guards without assuming any hidden protection exists.
G-6  Whether ZCL_ABAPGIT_SERIALIZE exposes any user-cancellation flag was
     NOT confirmed this pass (UNKNOWN) — SER-2 treats cancellation as an
     owner-decision/verification item, not an invented mechanism.
```

## 8. Reusable / replace / retire recommendations (per class)

| Class | Disposition | Reason |
|---|---|---|
| ZCL_ABAPGIT_ORTEC_BULK_EXISTS | KEEP (verify only) | CLAS/INTF/DTEL/DOMA already covered; SER-1 needs no new implementation for the prototype scope, see SER-1 design |
| ZCL_ABAPGIT_ORTEC_SER_PREF | KEEP_AND_REFACTOR | add `extract_for_batch`, no behavior change to existing methods |
| ZCL_ABAPGIT_ORTEC_SER_PREF_EXT | KEEP_AND_REFACTOR | same, plus new DOMA cache added via a sibling class (see SER-3), not inside this one, to avoid growing an already-10-cache class further |
| ZCL_ABAPGIT_ORTEC_SER_PREF_OO | KEEP_AND_REFACTOR | add `extract_for_batch`; candidate to be wrapped by a thin provider facade (SER-3) |
| ZCL_ABAPGIT_ORTEC_WAPA | KEEP_WITH_CORRECTIONS | add tests first (mandatory prerequisite); defer provider-contract wrapping to a later, separately reviewed slice (SER-5) |

## 9. Measurements available now vs. must be gathered during implementation

```text
AVAILABLE NOW   Sequential vs parallel wall-clock and per-statement cost
                shape for a CLAS-only 459-object sample (all 5 attached
                traces); SPBT dispatch-overhead-at-scale evidence (17k-task
                trace).
MUST GATHER     Batch-vs-single-object wall time/DB/RFC-count on the SAME
                CLAS-only fixture once SLICE 1/2 exist (before/after);
                mixed CLAS/INTF/DTEL/DOMA fixture; DOMA-only baseline (no
                current DOMA-specific trace exists to compare against);
                peak memory during a full non-filtered 40,000-object
                serialization (still UNKNOWN per the prior Git-side backlog's
                M-3, unresolved, cross-referenced not repeated here).
```
