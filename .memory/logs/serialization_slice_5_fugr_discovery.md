# SER-SLICE-5 Phase 3 — FUGR remaining-potential discovery

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_5_FUGR_DISCOVERY
STATUS=DISCOVERY_COMPLETE
DEPENDS_ON=serialization_final_two_path_trace_audit.md (finding SLICE5-001)
```

## FUGR decomposition (both traces, 358 FUGR objects in each)

| Category | Evidence | Disposition |
|---|---|---|
| A. already covered metadata/directory reads | `mt_fugr_areat`/`mt_fugr_enlfdir`/`mt_fugr_func_meta` populated by `prepare_fugr` (verified correct FOR ALL ENTRIES TLIBT/ENLFDIR bulk read, keyed by `area`) | Correctly implemented, but see B - never actually consumed in production until this session's fix |
| B. repeated metadata reads that bypass the provider unexpectedly | `DB: Fetch/Open ENLFDIR` (358 hits), `DB: Fetch TCDRP`/`Select Single TCDRP` (358), all present at IDENTICAL counts and near-identical net time in BOTH Normal and Batch traces | **CONFIRMED - root cause is SLICE5-001** (worker never set `is_serial_prefetch_active=true`), not a provider-logic bug. `functions()`/`serialize_xml()` in `zcl_abapgit_object_fugr.clas.abap` correctly check the provider first; the provider itself (`prepare_fugr`/`get_fugr_enlfdir`/`get_fugr_areat`) is correctly implemented and correctly injected - it was simply never reachable |
| C. source/include reads suitable only for standard per-object access | `DB: Fetch REPOSRC/REPOTEXT/D010INC/RSEUINC/EUDB`, `Call Function RS_GET_ALL_INCLUDES`, `GE_FUNCTION_LIST`, `FUNCTION_INCLUDE_SPLIT`, `RS_FUNCTION_POOL_CONTENTS` (all 358, identical in both traces - NOT gated by any provider check in source, by design; SER-SLICE-4's own approved design explicitly scoped these out as Option C, MEASURE_FIRST) | Unchanged this slice - no new evidence collected for a source/include provider |
| D. source/include data possibly suitable for bounded batch prefetch | Not measured - would require isolating REPOSRC/D010INC/RSEUINC byte volume per function group at scale, a dedicated ST05/SAT experiment not performed this slice | DEFERRED, see &sect;"Focused measurement plan" |
| E. duplicate API work within one FUGR serialization | `Call M. ZCL_ABAPGIT_OBJECT_FUGR->FUNCTIONS` (358, calls `RS_FUNCTION_POOL_CONTENTS` then conditionally `SELECT * FROM enlfdir`) and a SEPARATE `Fetch/Open ENLFDIR` elsewhere in the same object's serialize flow (e.g. `serialize_xml`/`get_fugr_main_program`) - both correctly reuse the SAME provider cache when it is populated; no NEW duplication found beyond what SER-SLICE-4's own FG-001..004 findings already closed | No new finding |
| F. calls made in status/CHANGED_BY rather than serialization | `Call M. {O:*ZCL_ABAPGIT_OBJECT_FUGR}->ZIF_ABAPGIT_OBJECT~CHANGED_BY` (358, ~6.1-6.6M gross, standard status-check path, unrelated to the serializer provider) | Confirmed out of provider scope, matches the task's own caution not to attribute all FUGR trace cost to the serializer |

## Candidate options re-evaluated in light of SLICE5-001

```text
F1 NO_FURTHER_CHANGE            Rejected - the provider has real, unmeasured
                                 potential once actually reachable.
F2 REPAIR_PROVIDER_COVERAGE     SELECTED. The "repair" is SLICE5-001's fix
                                 (already applied, source-verified, get_errors
                                 clean, NOT yet IT8-validated). No FUGR-specific
                                 code change is needed beyond that one shared
                                 fix - `prepare_fugr`/`get_fugr_enlfdir`/
                                 `get_fugr_areat`/`extract_for_batch_fugr`/
                                 `inject_batch_from_buffer_fugr` were already
                                 correct.
F3 BATCH_INCLUDE_DIRECTORY_ONLY Not needed - AREAT/ENLFDIR/FUNC_META already
                                 batched (category A); no new include-directory
                                 gap identified.
F4 BOUNDED_SOURCE_PROVIDER      Not authorized - no measured evidence this
                                 slice; category D above remains open.
F5 OPTIMIZE_STANDARD_INTERNAL_
   DUPLICATION                  Not needed - no new duplication found beyond
                                 SER-SLICE-4's own closed findings.
```

```text
FUGR_DECISION=REPAIR_EXISTING_PROVIDER_COVERAGE
```

Expected benefit after the fix: every one of the 358 FUGR objects per full-repo
run should now show `provider_hit=1` AND a real, measurable drop in `DB: Fetch/
Open ENLFDIR`/`TCDRP` native SQL (category B) - the exact ENLFDIR/AREAT/
FUNC_META reads already batched in category A. Category C (source/include)
remains untouched and unauthorized.

## Focused measurement plan (for category D, if pursued later)

If a full source/include FUGR provider is ever proposed, the smallest focused
experiment is: capture ONE more SAT trace pair (Normal vs Batch, WITH the
SLICE5-001 fix applied) on a FUGR-heavy repository, and specifically diff
`DB: Fetch REPOSRC`/`D010INC`/`RSEUINC`/`EUDB` gross+net between the two - if
these remain large AND identical between runs (as category C predicts, since
they are NOT provider-gated today), that is the evidence needed to size a
bounded source/include provider design. This can be owner-executed alongside
the IT8 retest requested for SLICE5-001 - no separate trace capture is
required.

```text
FUGR_DECISION=REPAIR_EXISTING_PROVIDER_COVERAGE (fix applied, IT8 pending)
```
