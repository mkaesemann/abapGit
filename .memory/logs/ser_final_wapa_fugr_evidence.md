# SER-FINAL — WAPA/FUGR/DDLS evidence reconciliation (2026-08-10)

`BASELINE_HEAD=d1056132e9f84365f3786a677b83372532ed69b4` (SER-SLICE-5 IT8
closeout commit; clean working tree except an untracked memory-dump text
file, unrelated). No code changed by this pass yet.

## Trace classification (mandatory, done before any conclusion)

| File | Classification | Basis |
|---|---|---|
| `WAPA Set - Normal - Worker 1/2/3.txt` | TRUE_WORKER, standard path | `CL_O2_API_PAGES=>LOAD`, `->READ_FROM_DB`, `Import From Database O2PAGCON` — these are `ZCL_ABAPGIT_OBJECT_WAPA`'s legacy per-page API calls, never `ZCL_ABAPGIT_ORTEC_WAPA`. "Normal" run ⇒ `ZCL_ABAPGIT_ORTEC_SER_ORCH` never runs ⇒ `is_wapa_active()`/`is_serial_prefetch_active()` never turned on for this run's duration ⇒ pure standard path is *expected*, not a bug. |
| `FUGR Set - Normal - Worker 1/2/3.txt` | TRUE_WORKER, standard path | Same reasoning; `RFC Z_ABAPGIT_SERIALIZE_PARALLEL`-driven (standard's own parallel dispatch), not the ORTEC batch RFC. |
| `FUGR Set - Batch - Worker 1/2/3.txt` | TRUE_WORKER, ORTEC batch path | Dedicated FUGR extraction, driven under `is_serial_batch_active`. |
| `FUGR+DDLS+WAPA - Normal - Main.txt` | MAIN, standard path | `ZCL_ABAPGIT_SERIALIZE`, `Rfc Z_ABAPGIT_SERIALIZE_PARALLEL` (standard dispatcher), `Call Screen 1001` (UI/main session marker). |
| `FUGR+DDLS+WAPA - Normal - Worker.txt` | TRUE_WORKER, standard path | Same RFC (`Z_ABAPGIT_SERIALIZE_PARALLEL`) executing inside the worker session. |
| `FUGR+DDLS+WAPA - Batch - Main.txt` | MAIN, ORTEC batch path | `ZCL_ABAPGIT_ORTEC_SER_ORCH`, `Rfc Z_ABAPGIT_ORTEC_SER_BATCH`, `Call Screen 1001`. |
| `FUGR+DDLS+WAPA - Batch - Worker.txt` | TRUE_WORKER, ORTEC batch path | `ZCL_ABAPGIT_ORTEC_SER_ORCH=>ON_END_OF_BATCH`/`DISPATCH_BATCH` executing inside the aRFC callback session — this is the authoritative worker-side evidence for this pass. |

None of the WAPA-dedicated traces exercise `ZCL_ABAPGIT_ORTEC_WAPA` (no
"Batch" WAPA-only trace was supplied). The mixed Batch traces do not show
enough isolated WAPA hit-list detail (their summarized hit lists are
dominated by FUGR/DDLS/status-calc rows) to prove or disprove multi-WAPA
batching benefit. Per the mission's own caveat, this evidence is
sufficient for end-to-end orchestration/coverage conclusions, but **not**
sufficient to justify a WAPA content-batching architecture change.

## WAPA reconciliation

- Standard path (`ZCL_ABAPGIT_OBJECT_WAPA~SERIALIZE`, `is_wapa_active()=
  FALSE`): `cl_o2_api_application=>load` → `get_navgraph` → `cl_o2_api_
  pages=>get_all_pages` → per-page `read_page()` → `cl_o2_api_pages=>load`
  + `get_attrs`/`get_event_handlers`/`get_parameters`/`get_type_source`
  + `get_page_content()` → `io_page->get_page` (which internally does the
  `IMPORT ... FROM DATABASE o2pagcon(tr)` per page). This matches the
  `WAPA Set - Normal` traces exactly (159/61 hits on `CL_O2_API_PAGES=>
  LOAD`/`READ_FROM_DB`, `DB: Open/Import O2PAGCON`).
- ORTEC replacement (`ZCL_ABAPGIT_ORTEC_WAPA=>serialize`, already
  implemented, source read this pass): `build_context()` does **bulk**
  `SELECT ... FOR ALL ENTRIES` on `O2PAGDIR`/`O2PAGDIRT`/`O2PAGEVH`/
  `O2PAGPAR`/`O2PAGPART` for every page in the app in 5 statements total,
  eliminating the per-page `GET_ATTRS`/`GET_EVENT_HANDLERS`/
  `GET_PARAMETERS`/`GET_TYPE_SOURCE` API round-trips entirely. It still
  does one `IMPORT ... FROM DATABASE o2pagcon(tr) ID <key>` per page per
  needed `objtype` (content always; `evhandler`/`types` additionally for
  full-type pages) — this is intrinsic to `O2PAGCON` being a **cluster**
  table keyed by `(applname, pagekey, objtype, version)`; ABAP has no
  bulk/multi-key `IMPORT` for cluster tables, and reconstructing cluster
  storage directly would violate the standing WAPA safeguard ("no direct
  cluster-table interpretation without proven parity").
- `exists()` replacement: single `SELECT SINGLE applname FROM o2appl`
  instead of `cl_o2_api_application=>load`. Already implemented,
  unaffected by this pass.
- Singleton construction: WAPA is batch-eligible **only** as a singleton
  batch (never mixed with another WAPA or any non-WAPA object) — this is
  a binding invariant in `.memory/state.md`, not something this pass
  proposes to change, and no evidence gathered this pass contradicts it.
- Per-WAPA memory lifetime / static state: `ZCL_ABAPGIT_ORTEC_WAPA` is
  `CREATE PRIVATE`, stateless (`CLASS-METHODS` only, no `CLASS-DATA`) —
  each call builds and discards its own `ty_context`; no cross-WAPA/
  session state exists to leak.
- Conclusion: the one remaining WAPA cost family visible in evidence
  (`O2PAGCON` cluster imports) is **already** the theoretical minimum
  given the storage shape and the safeguard against direct cluster
  reinterpretation. No further safe intra-object win identified.

## FUGR reconciliation (call-path/coverage matrix)

| Read | Where (method) | Provider seam exists? | Confirmed HIT in true-worker evidence? | Avoidable? |
|---|---|---|---|---|
| `ENLFDIR` (function list) | `functions()` | Yes — `get_fugr_enlfdir()`, populated by `before_dispatch`→`prepare_fugr(it_areas)` for the **current dispatch's** areas | Yes for the SERIALIZE-path worker traces: `FUGR Set - Batch - Worker 2` shows only 2 direct `DB: Fetch ENLFDIR` hits against ~19-40 `RPY_FUNCTIONMODULE_READ_NEW`/`Loop At LT_FUNCTAB` object-scale hits ⇒ provider mostly HIT | Already largely avoided |
| Function metadata (`rfcscope`/`rfcvers`) | `functions()`/direct dynamic SELECT | Yes — `get_fugr_func_metadata()` | Consistent with ENLFDIR HIT above | Already largely avoided |
| `AREAT` (group short text) | `update_func_group_short_text`/read path | Yes — `get_fugr_areat()` | Not separately isolated this pass | Already covered |
| PROG text-pool languages (i18n) | `serialize` i18n block | Yes — `get_prog_tpool_languages()` | Not separately isolated this pass | Already covered |
| `RS_GET_ALL_INCLUDES` | `includes()` **and independently** `zif_abapgit_object~changed_by()` (own `mt_includes_all` cache) | **No** | `FUGR+DDLS+WAPA - Batch - Worker.txt`: `RS_GET_ALL_INCLUDES` = 147 hits, exactly matching `CHANGED_BY` = 147 hits and `FUNCTIONS` = 147 hits (internal caller = `ZCL_ABAPGIT_OBJECT_FUGR`) — confirms `CHANGED_BY` alone drives all 147 calls in this trace (no evidence `includes()` executed in this same scope) | Only avoidable via a **new**, differently-scoped prefetch (see design log) |
| `REPOTEXT`/`REPOSRC`/`EUDB`/`D010INC`/`RSEUINC` (author/date/time stamps) | `zif_abapgit_object~changed_by()` only | **No** | Same trace: each of these shows exactly 147 hits, ~150-250ms gross each, ≈1.3s aggregate DB time for this trace's FUGR set, 100% direct, 0% provider-covered | Same as above |
| `RS_FUNCTION_POOL_CONTENTS` | `functions()` | No dedicated seam (ENLFDIR seam only covers the cross-check SELECT, not this FM) | 147 hits, matches object count exactly — this FM has no bulk/batch variant in the standard API | Not avoidable without reimplementing FM's own logic (rejected — out of scope) |
| Dynpro (`D020S`/`D020T`/`D021T`, `RPY_DYNPRO_READ*`) | `ZCL_ABAPGIT_OBJECTS_PROGRAM=>SERIALIZE_DYNPROS` (shared with PROG) | No | Dominant absolute cost in every FUGR/PROG trace (hundreds of ms–seconds per object with many screens) | No — each Dynpro is unique per program; nothing to share across objects; direct-table reconstruction is explicitly the rejected Candidate E pattern |

### Key finding — architectural scope mismatch, not a regression

`is_serial_prefetch_active()` is turned on for the **whole duration of
`ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE`'s run** (confirmed by source:
`zcl_abapgit_ortec_git_switch=>is_wapa_active`/`is_serial_prefetch_active`
doc comments + call site at ORCH line ~921/966), so the flag genuinely is
on inside the RFC worker (this is the SLICE5-001 fix working as intended
— see "SLICE5-001 confirmation" below). But the FUGR ENLFDIR/func-
metadata **cache** (`mt_fugr_enlfdir`/`mt_fugr_func_meta`) is populated by
`prepare_fugr(it_areas)`, called from `before_dispatch` **only for the
areas in the batch currently being dispatched for SERIALIZE**. `CHANGED_
BY` is invoked by `ZCL_ABAPGIT_OBJECTS` for a much broader sweep (status
calculation across the whole repository, 147 FUGR objects in this trace
vs. the much smaller per-dispatch batch sizes seen elsewhere, e.g. 19-40
objects per FUGR-only worker), so most `CHANGED_BY` calls fall outside
any single dispatch's prefetch scope and always MISS — this is **not** a
gate-activation bug (SLICE5-001 already fixed that); it is an unaddressed
**coverage gap**: the existing provider is scoped to the dispatch/
SERIALIZE lifecycle, not to the separate, broader CHANGED_BY/status-calc
sweep. See `ser_final_fugr_design.md` Candidate B for the follow-up shape.

### SLICE5-001 confirmation (closes the SER-SLICE-5 `OWNER_ACTION_REQUIRED`)

The fresh SAT evidence supplied this pass is the requested retest: the
true-worker `FUGR Set - Batch - Worker` traces show the FUGR ENLFDIR/
function-metadata provider **actually being consulted and mostly HIT**
inside the RFC worker's own aRFC session (only 2 direct-DB fallbacks
against ~19-40 objects). This is concrete, positive, worker-side proof
that `is_serial_prefetch_active()`/`is_wapa_active()` are live inside
`Z_ABAPGIT_ORTEC_SER_BATCH`'s own session, i.e. SLICE5-001 is confirmed
fixed and working in this evidence. No further SAT retest is required to
close that specific residual.

## DDLS reconciliation (bounded, source/evidence only — no implementation)

`CL_DD_DDL_HANDLER=>GET`/`GET_ALL`/`GET_INDX`/`GET_TS` and the underlying
`DDDDLSRC*` table opens/fetches occur once per DDLS object (761-763 hits
matching object count in every Main/Worker Batch trace), with per-call
net cost in the low-microsecond-to-single-millisecond range (e.g.
`GET_ALL` net 19-31ms **total** across all 761-763 calls; `GET_INDX` net
~26.5-32.6ms total). No duplicate/repeated read pattern (unlike FUGR's
`CHANGED_BY` vs `SERIALIZE` split) was found for DDLS in this evidence —
each DDLS object's handler calls are already singular. The gross time is
large in aggregate (multi-second across ~760 objects) but is attributable
to `CL_DD_DDL_HANDLER`'s own internal release-dependent processing, not
to duplicated caller-side work. No safe, bounded, handler/result-reuse
optimization is evidenced this pass. See `ser_final_ddls_disposition.md`.
