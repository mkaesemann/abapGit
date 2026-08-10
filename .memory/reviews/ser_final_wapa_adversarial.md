# SER-FINAL — WAPA adversarial review (2026-08-10, condensed — no productive diff to attack)

Since Candidate D (no change) was selected, this review's job is to
attack the *decision not to change anything*, not a diff.

| Attack | Verdict | Evidence |
|---|---|---|
| False interpretation of aggregate traces as true workers | PASS | Trace classification table in evidence log explicitly separates MAIN/AGGREGATE/TRUE_WORKER before any conclusion; WAPA-specific conclusions were drawn only from the dedicated `WAPA Set` TRUE_WORKER traces |
| Assuming "no batching evidence" excuses missing a real, cheap win elsewhere | CHALLENGED — is there a cheap win hiding in the *standard* legacy path shown by `WAPA Set - Normal`? | REJECTED as in-scope: those traces are feature-OFF pure-standard by construction; touching `ZCL_ABAPGIT_OBJECT_WAPA`'s legacy body itself would violate "Do not rewrite standard abapGit object serializers" |
| WAPA state leakage / oversized payloads | N/A — no code changed | — |
| Cluster/import semantic mismatch | N/A — no code changed; existing `ZCL_ABAPGIT_ORTEC_WAPA` reviewed, uses the same `IMPORT ... FROM DATABASE o2pagcon(tr)` primitive as before | PASS |
| Incomplete page sets or ordering drift | N/A — no code changed | — |
| Multi-WAPA partial success | N/A — not implemented | — |
| Feature-OFF contamination | PASS — `is_wapa_active()` gate untouched | — |
| Memory amplification | N/A — no code changed | — |

## Verdict

`APPROVE` — no BLOCKER/MAJOR findings against the "no change" decision.

## SER-FINAL-CORRECTION cycle (2026-08-10) — attacking the per-candidate dispositions, not a diff

No WAPA code was implemented this pass either (candidates 1/4 are
source-provably empty; candidates 2/3/5 are blocked on IT8 evidence this
workspace cannot produce). This cycle attacks whether those dispositions
themselves are sound, per the mission's explicit WAPA attack list.

| Attack | Verdict | Evidence |
|---|---|---|
| WAPA page-set completeness and ordering | N/A - no implementation | Candidates 1/4's rejection proof is about *duplicate* reads, not page-set completeness; unaffected either way |
| Cluster/import semantic parity | Directly the subject of Candidate 2's blocker | The IT8 experiment (`ser_final_wapa_it8_experiment.md` Step 3) is specifically designed to prove or disprove this before any implementation is authorized - correctly deferred, not assumed |
| WAPA memory bounds | N/A | No new buffers/caches proposed |
| Multi-WAPA admission/splitting | N/A | Candidate 5 correctly not implemented without `EXPECTED_GAIN=YES` evidence (Step 4) |
| Cleanup after exceptions | N/A | No new resources allocated |
| Feature-OFF purity | PASS | No `ZCL_ABAPGIT_OBJECT_WAPA`/`ZCL_ABAPGIT_ORTEC_WAPA` code touched this pass |
| No cross-object/request state | N/A | No new state |
| No fallback that pays preload cost and repeats all old work | N/A - the pattern this attacks doesn't exist yet (nothing implemented) | If Candidate 2 is later implemented, the experiment plan's own pass/fail criteria (Step 3) mandate "a mandatory fallback to today's per-key IMPORT ... FROM DATABASE on any decode mismatch/exception" - this specific attack vector is pre-emptively addressed in the design sketch, to be enforced at implementation time |
| **Are candidates 1/4's "no duplicate read" proofs actually exhaustive, or do they miss a call path?** | Re-verified | `grep`-confirmed: `read_page` has exactly one caller (`serialize`'s `LOOP AT lt_pages`); `add_page_content_file`/`add_full_page_details` are each called exactly once, from `read_page` only; no recursive/repeated invocation anywhere in `zcl_abapgit_ortec_wapa.clas.abap`. The proof is exhaustive over the *entire* class, not a sampled subset |
| **Is "BLOCKED_BY_REQUIRED_IT8_EXPERIMENT" being used as a disguised rejection to avoid doing the work?** | Rejected as a finding | A concrete, ordered, 4-step experiment with exact objects/breakpoints/pass-fail thresholds was produced (`ser_final_wapa_it8_experiment.md`) - this is a genuine blocker (live SE11/ADT/debugger access this workspace does not have), not a stand-in for "we didn't look" |

Verdict: 0 BLOCKER, 0 MAJOR. `APPROVE` (dispositions sound; the mission's
own required IT8 experiment is the correct next step for candidates
2/3/5, not a further design or implementation attempt from this session).

## SER-FINAL apply-IT8-results cycle (2026-08-10) — implementation adversarial review

Diff under review: `src/ortec/serial/zcl_abapgit_ortec_wapa.clas.abap`
(new `try_raw_prefetch`/`build_requested_keys`/`read_raw_rows`/
`assemble_and_decode`/counters, and the conditional read in
`add_page_content_file`/`add_full_page_details`), plus
`.clas.testclasses.abap`. This is a genuine implementation-adversarial
pass, kept separate from `ser_final_correctness.md` per owner instruction.

| Attack | Verdict | Evidence |
|---|---|---|
| Incomplete logical key | PASS | Key = `(pagekey, objtype)` with `applname`/`version` fixed per call; `read_raw_rows`'s `WHERE` filters on all four (`relid` literal + `applname` param + `pagekey`/`objtype` from the driver table + `version` literal) - no key component can be silently dropped |
| Interleaved physical rows | PASS | `assemble_and_decode` looks up/creates the group buffer by `(pagekey, objtype)` **per row**, not by assumed contiguity; `interleaved_rows_multi_keys` test proves this directly with a hand-interleaved input table |
| Wrong SRTF2 order | PASS | Defensive `SORT lt_sorted BY pagekey objtype srtf2 ASCENDING` before grouping, independent of what order the SQL or a test supplies; `multi_row_ordered_by_srtf2` test feeds a deliberately `DESCENDING`-sorted input |
| Ignored CLUSTR truncation | PASS | `lv_chunk = <ls_row>-clustd(<ls_row>-clustr)` - a single, explicit truncating assignment, never the full `clustd` field; `clustr_truncation_ignores_pad` test proves garbage bytes beyond `clustr` are never included in the decoded result |
| Missing or duplicate sequence rows | PASS | `next_srtf2` counter per group raises on any `srtf2 <> next_srtf2` - this rejects both a gap (skipped number) and a duplicate (repeated number) identically; `duplicate_srtf2_raises`/`gapped_srtf2_raises` tests cover both |
| Absent optional EVHNDL/TYPES records | PASS | `assemble_and_decode`'s final loop inserts an empty map entry for a requested-but-rowless EVHNDL/TYPES key (not an anomaly), but raises for a requested-but-rowless PAGE key (mirrors the reference path's own hard error) - `optional_evhndl_absent_is_empty`/`optional_types_absent_is_empty`/`missing_page_content_raises` tests cover all three |
| Active/inactive/version mixing | PASS | `read_raw_rows`'s `WHERE version = @c_active` is unconditional and literal - never parameterized from row data, so an inactive-version physical row can never enter the candidate result; `read_raw_rows_filters_by_key` test explicitly inserts an inactive-version row for the same key and proves it is excluded |
| Large multi-row payloads | PASS (design), not independently re-measured this pass | Row cap (20000) and byte cap (20 MB, shared with `ZCL_ABAPGIT_ORTEC_SER_ORCH`) bound this; IT8 Experiment 2 already proved successful reconstruction up to 1126 rows/~3 MB for a single real key, well inside both caps |
| SQL parameter/statement limits | PASS | Exactly one `SELECT ... FOR ALL ENTRIES` per WAPA, bounded by `UP TO n ROWS`; no per-row or per-key statement is issued on the raw-prefetch hit path |
| Memory amplification (raw rows + reconstructed buffers + decoded tables + serializer output) | PASS | All four are bounded by the same 20 MB/20000-row ceiling and are local to one `serialize()` call's stack/context; no table holds more than one WAPA's worth of data at a time (see design log decision 12) |
| Decode failure after partial reconstruction | PASS | Eager, whole-WAPA validate-and-decode (design decision 7) - `assemble_and_decode` either fully succeeds (all keys decoded) or raises before `try_raw_prefetch` ever sets `raw_prefetch_active = abap_true`; no page can have already consumed candidate data when a later key's decode fails |
| Fallback that combines candidate and reference data | PASS | `raw_prefetch_active` is a single, whole-context boolean; `add_page_content_file`/`add_full_page_details` never read `is_context-raw_*` unless it is `abap_true`, and never partially apply it (each is an independent `IF ... ELSE <original>` per sub-key, but the *decision to have raw data available at all* was already made atomically for the whole WAPA) |
| Fallback retaining large candidate buffers | PASS | On any anomaly, `try_raw_prefetch` simply `RETURN`s without assigning `lt_content`/`lt_evhandler`/`lt_typesource` into `cs_context` - the local buffers go out of scope and are garbage-collected normally; nothing oversized is retained |
| Feature-OFF contamination | PASS | No change to `ZCL_ABAPGIT_OBJECT_WAPA`'s `is_wapa_active()` gate or any other feature-OFF code path; `zcl_abapgit_ortec_wapa` is unreachable when the feature is off, unchanged |
| Output ordering/hash drift | PASS (design + IT8 Experiment 3), not independently re-measured this pass | The reference path's own downstream logic (language conversion, `add_raw`, XML assembly) is completely untouched - only the *source* of `lt_content`/`lv_xml_source`/`lt_ev_handler_sources`/`cs_page-types` is substituted, with IT8-proven byte/hash parity for the substituted values themselves |
| Cross-WAPA or cross-request state | PASS | No `CLASS-DATA` holds page content; only the two lightweight `i`-typed hit/fallback counters are session-lifetime, and they carry no page data |
| Accidental multi-WAPA planner change | PASS | Confirmed via `git diff --name-only` - only `src/ortec/serial/zcl_abapgit_ortec_wapa.clas.*` changed; `zcl_abapgit_ortec_ser_orch.*` untouched |

Verdict: 0 BLOCKER, 0 MAJOR. `APPROVE`. Cycles 2-3 not required (0 findings
on cycle 1, no revision made).
