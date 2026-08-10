# SER-FINAL — FUGR adversarial review (2026-08-10, condensed — no productive diff to attack)

| Attack | Verdict | Evidence |
|---|---|---|
| Provider HIT that still executes direct DB/API work | **CONFIRMED FINDING, NOT A REGRESSION** — `CHANGED_BY`'s internal `functions()`/`RS_GET_ALL_INCLUDES` calls are a 100% MISS in the supplied true-worker trace (147/147), because `prepare_fugr` is scoped to the currently-dispatched batch's areas, while `CHANGED_BY` runs across the whole repository status-calc sweep. This is documented as the Candidate B follow-up, not silently accepted as "fine" | `ser_final_wapa_fugr_evidence.md` §"Key finding" |
| False interpretation of aggregate traces as true workers | PASS | Same classification discipline as WAPA; the FUGR conclusion (SLICE5-001 confirmed live) was drawn only from `FUGR Set - Batch - Worker` (dedicated, TRUE_WORKER) traces, not the mixed Main trace |
| Duplicated versus intentionally separate FUGR API calls | Investigated precisely: `functions()`/`RS_GET_ALL_INCLUDES` are called once each from `CHANGED_BY` in the analyzed trace scope (not duplicated *within* one instance's `CHANGED_BY` call, thanks to `mt_includes_all`'s existing `IF ... IS INITIAL` guard) — the real gap is *cross-lifecycle* (dispatch-scoped prefetch vs. repo-wide status sweep), not intra-call duplication | Exact `Select-String` grep of the raw trace file (147/147/147/147 for `CHANGED_BY`/`FUNCTIONS`/`RS_GET_ALL_INCLUDES`/`RS_FUNCTION_POOL_CONTENTS`) |
| FUGR active/inactive/version/language drift | N/A — no code changed | — |
| Dynpro release compatibility | N/A — Candidate D/E rejected, no code proposed | — |
| Feature-OFF contamination | PASS — no code changed | — |
| Performance gain erased by marshaling | N/A | — |

## Challenge: "is F (no change) too conservative given a concrete, evidenced ~1.3s/trace gap was found?"

Yes, this is a real, evidenced, valuable gap. It is deliberately **not**
implemented this pass because: (1) it requires a still-undiscovered call
site (the repo-wide `CHANGED_BY` sweep orchestrator) this pass did not
read; (2) it touches `CHANGED_BY`, which is correctness-sensitive for CTS
integration (`ZCL_ABAPGIT_CTS_INTEGRATION=>FIND_CHANGED_BY`/`CHANGED_BY_
BULK`, confirmed live in the trace); (3) getting the multi-table tie-break
semantics (`SORT ... DESCENDING`, `iv_extra` override) bit-for-bit right
without a dedicated design+adversarial pass risks a silent, hard-to-detect
"changed by" metadata regression — exactly the class of risk this mode's
gates exist to prevent. Recorded as a named, well-specified follow-up
rather than either rushed or silently dropped.

## Verdict

`APPROVE` — 0 BLOCKER/MAJOR against the "no implementation this pass"
decision; 1 concrete, well-documented, evidenced follow-up recorded.
