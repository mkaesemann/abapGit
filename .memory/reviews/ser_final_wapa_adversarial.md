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
