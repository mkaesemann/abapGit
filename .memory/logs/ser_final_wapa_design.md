# SER-FINAL — WAPA design (2026-08-10)

## Candidates evaluated

- **A. Intra-object page-content prefetch/cache for one WAPA** — REJECTED.
  `ZCL_ABAPGIT_ORTEC_WAPA` already bulk-loads every directory/text/
  handler/parameter table for the whole app in `build_context()` (5
  `SELECT ... FOR ALL ENTRIES` statements total, replacing N×4 API calls).
  The only remaining per-page cost is the `IMPORT ... FROM DATABASE
  o2pagcon(tr) ID <key>` cluster read, which is genuinely one-key-at-a-
  time by ABAP language design (no bulk/multi-key cluster IMPORT exists)
  and there is no repeated/duplicate import of the *same* page within one
  object today — the current implementation calls it exactly once per
  `(pagekey, objtype)` needed. A cache would have nothing to reuse.
- **B. Bounded multi-WAPA batching** — REJECTED this round. No true-
  worker evidence exists for the ORTEC WAPA batch path at all (no
  dedicated "WAPA Set - Batch" trace was supplied, and the mixed Batch
  traces do not isolate WAPA hit-list detail). The mandatory safeguard
  ("Start with a conservative bounded policy and prove split/fallback
  behavior") is not satisfiable without that evidence. Implementing an
  admission policy (object-count + byte/estimated-output bound) without
  proof of an actual per-page/per-WAPA cost that benefits from batching
  (batching primarily amortizes RFC/dispatch overhead, not per-page
  cluster IMPORT cost, which stays serial regardless of batching) would
  add real complexity (partial-success handling, byte budget dimension,
  planner interaction) for an unproven gain.
- **C. Both** — moot; A has nothing left to optimize, so C reduces to B,
  already rejected.
- **D. Keep singleton/no change** — **SELECTED**.

## Rationale

The pre-existing `ZCL_ABAPGIT_ORTEC_WAPA` implementation already achieves
the safe, correctness-preserving optimization this mission's WAPA
safeguards describe (bulk directory reads instead of `cl_o2_api_pages`
API round-trips per page). The one remaining cost family (`O2PAGCON`
cluster imports) is bounded by the ABAP cluster-table model itself, and
bulk-reconstructing it directly is explicitly the kind of "direct
cluster-table interpretation without proven parity" this mission
prohibits. No source or trace evidence gathered this pass shows repeated/
duplicate work within one WAPA object, so Candidate A has nothing to
build. Candidate B requires evidence this pass does not have.

## Mandatory safeguards — status against current (unchanged) source

| Safeguard | Status |
|---|---|
| No direct cluster-table interpretation without proven parity | Held — `ZCL_ABAPGIT_ORTEC_WAPA` uses `IMPORT ... FROM DATABASE o2pagcon(tr)`, the same cluster-access primitive the standard API uses internally, never raw storage tables |
| Active/inactive/version semantics preserved | Held — `c_active = 'A'` used consistently; `exists()` explicitly checks both `A` and `I` versions, matching `cl_o2_api_application=>load`'s documented behavior |
| Complete page key + deterministic ordering | Held — all bulk SELECTs use `ORDER BY PRIMARY KEY`; page iteration order is driven by the same `it_pages` list the standard path uses |
| Bounded bytes/page count/WAPA count | N/A this round — no batching proposed |
| Oversized WAPA remains singleton | Unchanged — no batching proposed |
| No cross-WAPA/session state | Held — class is stateless (`CLASS-METHODS` only, no `CLASS-DATA`) |
| Cleanup on success and exception | Unchanged — no new resources allocated |
| No unbounded preloading | Held — `build_context` bulk-loads exactly the rows implied by `it_pages`, nothing more |
| Feature OFF remains pure standard | Held — the only entry point is the existing `is_wapa_active()` guard in `ZCL_ABAPGIT_OBJECT_WAPA`, unchanged this pass |
| Output byte parity | Not re-verified this pass — no code changed |

## Decision (SER-FINAL, first pass)

`WAPA_SELECTED_DESIGN=D_KEEP_SINGLETON_NO_CHANGE`. No implementation.
Multi-WAPA batching (Candidate B) remains a named, resumable backlog item:
**entry condition** = a dedicated "WAPA Set - Batch" true-worker SAT trace
(or equivalent) proving `ZCL_ABAPGIT_ORTEC_WAPA` is actually exercised in
a batch dispatch and showing a concrete, repeated, batching-addressable
cost (e.g. RFC/dispatch overhead dominating over per-page cluster-import
cost for small WAPAs) - not evidenced this pass.

## SER-FINAL-CONTINUOUS re-evaluation (2026-08-10, second pass)

The owner explicitly directed not to reject W-A solely because `O2PAGCON`
is a cluster/import mechanism, and to check for: supported SAP APIs that
avoid it, raw-payload preload with identical decode semantics, request-
scoped result reuse, and elimination of duplicate page reads. This pass
re-read `ZCL_ABAPGIT_ORTEC_WAPA=>serialize` in full (not just the helper
methods read previously):

- `serialize()` calls `build_context()` exactly once (bulk directory
  reads), then `LOOP AT lt_pages ... read_page(...)` exactly once per
  page - confirmed no page is visited twice.
- `read_page()` calls `add_page_content_file` (1 cluster IMPORT,
  `objtype = PAGE`) and, for full-type pages only, `add_full_page_details`
  (up to 2 more cluster IMPORTs, `objtype = EVHNDL`/`TYPES`) - these are
  three **different keys** in the same cluster table (objtype differs),
  not repeated reads of the same key. No duplicate-read pattern exists to
  eliminate via request-scoped reuse (W-A) or worker-local dedup (W-B).
- Checked for a "raw payload preload, identical decode" alternative: the
  only way to read multiple `O2PAGCON` cluster records in fewer round
  trips than one `IMPORT` per key is to read the cluster's own physical
  storage table directly (bypassing `IMPORT ... FROM DATABASE`) and
  decompress/decode it in ABAP - this is precisely "direct cluster-table
  interpretation" and the mission's own instruction is explicit: "Reject
  direct cluster reconstruction when parity cannot be proven." No parity
  proof is possible in this pass (would require reverse-engineering the
  kernel's cluster compression/paging format), so this remains rejected,
  not merely assumed-risky.
- Conclusion unchanged: **W-D remains correct** on a strictly more
  thorough re-read. This is not a conservative refusal to look - the
  source genuinely contains no duplicate/redundant WAPA read to remove,
  and the one theoretically "bulkable" avenue (cluster reconstruction) is
  explicitly out of bounds without parity proof this pass cannot produce.

## Decision (SER-FINAL-CONTINUOUS, final)

`WAPA_INTRA_OBJECT_DESIGN=W-D_NO_CHANGE` (re-confirmed).
`WAPA_MULTI_OBJECT_DESIGN=W-D_NO_CHANGE` (re-confirmed - still no true-
worker batch evidence). No implementation for either. Per the mission's
own operating rule, a no-change conclusion does not end the mission -
continuing directly to FUGR (see `ser_final_fugr_design.md` and
`fugr_changed_by_design.md`).
