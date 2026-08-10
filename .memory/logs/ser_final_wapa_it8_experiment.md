# SER-FINAL-CORRECTION — WAPA implementation-grade re-evaluation (2026-08-10)

Owner correction: the prior pass's "architectural floor" conclusion was
not accepted as final. This pass re-derives each of the 5 named
candidates independently, with exact source line citations, and assigns
exactly one of `IMPLEMENT` / `REJECT_WITH_SOURCE_PROOF` /
`BLOCKED_BY_REQUIRED_IT8_EXPERIMENT` to each - no bare "cluster mechanism,
therefore reject" reasoning is used anywhere below.

## Source anchors read in full this pass

`src/ortec/serial/zcl_abapgit_ortec_wapa.clas.abap`:
`build_context` (L176), `add_page_content_file` (L279, `IMPORT content` at
L297), `add_full_page_details` (L351, `IMPORT evhandler` at L374,
`IMPORT typesource` at L399), `read_page` (L472), `serialize` (L518).

## Candidate 1 — request/worker-local page-content cache keyed by complete WAPA/page/version identity

**Exact call graph (proven by direct read, not inference):**

```
serialize()  [L518]
  build_context(...)                         -- called exactly ONCE per object
  LOOP AT lt_pages ASSIGNING <ls_page>.       -- one iteration per page, no re-entry
    read_page( is_context, is_page, io_files )   -- called exactly ONCE per page
  ENDLOOP.

read_page()  [L472]
  IF pagetype <> CONTROLLER.
    add_page_content_file(...)    -- 1 cluster IMPORT, key = (applname,pagekey,PAGE,A)
    add_full_page_details(...)    -- up to 2 MORE cluster IMPORTs, keys =
                                      (applname,pagekey,EVHNDL,A) and
                                      (applname,pagekey,TYPES,A)
    add_parameters(...)           -- 0 DB access, reads BUILD_CONTEXT's bulk tables
  ENDIF.
```

**Proof that Candidate 1 has nothing to cache:** every `(applname, pagekey,
objtype, version)` key that is ever passed to `IMPORT ... FROM DATABASE
o2pagcon(tr) ID ...` is constructed from the CURRENT loop iteration's
`<ls_page>` and a FIXED `objtype` literal (`so2_objtype_page` /
`so2_objtype_evhndl` / `so2_objtype_types`), and each of these 1-3 keys is
built and consumed exactly once, inside a single pass over `lt_pages`
that itself has no re-entry (no recursive call to `read_page`, no second
`LOOP AT lt_pages` anywhere in the class, confirmed by `grep` — `serialize`
is the only caller of `read_page`). A cache keyed by
`(applname,pagekey,objtype,version)` would therefore see a 100% miss
rate on every lookup — it cannot reduce the number of `IMPORT` statements
executed, only add bookkeeping overhead.

**Disposition: `REJECT_WITH_SOURCE_PROOF`.** The missing proof this
mission requires me to state explicitly: *proof that no key is ever
requested twice* — provided above by exhaustive enumeration of every
`IMPORT ... FROM DATABASE o2pagcon` call site (3 total in the whole class)
and their single call site each. No IT8 experiment can change this — it
is a static property of the current source, not a runtime behavior that
could differ across environments/releases.

## Candidate 4 — deduplication of repeated `READ_PAGE` / `GET_PAGE_CONTENT` calls

**Source proof:** `ZCL_ABAPGIT_ORTEC_WAPA` (the class actually used when
`is_wapa_active() = TRUE`) does not call `ZCL_ABAPGIT_OBJECT_WAPA`'s
`READ_PAGE`/`GET_PAGE_CONTENT` methods at all — it has its own,
independent `read_page`/`get_page_content` implementations (confirmed:
`grep` for `READ_PAGE\(` and `GET_PAGE_CONTENT\(` inside
`zcl_abapgit_ortec_wapa.clas.abap` finds only the class's own method
definitions/self-calls, never a call into `ZCL_ABAPGIT_OBJECT_WAPA`).
Within `ZCL_ABAPGIT_ORTEC_WAPA` itself, `read_page` is called exactly
once per page (see Candidate 1's call-graph proof) and
`get_page_content` is called exactly once per page that needs content
(from inside `add_page_content_file`, itself called exactly once per
page). There is no repeated call of either method to deduplicate.

**Disposition: `REJECT_WITH_SOURCE_PROOF`.** Same exhaustive-enumeration
proof as Candidate 1 — this candidate is the same underlying fact viewed
from the "which method is called repeatedly" angle instead of the "which
DB key is read repeatedly" angle; both come back empty for the same
reason.

## Candidate 2 — one-time preload of raw page-content rows followed by exact existing decode/import semantics

This is **not** rejected on the "cluster mechanism" alone, per the owner's
explicit instruction. Re-derivation:

- `O2PAGCON` is accessed via `IMPORT ... FROM DATABASE o2pagcon(tr) ID
  <key>` — the `(tr)` area-id syntax is specific to ABAP **database
  cluster tables**. A cluster table's logical records are physically
  stored as one-or-more rows in a real, `SELECT`-able database table
  (commonly with columns for the key, a page/sequence number, and a raw
  data column) — this physical table **can** be bulk-read with a normal
  `SELECT ... FOR ALL ENTRIES` like any other table, because it is a real
  transparent table underneath, not a black box.
- ABAP's `IMPORT ... FROM DATA BUFFER <xstring>` statement is a
  documented, standard language feature that runs the **exact same**
  decompression/type-checking logic as `IMPORT ... FROM DATABASE`, given
  a buffer that was correctly assembled from a cluster's raw physical
  rows (concatenated in the cluster's own page-sequence order). This is
  reuse of the kernel's own decode logic, not a reimplementation of it —
  a materially different, much lower-risk proposition than manually
  parsing the compressed payload itself.
- **The exact missing proof, stated precisely:** (a) `O2PAGCON`'s real
  physical column layout (name of the raw-data column(s), name/type of
  the page-sequence column, whether multi-row spanning is used at all
  for typical WAPA page sizes) is DDIC/kernel information not visible
  from this repository's source (it is a standard SAP cluster, not an
  abapGit-defined table) and cannot be enumerated without ADT/SE11
  access to a live system; (b) even with the layout known, byte-for-byte
  decode parity against the existing `IMPORT ... FROM DATABASE` result
  must be empirically confirmed for at least one multi-row-spanning
  record (small pages likely fit in one physical row, which would not
  exercise the part of this approach most likely to be wrong).
- **No safe local experiment can supply this proof** — it requires SE11/
  ADT metadata of a real SAP-delivered cluster table plus a live
  comparison run, neither of which exists in this workspace.

**Disposition: `BLOCKED_BY_REQUIRED_IT8_EXPERIMENT`** (Experiment steps
1-3 below).

## Candidate 3 — supported SAP mass-read API, if one exists

`CL_O2_API_PAGES`/`CL_O2_API_APPLICATION`/`CL_O2_PAGE` are standard SAP
classes; their source is not part of this repository, so their full
public interface cannot be exhaustively enumerated from source alone —
only the methods abapGit itself already calls are visible here (`load`,
`get_all_pages`, `create_new_page`, `get_attrs`, `get_event_handlers`,
`get_parameters`, `get_type_source`, `get_page`, `delete_page_for_
application`). None of these is a multi-page mass-content-read method,
but I cannot assert **completeness** of that list without inspecting the
class definitions directly (ADT class browser / SE24), which requires
live system access.

**Disposition: `BLOCKED_BY_REQUIRED_IT8_EXPERIMENT`** (Experiment step 1
below — a cheap, first, ADT-only check that should run *before* attempting
Candidate 2's much larger raw-cluster-read investigation, since a real
mass-read API, if found, would make Candidate 2 unnecessary).

## Candidate 5 — bounded multi-WAPA batching (after intra-object behavior is understood)

Correctly sequenced *after* Candidates 2/3, per the mission's own
"preferred sequence". Independent of that ordering, `EXPECTED_GAIN=YES`
(a mandatory implementation gate) cannot be established from source alone
here: multi-WAPA batching's value proposition is amortizing **RFC/
dispatch overhead** across several WAPA objects in one batch, not
reducing per-page cluster-import cost (which stays fully serial
regardless of batching, per Candidates 1/2's own analysis). Whether that
RFC/dispatch overhead is a material fraction of total WAPA processing
time for realistic WAPA sizes is an empirical, runtime question — no
true-worker WAPA-batch SAT trace exists in any pass's evidence set to
measure it (both this and the prior pass's supplied traces are either
pure-standard `WAPA Set - Normal` runs or mixed traces that do not
isolate WAPA hit-list detail).

**Disposition: `BLOCKED_BY_REQUIRED_IT8_EXPERIMENT`** (Experiment step 4
below). Implementing the bounded-admission policy (hard count limit, byte
budget, oversized-singleton fallback, split-before-dispatch, no state
reuse, no partial success) without first confirming `EXPECTED_GAIN=YES`
would violate the mandatory implementation gate and would add real,
non-trivial planner complexity for an unproven benefit — explicitly not
authorized to implement speculatively.

## Summary table

| Candidate | Disposition |
|---|---|
| 1. request/worker-local page-content cache | `REJECT_WITH_SOURCE_PROOF` |
| 2. one-time raw preload + existing decode semantics | `BLOCKED_BY_REQUIRED_IT8_EXPERIMENT` |
| 3. supported SAP mass-read API | `BLOCKED_BY_REQUIRED_IT8_EXPERIMENT` |
| 4. dedup of repeated READ_PAGE/GET_PAGE_CONTENT | `REJECT_WITH_SOURCE_PROOF` |
| 5. bounded multi-WAPA batching | `BLOCKED_BY_REQUIRED_IT8_EXPERIMENT` |

`WAPA_INTRA_OBJECT` (candidates 1-4) = mixed: 1 and 4 are fully closed
(`REJECT_WITH_SOURCE_PROOF`, no IT8 needed, will not change with new
evidence since they are proofs about the CURRENT SOURCE's static
structure); 2 and 3 are `BLOCKED_BY_REQUIRED_IT8_EXPERIMENT`.
`WAPA_MULTI_OBJECT` (candidate 5) = `BLOCKED_BY_REQUIRED_IT8_EXPERIMENT`.

No WAPA productive code was implemented this pass, because every
candidate that is NOT source-provably-empty (1, 4) requires evidence this
workspace cannot produce (2, 3, 5) — implementing any of 2/3/5 without
that evidence would risk exactly the "silent wrong value"/"unproven
speculative complexity" failure modes this mission's own gates
(`CORRECTNESS_MODEL=COMPLETE`, `EXPECTED_GAIN=YES`) exist to prevent.

## Required IT8 experiment (single coordinated session, ordered steps)

**Objects:** any WAPA (`WAPD`/BSP application) with at least 3 pages,
where at least one page is a "full" page type with event handlers +
type source (to exercise all 3 `O2PAGCON` objtypes), plus one WAPA large
enough that at least one page's content is likely to span multiple
physical cluster rows (a page with several KB of layout source — check
via `SE16`/ADT on `O2PAGCON` row sizes for a candidate application first).

**Step 1 (Candidate 3 — mass-read API check, ~10 min, no debugger needed):**
Open `CL_O2_API_PAGES`, `CL_O2_API_APPLICATION`, `CL_O2_PAGE` in ADT/SE24
and list every public method. **Pass/fail:** if any method accepts a
TABLE of page keys and returns content for all of them in one call,
Candidate 3 becomes `IMPLEMENT` (route through it exactly like existing
`get_all_pages`); otherwise Candidate 3 is `REJECT_WITH_SOURCE_PROOF`
(now provable) and proceed to Step 2.

**Step 2 (Candidate 2 — physical layout, ~15 min):** Open the DDIC
definition of `O2PAGCON` (SE11/ADT "Database Table" view — cluster tables
show their own physical column list). Record: raw-data column name(s),
page/sequence column name, key column order. **Pass/fail:** if the table
does not expose a `SELECT`-able raw-data column at all (e.g. it is a pool
table with no visible structure, or access is blocked by an authorization/
buffering layer that prevents a plain `SELECT`), Candidate 2 is
`REJECT_WITH_SOURCE_PROOF` (now provable) — stop here. Otherwise proceed
to Step 3.

**Step 3 (Candidate 2 — decode parity, ~30 min, needs a debugger):** For
the chosen full-page, multi-row-spanning WAPA page:
1. Set a breakpoint at `ZCL_ABAPGIT_ORTEC_WAPA=>ADD_PAGE_CONTENT_FILE`,
   line with `IMPORT content ... FROM DATABASE o2pagcon(tr) ID
   ls_pagecon_key`. Capture `lt_content` (decoded result) into a
   debugger variable/export for comparison.
2. In a separate test report (or the same debugger session), `SELECT *
   FROM o2pagcon` (the real physical table from Step 2) for the exact
   same key, ordered by the page/sequence column, concatenate the raw
   data column(s) in order into one `xstring`, then `IMPORT content FROM
   DATA BUFFER <that xstring>` into a second variable.
3. **Pass/fail:** compare the two `lt_content` results field-by-field
   (`cl_abap_unit_assert=>assert_equals` style, or manual inspection for
   a one-off proof run). Byte-identical ⇒ Candidate 2 is `IMPLEMENT`
   (build the bulk `SELECT ... FOR ALL ENTRIES` + per-key buffer-assembly
   + `IMPORT ... FROM DATA BUFFER` path, gated behind `is_wapa_active()`,
   with a mandatory fallback to today's per-key `IMPORT ... FROM
   DATABASE` on any decode mismatch/exception). Any difference, for any
   tested page, ⇒ Candidate 2 is `REJECT_WITH_SOURCE_PROOF` (parity
   disproven, not just "not provable") — do not implement.

**Step 4 (Candidate 5 — RFC/dispatch overhead, ~20 min, needs a
debugger/SAT):** Force at least 2 WAPA objects into the SAME ORTEC batch
dispatch (temporarily relax the singleton-only admission check under
debugger control, or use a repo with `is_serial_batch_active` on and a
breakpoint in `ZCL_ABAPGIT_ORTEC_SER_ORCH=>DISPATCH_BATCH`/`BEFORE_
DISPATCH` to confirm admission) and capture a SAT/ST12 trace scoped to
the RFC worker for that specific dispatch. **Pass/fail:** if `Rfc
Z_ABAPGIT_ORTEC_SER_BATCH` + `Wait Async` + `Dynpro Entry CPIC ASYNC`
overhead for that dispatch is ≥20% of the dispatch's total gross time
(a threshold chosen to represent "clearly worth amortizing"), Candidate 5
is `IMPLEMENT` (design the bounded admission policy per the mission's
explicit safeguards); below that threshold, Candidate 5 is `REJECT_WITH_
SOURCE_PROOF` (now provable: dispatch overhead is negligible relative to
per-page content cost, so batching would not meaningfully help even
though it is technically possible).
