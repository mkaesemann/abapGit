# SER-FINAL-CONTINUOUS — FUGR CHANGED_BY design + gate (2026-08-10)

## Automatic implementation gate evaluation

```text
THEORETICAL_GAIN=YES
```
Proof: `functions()` (⇒ `RS_FUNCTION_POOL_CONTENTS` FM call + a provider-or-direct `ENLFDIR`
resolution) was called unconditionally inside `CHANGED_BY`, but its sole consumer — the
`LOOP AT lt_functions ... WHERE funcname = to_upper( iv_extra )` — is a **provable no-op**
whenever `iv_extra` is initial, because ABAP function-module names returned by
`RS_FUNCTION_POOL_CONTENTS`/`ENLFDIR` are never empty strings, so `funcname = ''` can never be
TRUE. The dominant real caller (`zcl_abapgit_repo_content_list`) always invokes `CHANGED_BY`
with an empty `iv_extra`. This is exactly the mission's named pattern: "repeated STANDARD-table
lookup ... eliminated" / "data already computed ... reused safely" (here: *not needed at all*,
which is the strongest form of the same class of win).

```text
BOUNDED_COMPLEXITY_IMPROVEMENT=YES
```
Per call, this removes exactly one `RS_FUNCTION_POOL_CONTENTS` FM invocation and, in the (evidenced) common case where the ENLFDIR batch-prefetch provider has not covered this call's area, one additional direct `SELECT * FROM enlfdir` — both O(1) removals per `CHANGED_BY` invocation, applied at up to O(F) call sites (F = number of changed FUGR objects in a given content-list render). No new loop, no new complexity class introduced.

```text
CORRECTNESS_MODEL=COMPLETE
```
The removed call path can never influence `lv_program`/`lv_found`/`lt_stamps` when `iv_extra`
is initial (proof above). For the complementary case (`iv_extra` non-initial), the guard leaves
the original code path byte-for-byte unchanged, so per-file/per-include/per-function-module
`CHANGED_BY` lookups (used by the diff view) are completely unaffected. This is a full, not
approximate, correctness model — output is provably identical in every case, not merely "close
enough" (unlike, e.g., the existing `ZCL_ABAPGIT_CTS_INTEGRATION=>CHANGED_BY_BULK` mechanism,
which already accepts a courser per-object approximation for PROG/CLAS/INTF/TABL/VIEW/DTEL/DOMA/
MSAG — that precedent was considered and explicitly NOT followed here, because this fix achieves
full parity instead of a new approximation).

```text
NO_CROSS_REQUEST_STATE=YES
```
No new member data, no new cache, no new class. Purely a narrower branch condition around
already-existing local variables.

```text
EXPECTED_SAVING_GT_IMPLEMENTATION_OVERHEAD=YES
```
Per the prior pass's trace evidence, `RS_FUNCTION_POOL_CONTENTS` (698ms gross) + the direct
`ENLFDIR` fallback SELECT (245ms gross) together account for roughly 70% of the ~1.3s aggregate
`CHANGED_BY`-attributed cost for 147 FUGR objects in that trace — recalculated here as an
**order-of-magnitude estimate, not an exact number** (SAT "gross" time overlaps/nests and is not
strictly additive; the true wall-clock reduction depends on DB/FM internals not fully visible in
the hit list). Implementation cost is a single `IF` guard.

## Rejected alternative: bulk cross-object CHANGED_BY provider (Candidate B from the prior pass)

Still **not implemented**. Re-confirmed this pass: the only bulk cross-object mechanism that
exists for `CHANGED_BY` today is `ZCL_ABAPGIT_CTS_INTEGRATION=>CHANGED_BY_BULK` (`src/ortec/`,
ORTEC-owned), which already has per-object-type `CASE` branches for PROG/CLAS/INTF/TABL/VIEW/
DTEL/DOMA/MSAG (each a single bulk `FOR ALL ENTRIES` keyed on the object's own name only), but
**no FUGR branch** — anything not covered falls through to the exact per-object
`zcl_abapgit_objects=>changed_by(...)` path unaffected by this pass's fix's *absence* (the fix
lives in `ZCL_ABAPGIT_OBJECT_FUGR` itself, so it benefits this fallback path too). Adding a FUGR
branch to `CHANGED_BY_BULK` was evaluated and **rejected this pass**: every existing branch there
returns ONE approximate value per `obj_type`+`obj_name` (ignoring per-file granularity) using the
object's own single primary table (e.g. `reposrc` by `progname = obj_name` for PROG) — this
approximation is acceptable for those types but is a **materially different, weaker correctness
model** for FUGR, whose real algorithm rolls up `REPOSRC` across the main program **and every
include**, plus `REPOTEXT`/`EUDB` — a bulk FUGR branch limited to the main program's own
`REPOSRC` row could silently under-report a more-recently-changed include, function module GUI
edit, or text-pool change. Implementing this would violate `CORRECTNESS_MODEL=COMPLETE`. Not
authorized without a dedicated pass proving a bulk formulation that preserves the full rollup
(would need bulk `REPOSRC`/`REPOTEXT`/`EUDB` `FOR ALL ENTRIES` across every FUGR's main program +
every include across the whole `CHANGED_BY_BULK` call, still requiring one `RS_GET_ALL_INCLUDES`
per FUGR up front — a real, larger, differently-shaped design not resolvable in this pass without
guessing at correctness-sensitive tie-break semantics). Recorded as the same named backlog item
as before (`FUGR-CHANGED-BY-STATUS-SWEEP` in `.memory/state.md`), **not gated automatically** by
this mission's CHANGED_BY gate because it fails `CORRECTNESS_MODEL=COMPLETE` as scoped, and a
complete version is a genuinely new, larger design effort, not a bounded fix.

## Gate result

`FUGR_CHANGED_BY_THEORETICAL_GAIN=YES` → automatic implementation authorized and completed.
