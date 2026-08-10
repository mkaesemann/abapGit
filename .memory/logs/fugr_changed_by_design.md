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

## SER-FINAL-CORRECTION additions (2026-08-10)

### Implemented: test seam extraction (no behavior change)

`needs_function_lookup( iv_extra )` and `most_recent_user( it_stamps )`
were extracted from `CHANGED_BY`'s body into small, pure, private
`CLASS-METHODS` (the exact `ty_stamps` local type was promoted to a
class-level `ty_changed_by_stamp`/`ty_changed_by_stamp_tt` type pair to
support this). This is a pure refactor - the method bodies are
byte-for-byte the same logic, just parameterized - done specifically to
enable local ABAP Unit coverage (see
`src/objects/zcl_abapgit_object_fugr.clas.testclasses.abap`,
`LOCAL FRIENDS ltcl_changed_by`, mirroring the exact precedent already
used in this codebase for `zcl_abapgit_object_ecatt_super`'s own
`ltcl_changed_by` test class).

### Implemented: ENLFDIR lookup complexity fix (BOUNDED_COMPLEXITY_IMPROVEMENT gate)

`functions()`'s `LOOP AT rt_functab ... READ TABLE lt_enlfdir WITH KEY
funcname = ... TRANSPORTING NO FIELDS` was a linear scan of a table that
is unconditionally `SORT`ed by the exact same key on the immediately
preceding line - added the `BINARY SEARCH` addition. This is a provably
behavior-preserving, O(F·E)→O(F·log E) fix (ABAP guarantees identical
found/not-found results for `BINARY SEARCH` against a table sorted by the
specified key) - matches this mission's own named pattern "repeated
STANDARD-table lookup changes from O(N·K) to O(K log N)" exactly. Not
unit-tested in isolation (would require refactoring `functions()`'s
`RS_FUNCTION_POOL_CONTENTS` dependency out, judged not worth the
additional surface area for a change whose correctness is provable by
the ABAP language's own `BINARY SEARCH` contract); covered by static
proof in the correctness review instead.

### Evaluated and REJECTED (source-backed): `mt_includes_all` LOOP...WHERE conversion to sorted/binary-search read

`CHANGED_BY`'s `LOOP AT mt_includes_all ASSIGNING <lv_include> WHERE
table_line = to_upper( iv_extra ). ... EXIT.` is a single find-first scan,
executed exactly once per `CHANGED_BY` call (not repeated), against a
table whose size is bounded by one function group's own include count
(typically tens, not thousands). Converting this to a sort + binary-search
read would add a `SORT` statement whose cost is not clearly recouped for
a single non-repeated lookup against a small table - this is exactly the
mission's own named non-authorized case ("micro-optimizing small tables
without repeated lookup"). `NO_CHANGE_JUSTIFIED`, source-backed: no
repeated lookup exists at this call site to amortize a sort against.

### Evaluated and REJECTED (source-backed): cross-object `CHANGED_BY_BULK` FUGR branch

Re-considered under this pass's more permissive framing (the mission
explicitly asks about "existing data already available in CTS
integration"). A complete (non-approximate) bulk FUGR branch in
`ZCL_ABAPGIT_CTS_INTEGRATION=>CHANGED_BY_BULK` would require: (1)
exposing `main_name()`'s private namespace-computation as a shared
primitive (to avoid a second, independently-maintained copy of
`FUNCTION_INCLUDE_SPLIT`-based namespace logic silently drifting from the
original over time); (2) one `RS_GET_ALL_INCLUDES` call per FUGR up front
(same F calls as today, not a regression, but still needed before any
bulk SELECT can be built); (3) a **new** per-FUGR stamp-partitioning
algorithm proven correct for every `iv_extra` shape (whole-object,
per-include, per-function) - none of which exists anywhere in this
codebase today (the *other* 8 object types already covered by
`CHANGED_BY_BULK` all use an explicitly weaker, `iv_extra`-blind, single-
table approximation, previously and again this pass judged unacceptable
for FUGR's fundamentally multi-source rollup). This is not a bounded
complexity fix to *existing* code - it is new architecture requiring its
own design + adversarial review pass, matching how this mission itself
treats FUGR serializer Candidate F-F ("authorized only if...can all be
proven") and the DDLS gate (reject net-new semantic-reconstruction risk).
`REJECT_WITH_SOURCE_PROOF` - not implemented; remains the same named
`FUGR-CHANGED-BY-STATUS-SWEEP` backlog item in `.memory/state.md`, now
with a precise statement of exactly what new primitive/algorithm would
need to be designed and reviewed before it could be attempted.
