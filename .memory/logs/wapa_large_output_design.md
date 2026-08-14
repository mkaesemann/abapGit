# WAPA large-output page-content performance - design revision 3

```text
TASK_ID=WAPA_LARGE_OUTPUT_OPTION1_DESIGN_20260813
STATUS=DESIGN_COMPLETE_AWAITING_CORRECTNESS_AND_PERFORMANCE_RE_REVIEW
SCOPE=ZCL_ABAPGIT_ORTEC_WAPA and direct raw-prefetch helpers only
PRODUCTIVE_EDITS=NONE
OWNER_DECISION=OPTION_1_TERMINAL_REFERENCE_RANGE_FALLBACK
OPEN_BLOCKERS=0
OPEN_MAJORS=0
SUPERSEDES=WAPA_LARGE_OUTPUT_DESIGN_REVISION2_20260813 in full
```

This is the complete normative design. The owner approved a bounded adaptive split followed by
terminal reference serialization of the remaining contiguous page range. No path may bisect to
singletons indefinitely or re-enter raw admission after range fallback begins.

## 1. Review convergence

| Finding | Disposition | Changed sections |
|---|---|---|
| DR-001 | ACCEPTED_AND_FIXED | 5, 6, 9: all three maps and ACTIVE are cleared at every attempt, sibling, page, reference, exception, and exit boundary. |
| DR-002 | ACCEPTED_AND_FIXED | 4.4, 7, 9: depth 5 is terminal; a rejected range uses current reference READ_PAGE once per page with zero further raw-helper SELECTs. |
| WAPA-PD-01 | ACCEPTED_AND_FIXED | 4.2-4.3: metadata-first admission and conservative CLUSTD-capacity charging prevent rejected-parent payload reads. |
| WAPA-PD-02 | ACCEPTED_AND_FIXED_WITH_OWNER_BOUND | 5 and 7: no false decoded preflight claim; raw capacity is physical, decode is one page, and the owner binds productive decoded page content to 15 MiB. |
| WAPA-PD-03 | ACCEPTED_AND_FIXED | 8: executable 40,000-page acceptance and analytical 1,000,000-page proof are mandatory. |
| WAPA-PD-04 | ACCEPTED_AND_FIXED | 4.4 and 7: `C_MAX_RAW_SPLIT_DEPTH = 5` gives at most 127 raw-helper SELECTs per initial group, 5,080 at 40k, and 127,000 at 1m. |
| WAPA-PD-05 | ACCEPTED_AND_FIXED_AT_DESIGN_GATE | 5 and 8: exact application-owner lifetime is bounded; paired SAT must confirm <=15 MiB decoded page, one kernel IMPORT transient, and zero range fallbacks on `/O4H/COMPANION`. |

WAPA-PD-05 execution evidence remains a production-acceptance gate, not an architecture choice.
Failure of the specified SAT thresholds stops publication; it does not reopen an implementation
alternative inside this design.

## 2. Facts and decisions

### CONFIRMED

- Active anchors are `SERIALIZE`, `TRY_RAW_PREFETCH`, `READ_RAW_ROWS`,
  `ASSEMBLE_AND_DECODE`, `READ_PAGE`, `ADD_PAGE_CONTENT_FILE`, and
  `ADD_FULL_PAGE_DETAILS` in `ZCL_ABAPGIT_ORTEC_WAPA`.
- Active `READ_RAW_ROWS` selects PAGEKEY/OBJTYPE/SRTF2/CLUSTR/CLUSTD into one table.
- Active reference `READ_PAGE` performs one mandatory PAGE cluster import and, for a full page,
  up to two optional EVHNDL/TYPES cluster imports.
- Active public counters are `GET_RAW_PREFETCH_COUNTERS(EV_HITS,EV_FALLBACKS)`.
- Active ORCH separates WAPA in `PARTITION_OBJECTS` and
  `BUILD_WAPA_SINGLETON_BATCHES`; each WAPA remains a one-object batch.
- `O2PAGCON-CLUSTD` is `ABAP.LRAW(2886)` and `CLUSTR` is `ABAP.INT2`.
- `/O4H/COMPANION` has 11,307 pages, 73,861 physical rows, and 791,297,854 final bytes.

### MEASURED

- `/O4H/COMPANION`: PAGE 51,247 rows; EV 11,307; TYPES 11,307.
- Warm worker baseline is about 42.75 seconds; O2PAGCON import net is about 10.54 seconds.
- Declared CLUSTD payload capacity is exactly 2,886 bytes per selected row.

### OWNER_DECISION

- Initial range: 1,000 pages. Manifest ceiling: 40,000 rows. Raw charged capacity: 100 MiB.
- Productive logical decoded content for each page is assumed to be <=15 MiB (15,728,640 bytes).
- Maximum adaptive split depth is 5, with the root at depth 0.
- A failed attempt at depth 5 serializes that current contiguous range through reference
  `READ_PAGE`; it does not split and does not retry raw admission.
- Range fallback is defensive only and must occur zero times on `/O4H/COMPANION` acceptance.
- WAPA stays singleton-batched. ORCH, BATCH, RFC, standard classes, DDIC, and persistence stay
  unchanged. No WAPA cache is introduced.

### HYPOTHESIS

- A normal 1,000-page `/O4H/COMPANION` range admits without splitting.
- Metadata + payload + verification is faster than reference imports on healthy ranges.
- ABAP copy-on-write reduces physical copies below the logical-owner bound; acceptance does not
  rely on that hypothesis.

### UNKNOWN

- Kernel-internal bytes used by one `IMPORT FROM DATA BUFFER` transient are not exposed in ABAP.
  They are resolved only by the mandatory paired SAT measurement in section 8.

### SUPERSEDED

- Unbounded bisection, singleton raw re-admission, the `4n-1` query shape, chunk-wide decoded
  maps, a decoded preallocation claim based on CLUSTR, and continuation with retained batch rows
  after decode failure are forbidden.

## 3. Binding invariants

| ID | Requirement | Mechanism |
|---|---|---|
| WAPA-INV-01 | Exact path/mode/content/order parity | Initial groups and splits are contiguous; first half completes before second; reference uses unchanged `READ_PAGE`. |
| WAPA-INV-02 | Singleton WAPA policy unchanged | ORCH/BATCH/RFC are read-only and byte-identical. |
| WAPA-INV-03 | No second full-WAPA payload copy | One admitted raw range plus one decoded page only. |
| WAPA-INV-04 | No partial map publication | Publish all current-page maps after complete decode only; ACTIVE is set last. |
| WAPA-INV-05 | Healthy path set-based | Exactly three raw-helper SELECTs per admitted range. |
| WAPA-INV-06 | Bounded adaptive retry | Root depth 0; depth 5 is terminal; at most 127 raw-helper SELECTs per initial group. |
| WAPA-INV-07 | Rejected parent has no payload fetch | Manifest admission precedes CLUSTD SELECT. |
| WAPA-INV-08 | Terminal fallback cannot re-enter raw | `SERIALIZE_REFERENCE_RANGE` contains no call to raw admission, splitting, or itself. |
| WAPA-INV-09 | No stale raw state | `CLEAR_RAW_CONTEXT` plus local FREE lifetime in sections 5-6. |
| WAPA-INV-10 | Raw row and byte bounds are physical | 40,001-row probe and 2,886-byte charge; payload only for admitted manifest. |
| WAPA-INV-11 | Page-copy lifetime is explicit | At most four application-level page payload owners and one current kernel IMPORT transient. |
| WAPA-INV-12 | Diagnostics cannot change behavior | Saturating counters only; no payload, cache, SQL, or exception side effect. |

## 4. Exact target algorithm

### 4.1 Constants, types, and signatures

```abap
CONSTANTS c_raw_prefetch_initial_pages TYPE i VALUE 1000.
CONSTANTS c_max_raw_manifest_rows       TYPE i VALUE 40000.
CONSTANTS c_max_raw_payload_bytes       TYPE int8 VALUE 104857600.
CONSTANTS c_raw_payload_row_bytes       TYPE i VALUE 2886.
CONSTANTS c_max_raw_split_depth         TYPE i VALUE 5.
CONSTANTS c_max_decoded_page_bytes      TYPE int8 VALUE 15728640.
```

`C_MAX_DECODED_PAGE_BYTES` is an owner-approved productive acceptance bound checked after
decode; it is not called a preallocation guard. A breach frees the admitted remainder and takes
terminal reference fallback for the current unconsumed suffix.

Add `TY_RAW_MANIFEST(PAGEKEY,OBJTYPE,SRTF2,CLUSTR)` with unique sorted key `BY_ROW` on
PAGEKEY/OBJTYPE/SRTF2. CLUSTR is a compared value, not a lookup component. Change
`TY_RAW_ROW_TT` to an empty primary key plus unique sorted key `BY_PAGE` on
PAGEKEY/OBJTYPE/SRTF2. All new global method and constant names in this document are <=30
characters.

`RAW_PREFETCH_AND_READ` imports `IT_PAGES`, `IO_FILES`, and
`IV_SPLIT_DEPTH TYPE I DEFAULT 0`, changes `CS_CONTEXT` and `CT_PAGES_INFO`, and raises the
existing exception. It owns one complete range attempt, including output consumption.

### 4.2 `READ_RAW_MANIFEST`

1. Validate injected limits: `0 < IV_MAX_ROWS <= 40000` and
   `0 < IV_MAX_PAYLOAD_BYTES <= 104857600`; otherwise raise the existing exception before SQL.
2. Build/deduplicate requested `(PAGEKEY,OBJTYPE)` keys in memory.
3. Set `LV_PROBE_ROWS = IV_MAX_ROWS + 1` in type I.
4. Execute one metadata SELECT with unchanged RELID/APPLNAME/VERSION predicates, the requested
   key join, projection PAGEKEY/OBJTYPE/SRTF2/CLUSTR, and `UP TO @LV_PROBE_ROWS ROWS`.
5. Increment `MANIFEST_SELECTS` exactly once after the SELECT returns, including a rejected probe.
6. Reject reason `R` if rows exceed 40,000; `B` if `LINES * 2886` in INT8 exceeds 100 MiB; `A`
   if CLUSTR is outside 0..2886, SRTF2 is negative/duplicate/noncontiguous, or mandatory PAGE is
   absent. A rejected manifest is freed before return.
7. Admit only when every check succeeds. Maximum admitted rows are
   `FLOOR(104857600 / 2886) = 36333`, charging 104,857,038 bytes.

### 4.3 `READ_RAW_ROWS`

1. Accept only an admitted manifest and identical positive production-bounded limits.
2. Execute one payload SELECT joined on PAGEKEY, OBJTYPE, SRTF2, and CLUSTR with unchanged
   RELID/APPLNAME/VERSION predicates and `UP TO 40001 ROWS`. Increment `PAYLOAD_SELECTS` once.
3. Require exact cardinality and PAGEKEY/OBJTYPE/SRTF2/CLUSTR equality; require
   `0 <= CLUSTR <= XSTRLEN(CLUSTD)` and charged capacity <=100 MiB.
4. Call `READ_RAW_MANIFEST` once more with identical keys/limits. This increments
   `MANIFEST_SELECTS` and `VERIFY_SELECTS` once; `VERIFY_SELECTS` is a subset label and is not
   added again when calculating total SQL.
5. Require the verification manifest to equal the admitted manifest exactly under `BY_ROW`.
6. On mismatch, free payload and verification rows, return incomplete, and publish no map.

An admitted healthy range therefore performs exactly three raw-helper SELECTs: admission
manifest, payload, verification manifest.

### 4.4 `RAW_PREFETCH_AND_READ`

The algorithm is decision-free:

```text
CLEAR_RAW_CONTEXT.
assert 0 <= IV_SPLIT_DEPTH <= 5.
record MAX_SPLIT_DEPTH_OBSERVED = max(old, IV_SPLIT_DEPTH).
build requested keys.
if no requested keys: READ_PAGE each page in order under empty active context; return.
READ_RAW_MANIFEST.
if admitted: READ_RAW_ROWS.
if admitted and complete:
  for each page in order:
    DECODE_RAW_PAGE, consuming/deleting that page's raw rows before IMPORT.
    if decode fails or decoded logical bytes exceed 15 MiB:
      free all page locals and all remaining raw/manifest rows.
      CLEAR_RAW_CONTEXT.
      SERIALIZE_REFERENCE_RANGE(current page through final unconsumed page, reason D).
      return.
    CLEAR_RAW_CONTEXT.
    move all three complete page-local maps to context; set ACTIVE last.
    READ_PAGE once; append result.
    CLEAR_RAW_CONTEXT; free page locals.
  free raw/manifest rows; CLEAR_RAW_CONTEXT; return.
free all payload/manifest/page locals; CLEAR_RAW_CONTEXT.
if page count > 1 and IV_SPLIT_DEPTH < 5:
  increment SPLIT_EVENTS.
  split at floor(n/2), preserving contiguous order and nonempty halves.
  recurse first half with depth+1.
  CLEAR_RAW_CONTEXT.
  recurse second half with depth+1.
  CLEAR_RAW_CONTEXT; return.
increment DEPTH_CEILING_HITS only when page count > 1 and depth = 5.
SERIALIZE_REFERENCE_RANGE(the complete current range, original reject reason).
return.
```

A singleton reject at any depth takes terminal reference range immediately. A depth-5 reject
with multiple pages increments both `DEPTH_CEILING_HITS` and the range counters. No child range
is created at depth 5.

### 4.5 `SERIALIZE_REFERENCE_RANGE`

For the supplied contiguous range, in original order:

1. Increment `RANGE_FALLBACKS` once on entry and add range cardinality to
   `RANGE_FALLBACK_PAGES` with saturation.
2. `FREE` all manifest/raw/decode locals before the first reference page.
3. For each page: `CLEAR_RAW_CONTEXT`; call unchanged `READ_PAGE` exactly once; append exactly
   once; increment `REFERENCE_PAGES`; `CLEAR_RAW_CONTEXT` again.
4. On exception: clear context and re-raise unchanged. On normal exit: clear context.
5. Do not call `READ_RAW_MANIFEST`, `READ_RAW_ROWS`, `RAW_PREFETCH_AND_READ`, or itself.

Reference DB work is reported separately: each page has one mandatory PAGE import and up to two
optional FULL_PAGE imports. Range fallback therefore has `N..3N` unchanged reference cluster
imports for N pages and exactly zero further raw-helper SELECTs.

## 5. Decode and memory lifetime

`DECODE_RAW_PAGE` changes `CT_ROWS` and returns at most one PAGE, EVHNDL, and TYPES map row plus
`EV_DECODED_BYTES TYPE INT8`. It processes only `IV_PAGEKEY` and only through `BY_PAGE`.

For each logical key, it validates SRTF2/CLUSTR, appends the current <=2,886-byte fragment to one
key buffer, and deletes that source row immediately after append. After all current-page rows are
assembled, no current-page CLUSTD row remains in `CT_ROWS`. It IMPORTs one key buffer at a time,
frees that buffer immediately, and never copies all `IT_ROWS` for sorting.

`EV_DECODED_BYTES` is the sum of byte lengths of decoded elementary payload fields and the DDIC
byte lengths of the decoded internal-table lines, accumulated in INT8. It counts PAGE content,
XML source, event-handler names/sources, and type-source lines once each. It excludes table
administration bytes and final serializer metadata. The 15 MiB check occurs before map
publication. This is a post-decode owner bound, not a claim that IMPORT allocation was prevented.

Maximum application-owned payload lifetime is:

```text
assembly: admitted raw rows + completed/old buffers <=100 MiB,
          plus one concatenate destination <=100 MiB,
          plus one <=2,886-byte fragment;
import:   remaining raw rows + complete key buffers <=100 MiB,
          plus one decoded page <=15 MiB,
          plus one current kernel IMPORT transient;
consume:  remaining raw rows <=100 MiB minus consumed page charge,
          plus at most four page-scale logical owners:
          context map, READ_PAGE assigned view/final page, conversion string/xstring,
          and IO_FILES accumulator destination.
```

`ADD_PAGE_CONTENT_FILE` and `ADD_FULL_PAGE_DETAILS` must use `ASSIGNING` for raw map reads and
must not create a second whole raw map. The four-owner bound is conservative and does not rely on
copy-on-write. Existing `F_SO_FAR` and final `FILES_XSTRING` remain linear final-output owners and
are identical to reference behavior. No cache exists; scope is one method/request.

If decode fails or exceeds 15 MiB, all remaining admitted raw rows are freed before reference
serialization of the unconsumed suffix. Thus reference IMPORT never overlaps the retained
100 MiB raw batch.

## 6. State, errors, crash, and publication

`CLEAR_RAW_CONTEXT` is exactly:

```abap
CLEAR cs_context-raw_prefetch_active.
FREE: cs_context-raw_content,
      cs_context-raw_evhandler,
      cs_context-raw_typesource.
```

Call it on serialize entry/exit; attempt entry/exit; before/between/after children; before map
assignment; after each page; before/after every reference page; before suffix fallback; and in
every exception cleanup. Page-local maps, manifest, rows, and buffers are freed before fallback.

| Event | Exact result |
|---|---|
| Manifest reject below depth 5 | Clear/free, split contiguous halves, first then second. |
| Manifest reject at depth 5 | Clear/free, serialize current range by reference, no raw retry. |
| Singleton reject | Clear/free, serialize singleton by reference, no raw retry. |
| Payload/verification mismatch | Same split/depth rule as manifest reject; no publication. |
| Decode failure or >15 MiB | Free remaining admitted batch; reference current unconsumed suffix. |
| READ_PAGE exception | Clear context and re-raise unchanged; no duplicate prior output. |
| Process termination | Existing worker failure; no persistent WAPA state exists. |
| Concurrent O2PAGCON change | Exact payload join plus verification detects pre-decode drift; later change retains current non-snapshot semantics. |
| Mixed version | Private helpers and consumers activate in one class publication unit. |
| Rollback | Reactivate prior complete class; no DDIC/data/RFC migration. |

WAPA remains read-only. The outer worker remains transaction owner. No COMMIT/ROLLBACK is added.

## 7. Bounded complexity proof

Let an initial group contain `n <= 1000` pages and let root depth be 0. Define `Q(d)` as the
maximum raw-helper SELECTs below one attempted node at depth `d`:

```text
Q(5) = max(3 for admission success, 1 for rejection then reference) = 3
Q(d) = max(3, 1 + 2*Q(d+1))
Q(0) = 127
```

The `1` is a rejected admission manifest. Payload and verification occur only on an admitted
leaf. A fully rejected tree uses 63 manifest SELECTs and then references 32 contiguous ranges.
The 127 maximum occurs when every depth-0..4 node rejects and all 32 depth-5 nodes admit.

For `P` pages and `G = CEIL(P/1000)`:

| Pages | Healthy raw SELECTs | Maximum raw SELECTs | Maximum reference pages | New HTTP/aRFC |
|---:|---:|---:|---:|---:|
| 1 | 3 | 3 | 1 | 0 |
| 1,000 | 3 | 127 | 1,000 | 0 |
| 40,000 | 120 | 5,080 | 40,000 | 0 |
| 1,000,000 | 3,000 | 127,000 | 1,000,000 | 0 |

Reference cluster imports are unchanged and bounded by `REFERENCE_PAGES..3*REFERENCE_PAGES`.
No raw-helper query occurs inside the reference-page loop. SQL complexity is `O(G)` for healthy
raw processing, at most `127G` for adversarial raw processing, plus the explicitly selected
reference path. HTTP complexity is zero new calls. CPU is `O(R log R)` manifest/key ordering plus
`O(R)` row consumption; no `O(P*R)` scan exists.

Row/byte policy: metadata probe <=40,001 rows; payload <=36,333 rows and <=104,857,038 charged
bytes; one fragment <=2,886 bytes; one owner-bounded decoded page <=15 MiB. An oversized or
anomalous range splits only through depth 5; after that the current contiguous range is reference.

Expected behavior:

- 1,000 objects/pages: one healthy range; at most 127 raw SELECTs under adversarial rejects.
- 40,000: 40 healthy ranges; executable adversarial ceiling 5,080 raw SELECTs.
- 1,000,000: 1,000 healthy ranges; analytical ceiling 127,000 raw SELECTs; no execution claim.

## 8. Counters, tests, and acceptance

Preserve `GET_RAW_PREFETCH_COUNTERS` first two outputs:

- `EV_HITS`: increment once after a `SERIALIZE` call finishes with every non-controller page
  consumed through raw admission and no reference page.
- `EV_FALLBACKS`: increment once after a `SERIALIZE` call that consumed one or more reference
  pages. A call increments exactly one of HITS/FALLBACKS, never both.

Add these exact optional exporting parameters without changing existing callers:

```text
EV_MANIFEST_SELECTS       actual metadata SELECT executions, including verification
EV_PAYLOAD_SELECTS        actual CLUSTD SELECT executions
EV_VERIFY_SELECTS         verification manifests; subset of EV_MANIFEST_SELECTS
EV_SPLIT_EVENTS           rejected nonterminal nodes split into two
EV_DEPTH_CEILING_HITS     rejected multi-page nodes at depth 5
EV_RANGE_FALLBACKS        terminal contiguous ranges entered
EV_RANGE_FALLBACK_PAGES   pages assigned to terminal range fallback
EV_DECODE_FALLBACK_PAGES  suffix pages referenced after decode failure/15 MiB breach
EV_REFERENCE_PAGES        READ_PAGE calls made with raw context inactive
EV_MAX_MANIFEST_ROWS      maximum observed metadata rows before rejection
EV_MAX_CHARGED_RAW_BYTES  maximum admitted/rejected charged capacity
EV_MAX_RAW_SPLIT_DEPTH    maximum attempted depth, never greater than 5
```

Every counter saturates at MAX_INT (INT8 for bytes), reset clears all, and SQL totals use
`EV_MANIFEST_SELECTS + EV_PAYLOAD_SELECTS`; `EV_VERIFY_SELECTS` is diagnostic only and not
double-counted. Backing class-data symbols use the same suffixes with prefix `GV_`.

Required focused tests:

- `MANIFEST_ROW_CAP`: 40,001 rejects before payload.
- `MANIFEST_BYTE_CAP`: 36,333 admits; 36,334 rejects before payload.
- `PAYLOAD_EXACT_MATCH` and `PAYLOAD_DRIFT_REJECT`.
- `SPLIT_DEPTH_CEILING_REF`: force every attempt to reject; assert 63 manifest SELECTs, zero
  payload/verification SELECTs, 32 range fallbacks, 1,000 reference pages, max depth 5.
- `CEILING_SELECTS_REFERENCE_RANGE`: force one depth-5 multi-page reject; assert the complete
  contiguous range uses reference in order and no subsequent manifest/payload/verify call occurs.
- `PAIR_OVER_BUDGET_RANGE_REF`: pairs exceed 100 MiB while single pages would admit; assert the
  depth ceiling selects range reference rather than singleton raw re-admission.
- `DEPTH5_LEAVES_ADMIT`: assert the exact maximum 63 manifests +32 payloads +32 verifications =
  127 total raw-helper SELECTs.
- `SUCCESS_THEN_SPLIT_CLEARS`: successful near-cap group followed by split/fallback; maps empty
  at every boundary and byte parity holds.
- `DECODE_FAIL_SUFFIX_REF`: prior pages appear once; current suffix references once; admitted
  rows are freed before first reference import.
- `DECODED_15M_BOUND`: exact bound passes; bound+1 frees maps/rows and references suffix.
- `COUNTER_EXACT_TREE` and `COUNTER_SATURATION`.

The 40,000-page instrumented acceptance uses 40 contiguous groups and a sink/hash accumulator.
Run healthy, fully rejected, depth-5-admitted, pair-over-budget, payload-drift, and decode-failure
fixtures. Assert output order/parity; max depth 5; raw SELECT totals 120 healthy and <=5,080 for
every adversarial run; no rejected-parent payload; no raw SELECT after each range fallback;
metadata <=40,001; admitted rows <=36,333; charged bytes <=104,857,600; maps/rows empty at
fallback boundaries; no dump; bounded diagnostics.

Paired IT8 `/O4H/COMPANION` acceptance is mandatory before production publication:

1. Exact path/mode/content/SHA-1 parity against current reference output.
2. `RANGE_FALLBACKS = 0`, `RANGE_FALLBACK_PAGES = 0`, `DEPTH_CEILING_HITS = 0`, and
   `REFERENCE_PAGES = 0` for raw-eligible pages.
3. `MAX_SPLIT_DEPTH_OBSERVED = 0` unless a traced source mutation invalidates the run.
4. Every decoded logical page <=15,728,640 bytes; record largest page and key.
5. Focused SAT shows one current kernel IMPORT transient, no retained current-page CLUSTD rows
   during IMPORT, no more than four application page-scale logical owners, and no unexplained
   duplicate full-range payload.
6. O2PAGCON time is at least 20% below 10.54 seconds; total worker time is no worse than 42.75
   seconds; process peak does not exceed reference peak by more than the explicitly observed
   bounded raw/one-page owners.
7. Syntax, activation, ABAP Unit, ATC, and active-source re-read pass.

Any nonzero fallback on `/O4H/COMPANION`, page >15 MiB, owner-count breach, unclassified kernel
transient, parity difference, or performance/memory regression stops publication.

## 9. Implementation packets

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_WAPA
METHOD_OR_DDIC=PRIVATE CONSTANTS/TYPES AND GET_RAW_PREFETCH_COUNTERS
ANCHOR=C_MAX_RAW_PREFETCH_ROWS; TY_RAW_ROW_TT; GV_RAW_PREFETCH_HITS; GET_RAW_PREFETCH_COUNTERS
ACTION=replace
CHANGE=Declare section 4.1 constants/types and section 8 counters. Preserve EV_HITS/EV_FALLBACKS positions and semantics; append optional detailed exports. Reset clears every counter. Saturate increments.
INVARIANTS=WAPA-INV-02,WAPA-INV-06,WAPA-INV-10,WAPA-INV-12
SQL_SHAPE=NONE
ERROR_ROLLBACK_FALLBACK=Counter overflow saturates and never raises
TESTS=COUNTER_EXACT_TREE;COUNTER_SATURATION
VALIDATION=IT8 syntax; existing callers compile unchanged
STOP_IF=Any symbol exceeds 30 characters or existing counter caller breaks

FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_WAPA
METHOD_OR_DDIC=CLEAR_RAW_CONTEXT;SERIALIZE_REFERENCE_RANGE
ANCHOR=TRY_RAW_PREFETCH declaration and implementation
ACTION=insert
CHANGE=Implement exact sections 4.5 and 6. Reference range loops once in order, clears before/after every READ_PAGE, and has no raw/split/self call.
INVARIANTS=WAPA-INV-01,WAPA-INV-08,WAPA-INV-09
SQL_SHAPE=NONE directly; unchanged READ_PAGE performs 1..3 cluster imports per non-controller/full page
ERROR_ROLLBACK_FALLBACK=Clear context then re-raise unchanged; no retry
TESTS=CEILING_SELECTS_REFERENCE_RANGE;DECODE_FAIL_SUFFIX_REF
VALIDATION=Static call search proves no raw helper below SERIALIZE_REFERENCE_RANGE
STOP_IF=Reference helper can re-enter admission or emit a page twice

FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_WAPA
METHOD_OR_DDIC=READ_RAW_MANIFEST
ANCHOR=READ_RAW_ROWS declaration/implementation
ACTION=insert
CHANGE=Implement section 4.2 exactly with production-bounded injectable limits and R/B/A reject reason.
INVARIANTS=WAPA-INV-05,WAPA-INV-07,WAPA-INV-10
SQL_SHAPE=One metadata-only requested-key join selecting PAGEKEY,OBJTYPE,SRTF2,CLUSTR with unchanged RELID/APPLNAME/VERSION and UP TO @LV_PROBE_ROWS ROWS
ERROR_ROLLBACK_FALLBACK=Free rejected manifest; caller applies depth rule
TESTS=MANIFEST_ROW_CAP;MANIFEST_BYTE_CAP
VALIDATION=IT8 syntax and SQL trace projection
STOP_IF=CLUSTD is projected or live CLUSTD width differs from 2886

FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_WAPA
METHOD_OR_DDIC=READ_RAW_ROWS
ANCHOR=Current READ_RAW_ROWS key loop and SELECT
ACTION=replace
CHANGE=Implement section 4.3 exactly; exact manifest join, defensive cap+1, then verification manifest; clear ET_ROWS unless complete.
INVARIANTS=WAPA-INV-04,WAPA-INV-05,WAPA-INV-07,WAPA-INV-10
SQL_SHAPE=One payload SELECT joined on PAGEKEY,OBJTYPE,SRTF2,CLUSTR plus one metadata verification SELECT
ERROR_ROLLBACK_FALLBACK=Incomplete with rows freed; caller applies depth rule
TESTS=PAYLOAD_EXACT_MATCH;PAYLOAD_DRIFT_REJECT
VALIDATION=Trace rows <=36333 and charged bytes <=104857600
STOP_IF=Any nonmanifest row can enter payload

FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_WAPA
METHOD_OR_DDIC=DECODE_RAW_PAGE
ANCHOR=ASSEMBLE_AND_DECODE declaration/body
ACTION=replace
CHANGE=Implement section 5 exactly. Consume CT_ROWS by BY_PAGE, delete each source row after append, IMPORT one key at a time, free key buffer immediately, return three local maps and EV_DECODED_BYTES.
INVARIANTS=WAPA-INV-03,WAPA-INV-04,WAPA-INV-11
SQL_SHAPE=NONE
ERROR_ROLLBACK_FALLBACK=Raise existing exception with locals freed; caller references unconsumed suffix
TESTS=DECODE_FAIL_SUFFIX_REF;DECODED_15M_BOUND
VALIDATION=SAT and row-lifetime assertions
STOP_IF=Method copies/sorts all CT_ROWS or current-page rows survive until IMPORT

FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_WAPA
METHOD_OR_DDIC=RAW_PREFETCH_AND_READ
ANCHOR=TRY_RAW_PREFETCH declaration/body
ACTION=replace
CHANGE=Implement section 4.4 exactly with IV_SPLIT_DEPTH default 0, terminal depth 5, first-half then second-half recursion, page consume, and suffix fallback.
INVARIANTS=WAPA-INV-01,WAPA-INV-04,WAPA-INV-06,WAPA-INV-08,WAPA-INV-09
SQL_SHAPE=Three SELECTs per admitted node; one metadata SELECT per rejected node; none below terminal reference helper
ERROR_ROLLBACK_FALLBACK=Exact section 6 matrix
TESTS=SPLIT_DEPTH_CEILING_REF;DEPTH5_LEAVES_ADMIT;PAIR_OVER_BUDGET_RANGE_REF;SUCCESS_THEN_SPLIT_CLEARS
VALIDATION=Assert max 127 raw SELECTs per initial group and max depth 5
STOP_IF=Depth-5 reject splits/retries or any fallback re-enters raw admission

FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_WAPA
METHOD_OR_DDIC=ADD_PAGE_CONTENT_FILE;ADD_FULL_PAGE_DETAILS
ANCHOR=Raw-prefetch-active READ TABLE branches
ACTION=replace
CHANGE=Use ASSIGNING raw map reads and consume the one-page context without copying a whole raw map; preserve reference branches byte-for-byte.
INVARIANTS=WAPA-INV-01,WAPA-INV-11
SQL_SHAPE=NONE on raw hit; unchanged 1..3 reference imports on miss
ERROR_ROLLBACK_FALLBACK=Existing behavior unchanged
TESTS=SERIALIZE_ORDER_PARITY;DECODE_FAIL_SUFFIX_REF
VALIDATION=SAT logical-owner count <=4
STOP_IF=Reference IMPORT statement or parity changes

FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_WAPA
METHOD_OR_DDIC=SERIALIZE
ANCHOR=TRY_RAW_PREFETCH followed by LOOP AT LT_PAGES and READ_PAGE
ACTION=replace
CHANGE=Keep BUILD_CONTEXT and final XML ADD. Partition LT_PAGES into contiguous <=1000-page groups and call RAW_PREFETCH_AND_READ at depth 0. Track whole-serialize hit/fallback semantics. Clear context on exit/exception.
INVARIANTS=WAPA-INV-01,WAPA-INV-02,WAPA-INV-05,WAPA-INV-09
SQL_SHAPE=NONE directly
ERROR_ROLLBACK_FALLBACK=Existing exception contract; context cleared before re-raise
TESTS=SERIALIZE_ORDER_PARITY;SERIALIZE_EMPTY_PAGES;COUNTER_EXACT_TREE
VALIDATION=Full WAPA Unit, ATC, active-source re-read, byte/path/mode parity
STOP_IF=BUILD_CONTEXT, final order, ORCH, BATCH, RFC, DDIC, or standard source changes
```

## 10. Rejected alternatives and checkpoints

- Unbounded/progressive bisection: violates WAPA-PD-04 and owner decision.
- Raw singleton re-admission after ceiling: recreates SQL per page.
- Full-WAPA or all-refs fallback: unnecessary; fallback is the current contiguous range only.
- CLUSTR as decoded bytes or a pre-decode 15 MiB claim: wrong domain.
- Post-decode split/retry: allocation already occurred; use terminal suffix reference after cleanup.
- WAPA cache/DDIC metadata/streaming decoder: outside approved scope.
- ORCH/RFC/standard changes: unrelated and forbidden.

Checkpoint order is mandatory:

1. A: constants/types/counters, clear helper, manifest helper and cap tests.
2. B: payload/verification helper and exact SQL trace.
3. C: consuming page decoder, owner-count tests, state-lifecycle tests.
4. D: depth-5 range fallback and exact 63/127 query tests.
5. E: SERIALIZE redirect, full Unit/ATC/parity.
6. F: 40k acceptance and paired `/O4H/COMPANION` SAT/trace acceptance.

Source completeness: productive diff is WAPA main plus WAPA test include only. ORCH/BATCH/RFC,
standard classes, DDIC, state, and diagrams remain byte-identical. Publication activates the
complete class, re-reads active distinctive symbols, checks active/inactive alignment, then runs
all gates. Rollback reactivates the prior full class; no migration or mixed persistent state exists.

```text
DESIGN_GATE=READY_FOR_CORRECTNESS_AND_PERFORMANCE_RE_REVIEW
IMPLEMENTATION_ALLOWED=NO_UNTIL_OWNER_REVIEW
OPEN_BLOCKERS=0
OPEN_MAJORS=0
NEXT_ACTION=Independent correctness and performance re-review of revision 3
```
