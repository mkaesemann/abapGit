# FDT0 Local Runtime Cache -- Adversarial Design Review (Cycle 1)

```text
TASK_ID=FDT0_LOCAL_CACHE_ADVERSARIAL_REVIEW_20260812
CYCLE=1 of max 3
BASELINE_COMMIT=b4f41e38372a0fe9f67483f71e968b1885b594c1
DESIGN_ARTIFACT=.memory/logs/fdt0_local_cache_design.md
DESIGN_STATUS_AT_REVIEW=DESIGN_COMPLETE_AWAITING_REVIEW
VERDICT=REVISE_AND_REVIEW_ONCE
OPEN_BLOCKER=2 (AR-1-001, AR-1-002)
OPEN_MAJOR=1 (AR-1-003)
OPEN_MINOR=1 (AR-1-004)
CLOSED=AR-1-005 (unactivated ORCH draft confirmed whitespace-only via independent
  action=diff; route_to_sequential_fallback anchor text unaffected)
```

## Scope of verification performed this cycle

- Read the design in full (`.memory/logs/fdt0_local_cache_design.md`, all 16
  sections) and `.memory/state.md` for cross-topic invariants.
- Live-read (IT8, this session): `Z_ABAPGIT_ORTEC_SER_BATCH` (full FM body),
  `ZCL_ABAPGIT_ORTEC_SER_ORCH` methods `serialize`, `route_to_sequential_fallback`,
  `dispatch_batch`, `partition_objects`, plus its full method inventory;
  `ZCL_ABAPGIT_ORTEC_SER_COST` FDT0 cost-estimate branch; `ZCL_ABAPGIT_ORTEC_GIT_SWITCH`
  full method inventory; `ZCL_ABAPGIT_ORTEC_OBJ_STORE` `store_object`/`store_objects`;
  `ZCL_ABAPGIT_OBJECT_FDT0` `get_application_id`, `filter_xml_serialize` (full body),
  `check_is_local`, `exists`, `before_xml_deserialize`; DDIC of `FDT_ADMN_0000S`,
  `FDT_INC_KEY_0001`, `FDT_INC_ADMN_0000_DATA`, `FDT_INC_QUERY_0000_DATA`; public
  method inventory of `zcl_abapgit_hash`; independent `action=diff` re-check of the
  flagged unactivated `ZCL_ABAPGIT_ORTEC_SER_ORCH` draft.
- No productive code/DDIC/test edits made. Design artifact not modified.

## Findings

### AR-1-001 -- BLOCKER

```text
ID=AR-1-001
SEVERITY=BLOCKER
CLAIM=FDT0-INV-01/02 hold because ZCL_ABAPGIT_ORTEC_GIT_SWITCH=>is_fdt0_cache_active/
  set_fdt0_cache_active, a session-scoped CLASS-DATA flag defaulting FALSE, safely
  gates the new cache, with "Production caller is exclusively
  ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE, mirroring the existing
  SET_SERIAL_PREFETCH_ACTIVE on-entry/off-on-every-exit lifecycle" (design section 4).
COUNTEREXAMPLE=Per ZCL_ABAPGIT_ORTEC_SER_COST=>get_estimate (WHEN 'FDT0':
  c_default_ms_brf = 150000), FDT0 is costed as the heaviest known object type, and
  per ZCL_ABAPGIT_ORTEC_SER_ORCH=>partition_objects it is never forced_seq and never
  WAPA -- it is routed into the normal ELIGIBLE/planner/batch path and dispatched via
  dispatch_batch's "CALL FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH' STARTING NEW TASK ...
  DESTINATION IN GROUP". FDT0 objects -- by design intent and by the design's own
  EVIDENCE (the 276078 ms / 18.6% measurement) -- are exactly the traffic this cache
  exists to help, and that traffic goes through the aRFC batch worker (interception
  anchor #1), not through route_to_sequential_fallback (anchor #2).
  Z_ABAPGIT_ORTEC_SER_BATCH's own live source contains an explicit, already-fixed
  prior-incident comment for precisely this failure mode: "this worker session is a
  SEPARATE aRFC process from ZCL_ABAPGIT_ORTEC_SER_ORCH - CLASS-DATA does not cross
  the RFC boundary, so every object serializer's own IF is_serial_prefetch_active( )
  ... gate ... was silently FALSE here" -- and the fix that incident required was an
  explicit zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_true ) call
  INSIDE the FM body itself (SER-SLICE-5). The FDT0 design's own edit list (section 4)
  has no equivalent: no new IMPORTING flag/buffer parameter is added to
  Z_ABAPGIT_ORTEC_SER_BATCH, and no call to set_fdt0_cache_active is ever added to
  that FM body. Independently: no anchor edit anywhere in section 4 ever calls
  set_fdt0_cache_active( abap_true ) at all -- not even in
  ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE, whose live body (read this session) has no
  such call today and is not listed as an edit target in section 4's decision-free
  pseudocode blocks (only route_to_sequential_fallback's call-site replacement is
  listed for that class). As literally specified, mv_fdt0_cache_active can never
  become abap_true in ANY process, on ANY code path -- the cache is inert on both
  interception anchors, and doubly inert on the batch anchor that is the design's own
  primary target.
EVIDENCE=Live SAPRead this session (IT8): Z_ABAPGIT_ORTEC_SER_BATCH (FM body +
  SER-SLICE-5 comment), ZCL_ABAPGIT_ORTEC_SER_ORCH methods serialize /
  route_to_sequential_fallback / dispatch_batch / partition_objects,
  ZCL_ABAPGIT_ORTEC_SER_COST get_estimate FDT0 branch (c_default_ms_brf=150000).
  Design sections 2 ("Transaction owner"), 4 (GIT_SWITCH block), 5 (Interception
  anchors).
IMPACT=correctness/functional -- not a crash risk, but the entire feature is
  permanently dead code: the AC-04 hit-path row can never be reached in any real
  production run, and IT-04's warm-run collapse of the 276078 ms / 18.6% EVIDENCE
  figure -- the design's entire stated purpose -- can never be observed.
REQUIRED_CHANGE=(a) Add an explicit activation edit to
  ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE (already in EXACT_SOURCE_SCOPE) calling
  zcl_abapgit_ortec_git_switch=>set_fdt0_cache_active( abap_true ) on entry and
  abap_false on every exit path, mirroring the existing set_serial_prefetch_active
  bracket already in that method -- closes anchor #2 (shares ORCH's dialog session).
  (b) For the aRFC batch anchor (#1), a bare CLASS-DATA flag cannot work at all --
  add a new IMPORTING parameter to Z_ABAPGIT_ORTEC_SER_BATCH (e.g.
  iv_fdt0_cache_active TYPE abap_bool), pass it from dispatch_batch's CALL FUNCTION
  (sourced from the flag ORCH set in (a)), and call
  zcl_abapgit_ortec_git_switch=>set_fdt0_cache_active( iv_fdt0_cache_active ) inside
  the FM body before its LOOP AT it_tadir, resetting to abap_false after the loop --
  the identical bracket pattern SER-SLICE-5 already established for
  set_serial_prefetch_active in that same FM. Update sections 4/5 to list both edits
  as new anchors; add a UT (flag ON in a simulated fresh/separate session state ->
  cache still engages) and an IT test dispatching a real FDT0 object through the
  actual aRFC batch path end-to-end (not only via route_to_sequential_fallback).
RETEST=Re-review confirms: (1) a diff-visible edit calls set_fdt0_cache_active(
  abap_true/abap_false ) inside Z_ABAPGIT_ORTEC_SER_BATCH's FM body bracketing the
  LOOP; (2) ORCH=>SERIALIZE has an equivalent bracket; (3) a live IT8 IT-04-style run
  with the flag on shows a warm-run hit on an FDT0 object dispatched through the
  NORMAL (non-forced-sequential) batch path.
```

### AR-1-002 -- BLOCKER

```text
ID=AR-1-002
SEVERITY=BLOCKER
CLAIM=Design section 6 "proves" FDT0-INV-03/AC-03 (ADD/UPDATE/DELETE anywhere in the
  application's owned graph always changes the signature) via a full-graph SELECT
  over fdt_admn_0000s rows sharing application_id, hashing
  id/object_type/version/ch_timestamp/deleted/tv_state/tv_timestamp/obsolete per row.
COUNTEREXAMPLE=The proof is sound only if every BRF+ CONTENT-level edit to a nested
  artifact (a rule's formula text, a decision table's actual row/cell values, an
  expression's configuration) always bumps that same artifact's own
  version/ch_timestamp in fdt_admn_0000s. The design's own evidence table only
  empirically confirms a version/ch_timestamp bump for the TOP-LEVEL APPLICATION
  row's own save counter (version=000007 observation) -- it never empirically
  confirms this for a NESTED leaf object's content edit specifically. This is exactly
  the open question IT-01 ("add then remove a comment/description on one nested
  rule") is meant to close, per the design's own section 9 test plan -- but section
  13 explicitly marks AC-07 "ADDRESSED (planned, not yet written/run - design
  stage)". In BRF+, several artifact kinds (notably decision-table row/cell content,
  as distinct from the decision table's own column/structure definition) are
  commonly persisted in dedicated FDT content/runtime tables distinct from the admin
  metadata table -- if any such content class does not reliably bump its owning
  object's version/ch_timestamp on every content-only save, this design's central
  correctness claim (a false hit is architecturally impossible) is false for that
  content class, and AC-03's non-false-hit proof does not hold as a general
  statement about "any graph change" -- only about admin-metadata-visible changes.
  This is materially different from, and more fundamental than, the
  "cross-application reference" residual risk the design already flags in
  sections 6/11 -- it is about whether an in-application, in-owned-object content
  edit that a real serialize() call would reflect is provably reflected in the
  signature, and it has not been empirically tested on any BRF+ artifact type, only
  theorized.
EVIDENCE=Design section 1 evidence table (top-level-application-only
  version/ch_timestamp observation), section 6 (proof text scoped to "every field a
  content change can plausibly touch"), section 9 IT-01 definition, section 13 AC-07
  status ("planned, not yet written/run"). Cross-checked this session against
  FDT_INC_QUERY_0000_DATA (version/name/application_id/access_level) and
  FDT_INC_ADMN_0000_DATA (ch_timestamp/tv_state/tv_timestamp/obsolete/deleted) DDIC
  structures -- these are administrative/versioning fields only; no content-payload
  field exists in this table, confirming the signature is necessarily a metadata
  proxy for content, not content itself, so its correctness is entirely contingent
  on this unverified metadata-follows-content guarantee holding for every BRF+
  artifact kind actually exercised in production.
IMPACT=correctness -- the risk class is a FALSE CACHE HIT (stale output silently
  returned as if current), the single most severe failure mode this design exists to
  prevent (FDT0-INV-01/03), and it is currently unverified for at least one
  plausible artifact kind (decision-table content).
REQUIRED_CHANGE=Before this design may be approved, execute IT-01 (or a stronger
  equivalent) against a real decision-table object specifically (not just a generic
  "nested rule" description edit): (1) resolve a low-risk BRF+ application
  containing at least one decision table; (2) record its full
  compute_signature-shape row set; (3) edit only a decision-table cell/row VALUE (not
  its structure, not its description) and save; (4) re-pull the row set and confirm
  at least one row's version/ch_timestamp changed. Repeat for at least one
  rule/expression FORMULA-only edit. If either experiment shows no detectable
  row-set change, the signature algorithm in section 6 must be revised (e.g.
  incorporate a structural/content hash sourced from the actual FDT content tables,
  or fall back to hashing the exported+filtered XML itself for a cheap pre-check)
  before this design can rely on fdt_admn_0000s alone. Document the actual measured
  result (not a plausibility argument) in the design's evidence table.
RETEST=Re-review requires a new section 1 evidence-table row with a live IT8
  measurement (query results before/after, showing which row(s) changed) for at
  least one decision-table content-only edit and one formula-only edit, both on real
  objects, both showing a version/ch_timestamp (or other hashed field) change.
```

### AR-1-003 -- MAJOR

```text
ID=AR-1-003
SEVERITY=MAJOR
CLAIM=Design section 4 compute_signature pseudocode computes the signature via
  `zcl_abapgit_hash=>sha1( if_data = zcl_abapgit_convert=>string_to_xstring_utf8(
  lv_concat ) iv_hash_algo = 'SHA1' )`, flagged by the design's own section 11
  STOP_IF as "not yet read this session - implementation MUST re-verify... before
  writing this method".
COUNTEREXAMPLE=Live read of zcl_abapgit_hash's full public method inventory (this
  session) shows no such overload exists. The only IMPORTING parameter names on any
  public method are iv_type+iv_data (sha1), or iv_data alone (sha1_blob,
  sha1_commit, sha1_raw, sha1_string, sha1_tag, sha1_tree) -- there is no if_data
  parameter and no iv_hash_algo parameter anywhere in this class. The pseudocode as
  literally written in section 4 will not compile. Separately, a much simpler
  existing method directly fits this use case without any xstring conversion at
  all: `CLASS-METHODS sha1_string IMPORTING iv_data TYPE string RETURNING
  VALUE(rv_sha1) TYPE zif_abapgit_git_definitions=>ty_sha1 RAISING
  zcx_abapgit_exception` -- takes lv_concat (already a STRING) directly.
EVIDENCE=Live SAPRead method=* on zcl_abapgit_hash, this session (8 public methods
  enumerated, none named/shaped as the design assumed).
IMPACT=buildability -- guaranteed compile failure exactly as specified; also a
  missed simplification (the design's own string_to_xstring_utf8 conversion step
  becomes unnecessary).
REQUIRED_CHANGE=Replace section 4's compute_signature body with:
  `rv_signature = to_upper( zcl_abapgit_hash=>sha1_string( lv_concat ) ).`
  removing the zcl_abapgit_convert=>string_to_xstring_utf8 call entirely (sha1_string
  takes the STRING directly). Note sha1_string RAISES zcx_abapgit_exception --
  compute_signature's own declared signature (section 4) has no RAISING clause, so
  this call must be wrapped in TRY/CATCH zcx_abapgit_exception with the same "treat
  as cache-unusable, return initial" fallback already specified for the
  empty/oversized-row-set case, to preserve FDT0-INV-02's "never propagate a cache
  helper failure" contract.
RETEST=Re-review confirms the updated section 4 pseudocode calls only a real,
  existing zcl_abapgit_hash overload with correct parameter names, and that the
  RAISING contract mismatch is closed with an explicit TRY/CATCH.
```

### AR-1-004 -- MINOR (closes a design-flagged stop condition)

```text
ID=AR-1-004
SEVERITY=MINOR
CLAIM=Design section 11 flags "Whether fdt_admn_0000s is client-dependent was not
  confirmed from the rendered DDL this session" as an open stop condition, and
  section 3 defends the new table's client key as "safe either way" pending that
  verification.
COUNTEREXAMPLE=Not a counterexample -- closing the open question with live evidence.
  FDT_INC_KEY_0001 (the sole key include of FDT_ADMN_0000S) contains only
  `id : fdt_uuid` -- no MANDT/client field anywhere in the key. FDT_ADMN_0000S is
  therefore genuinely CLIENT-INDEPENDENT (consistent with its
  @AbapCatalog.deliveryClass : #S). A given application_id's admin-row graph, and
  therefore export_xml_application's output, is identical across every client in
  this system -- the new ZAOG_FDT_CACHE table's `client` key does not add
  correctness (still "safe", confirming the design's own hedge) but does silently
  fragment the cache once per client for content that is provably identical across
  clients, reducing hit rate with no compensating benefit.
EVIDENCE=Live SAPRead this session: FDT_ADMN_0000S (table def), FDT_INC_KEY_0001
  (key structure, id only).
IMPACT=performance/design-cleanliness only, not correctness -- non-blocking.
REQUIRED_CHANGE=Owner's discretion: either (a) keep the client key as defensive
  convention (design's own fallback, now confirmed safe), documenting in section 3
  that it is a deliberate no-cost safety margin rather than a load-bearing
  necessity, or (b) drop the client key from the primary key (keep MANDT as a
  buffer-table field only, not key, per standard #L buffer-table convention) to
  share cache hits across clients. Either resolution is acceptable; section 11's
  stop condition should be marked CLOSED with this evidence either way.
RETEST=Re-review confirms section 11's stop condition is marked CLOSED with this
  session's DDIC evidence, and section 3 states an explicit rationale for whichever
  key design is kept.
```

## Verified-clean claims (no finding -- independently re-checked)

- `route_to_sequential_fallback`'s anchor text (design section 4) matches the live
  ACTIVE source exactly, word-for-word; the previously-flagged unactivated draft on
  `ZCL_ABAPGIT_ORTEC_SER_ORCH` is confirmed whitespace/comment-alignment only via an
  independent `action=diff` this session (touches only `dispatch_batch`'s parameter
  alignment and two `#EC CI_HASHSEQ` comment columns, nowhere near
  `route_to_sequential_fallback`) -- no anchor-matching risk.
- `resolve_application_id`'s proposed SQL is a verbatim match of
  `ZCL_ABAPGIT_OBJECT_FDT0`'s own private `GET_APPLICATION_ID`
  (`SELECT SINGLE application_id FROM fdt_admn_0000s WHERE object_type = 'AP' AND
  name = ms_item-obj_name`, no `deleted` filter in either) -- the cache introduces
  no new ambiguity beyond what the standard object already has.
- `filter_xml_serialize`'s volatile-field list quoted in design sections 1/10 is a
  complete, verified match against the live method body (`ComponentReleases` node
  removal + 11 named `set_field` calls + 8 root-attribute clears) -- the parity
  proof in section 10 is sound given the (unrelated) content-vs-metadata gap in
  AR-1-002.
- `ZCL_ABAPGIT_ORTEC_OBJ_STORE=>store_object`/`store_objects` do use plain
  `MODIFY`/`MODIFY ... FROM TABLE` as the design's evidence table claims -- but
  `store_object` DOES raise `zcx_abapgit_ortec_git` on `sy-subrc <> 0` (unlike the
  new design's own `store()`, which deliberately swallows the failure). This
  divergence is already explicitly called out and justified in design section 4
  ("this intentionally diverges..."), so it is not a fresh finding.
- `ZCL_ABAPGIT_ORTEC_SER_COST` confirms `FDT0` is costed via `c_default_ms_brf =
  150000` / `c_default_bytes_brf = 4000000`, consistent with design section 2's
  evidence narrative -- but this is also the direct evidence underpinning AR-1-001
  (FDT0 is routed to the batch/aRFC path, not the sequential-fallback path, in the
  normal case).

## Invariant coverage this cycle

| Invariant | Status |
|---|---|
| FDT0-INV-01 (hit = output-parity) | AT RISK -- AR-1-002 (signature soundness unverified for content-only edits) |
| FDT0-INV-02 (never degrades) | Mechanism sound in isolation, but AR-1-001 makes it moot on the primary path (cache never engages there at all) |
| FDT0-INV-03 (any graph change invalidates) | AT RISK -- AR-1-002 |
| FDT0-INV-04 (ORTEC-only changes) | Holds structurally; AR-1-001's required fix stays inside already-in-scope objects |
| FDT0-INV-05 (bounded payload/memory) | No new finding this cycle |
| FDT0-INV-06 (atomic publication) | No new finding this cycle |
| FDT0-INV-07 (concurrency safety) | No new finding this cycle -- content-addressing argument holds |
| FDT0-INV-08 (local #L runtime-only) | No new finding this cycle; AR-1-004 closes the client-dependency stop condition |

## Acceptance criteria coverage this cycle

| AC | Status |
|---|---|
| AC-01 (DDIC) | No new finding; activation-rejection stop condition remains open per design's own section 3 STOP_IF (no DDIC changes made this cycle to test) |
| AC-02 (exact names/anchors) | route_to_sequential_fallback anchor independently re-verified CLEAN; Z_ABAPGIT_ORTEC_SER_BATCH anchor text also verified correct, but AR-1-001 requires a NEW anchor (additional IMPORTING parameter + activation call) not yet in section 4 |
| AC-03 (non-false-hit signature proof) | BLOCKED -- AR-1-002 |
| AC-04 (hit/miss/corrupt/concurrent) | Logic sound on paper; AR-1-001 means the hit branch is unreachable as specified |
| AC-05 (parity/volatile XML) | Independently verified CLEAN (filter_xml_serialize full-body match) |
| AC-06 (LUW/locking/bounds) | No new finding this cycle |
| AC-07 (tests+IT8) | Still design-stage per design's own section 13; AR-1-002's REQUIRED_CHANGE adds a mandatory pre-approval IT-01 variant |
| AC-08 (scope/non-goals/stop conditions) | AR-1-004 closes one of two open stop conditions; AR-1-003 closes the other (zcl_abapgit_hash API) with a concrete fix |

## Verdict (Cycle 1)

REVISE_AND_REVIEW_ONCE. Two BLOCKERs (AR-1-001, AR-1-002) must be closed with
concrete design changes and/or live IT8 evidence, and the MAJOR (AR-1-003) must be
corrected in the design text, before the next review cycle. No owner policy decision
is required to close any of these -- all three are objective technical corrections.

---

# Cycle 2

```text
CYCLE=2 of max 3
DESIGN_ARTIFACT_AT_REVIEW=.memory/logs/fdt0_local_cache_design.md
  (TASK_ID=FDT0_LOCAL_CACHE_DESIGN_REVISION_20260812,
  STATUS=DESIGN_REVISED_CYCLE_1_AWAITING_REVIEW)
VERDICT=REVISE_AND_REVIEW_ONCE
OPEN_BLOCKER=0
OPEN_MAJOR=1 (AR-2-001, reopens the enforcement gap in AR-1-002's closure)
OPEN_MINOR=2 (AR-1-001-RESIDUAL, AR-1-004 -- both non-blocking)
CLOSED=AR-1-001 (BLOCKER, independently re-verified), AR-1-003 (MAJOR,
  independently re-verified)
```

## Scope of verification performed this cycle

Independent live re-reads this session (IT8), cross-checked against the design's own
citations rather than trusted at face value:

- `Z_ABAPGIT_ORTEC_SER_BATCH`: full signature (`includeSignature=true`, confirms
  `processingType=rfc`) and a targeted grep across the whole FM body for
  `set_serial_prefetch_active`, `LOOP AT`/`ENDLOOP`, every `CATCH`, `RAISE` -- to
  independently confirm line-172/285-287/311 anchor claims and the exception-handling
  shape around the per-object loop (no bare `RAISE error` found anywhere in the FM;
  exactly one `CATCH zcx_abapgit_exception INTO lx_error` scoped inside the loop).
- `ZCL_ABAPGIT_ORTEC_SER_ORCH` methods `serialize`, `dispatch_batch`,
  `route_to_sequential_fallback` (full bodies, active version).
- `ZCL_ABAPGIT_ORTEC_SER_ORCH` `action=diff` (active -> inactive), re-run fresh this
  cycle to catch any drift since Cycle 1.
- `ZCL_ABAPGIT_ORTEC_GIT_SWITCH` full class source (public/private sections +
  implementation), to independently confirm the `mv_serial_prefetch_active` /
  `is_serial_prefetch_active` / `set_serial_prefetch_active` anchor shape the design
  proposes to mirror for the new `fdt0_cache` flag pair.
- `ZCL_ABAPGIT_ORTEC_SER_COST` (grep `FDT0`) -- confirms `c_default_ms_brf = 150000`
  citation.
- `CL_FDT_ADMIN_DATA` methods `if_fdt_transaction~save` and `notify_change` (full
  bodies) -- independently re-verified the AR-1-002 architectural proof.
- `zcl_abapgit_hash` method `sha1_string` (full body) -- independently re-verified
  the AR-1-003 fix.
- `FDT_ADMN_0000S` and `FDT_INC_KEY_0001` DDIC definitions -- independently
  re-verified the AR-1-004 evidence (no `MANDT` in the key).
- No productive code/DDIC/test edits made. Design artifact not modified. No
  `.memory/state.md` write performed (not permitted this cycle).

## Findings

### AR-1-001 -- BLOCKER (Cycle 1) -- STATUS THIS CYCLE: CLOSED, VERIFIED

```text
ID=AR-1-001
RECHECK_VERDICT=CLOSED_AND_FIXED (independently confirmed)
VERIFICATION=Independently re-read (not trusted from the design's own ledger) the
  exact anchors the fix depends on: ZCL_ABAPGIT_ORTEC_SER_ORCH=>serialize's ACTIVE
  body has exactly one set_serial_prefetch_active( abap_true ) near the top and
  exactly three set_serial_prefetch_active( abap_false ) calls on its three distinct
  exit paths (cx_uuid_error catch, normal completion after purge_run_state, and the
  zcx_abapgit_exception catch) -- the design's proposed set_fdt0_cache_active
  bracket is a line-for-line structural mirror of this real, currently-active
  pattern, not a hypothetical one. dispatch_batch's CALL FUNCTION EXPORTING list is
  built, and would read is_fdt0_cache_active( ) for its new
  iv_fdt0_cache_active parameter, entirely within the window between the ON call and
  the three OFF calls (dispatch_batch is only ever invoked from serialize()'s own
  TRY block, before wait_for_run_completion). Z_ABAPGIT_ORTEC_SER_BATCH's own body
  independently confirms the exact anchor lines the design cites (172: activation
  call before the DOKIL preload block; 285-287: the serialize() call site inside
  LOOP AT it_tadir; 311: the reset call immediately after ENDLOOP) and confirms the
  RFC-crossing rationale (processingType=rfc on the FM signature) -- this is a
  genuinely separate process from ORCH's dialog session, so the new OPTIONAL
  IMPORTING parameter is structurally necessary, not defensive over-engineering.
RESIDUAL_FINDING (new this cycle, MINOR, non-blocking) -- see AR-1-001-RESIDUAL below.
```

```text
ID=AR-1-001-RESIDUAL
SEVERITY=MINOR
CLAIM=Design section 5/§17 states the aRFC boundary-crossing fix is closed with "no
  residual risk identified".
COUNTEREXAMPLE=Z_ABAPGIT_ORTEC_SER_BATCH's per-object loop has exactly ONE
  `CATCH zcx_abapgit_exception INTO lx_error` scoped per iteration (confirmed by
  grep, line 295) -- any OTHER exception class (a raw ABAP runtime error/dump, e.g.
  TIME_OUT, or an uncaught non-zcx_abapgit_exception raised deep inside BRF+'s own
  export_xml_application/DOM-parse chain, or inside the new cache wrapper's own
  unguarded `EXPORT data = is_serialization TO DATA BUFFER lv_payload.` in `store()`,
  which has no TRY/CATCH in the design's pseudocode) escapes the loop entirely,
  skipping the line-311 `set_fdt0_cache_active( abap_false )` reset. This is not a
  new architectural flaw introduced by this design -- the identical exposure already
  exists for `set_serial_prefetch_active` today, in production, since SER-SLICE-3/5 --
  but FDT0 is, by the design's own evidence (`c_default_ms_brf = 150000`, "max single
  object 202 s"), the single most fragile, longest-running, timeout-prone object type
  in the entire batch loop, and this design adds new SQL/EXPORT/IMPORT surface area
  specifically inside that same fragile window. Whether the RFC server group
  ("DESTINATION IN GROUP") reuses the same ABAP internal session/work-process (and
  therefore the same CLASS-DATA) for a *subsequent, unrelated* aRFC dispatch after
  one call dumps mid-loop was not verified either way this cycle (out of
  EXACT_SOURCE_SCOPE -- it depends on RFC server group/session semantics, not any
  ORTEC object) -- if it does, a single dump on a slow FDT0 object could leak
  mv_fdt0_cache_active=TRUE into a completely unrelated later dispatch on the same
  reused process.
EVIDENCE=Live grep this session (Z_ABAPGIT_ORTEC_SER_BATCH, `set_serial_prefetch_
  active|RAISE |LOOP AT|ENDLOOP|CATCH |...`) -- exactly one CATCH inside the loop,
  no other exception boundary; design §4 `store()` pseudocode (no TRY/CATCH around
  EXPORT).
IMPACT=reliability/correctness, low probability (mirrors an already-accepted,
  long-production risk; not proven to actually cross calls; would require both a
  dump on an FDT0 object AND session/CLASS-DATA reuse across dispatches to manifest) --
  not blocking, but "no residual risk identified" in §17 overstates certainty.
REQUIRED_CHANGE=Non-blocking. Either (a) soften §17's AR-1-001 residual-risk claim
  from "none identified" to name this exposure explicitly and note it is inherited,
  symmetric with the existing set_serial_prefetch_active pattern, or (b) as
  defense-in-depth, wrap `store()`'s `EXPORT` in the same TRY/CATCH-and-swallow style
  already used elsewhere in that method. Neither is required for approval.
RETEST=Not required for APPROVE; if addressed, re-review confirms §17's language no
  longer claims zero residual risk without qualification.
```

### AR-1-002 -- BLOCKER (Cycle 1) -- STATUS THIS CYCLE: REOPENED AS AR-2-001 (MAJOR)

```text
ID=AR-1-002
RECHECK_VERDICT=ARCHITECTURAL PROOF INDEPENDENTLY CONFIRMED ACCURATE; ENFORCEMENT
  CLOSURE REJECTED -- see AR-2-001
VERIFICATION=Independently read (not taken from the design's quoted excerpts) the
  full bodies of CL_FDT_ADMIN_DATA's IF_FDT_TRANSACTION~SAVE and NOTIFY_CHANGE.
  Confirmed accurate: SAVE is a hard no-op unless has_unsaved_changes( ) = abap_true
  (`CHECK has_unsaved_changes( ) EQ abap_true.`), and when it proceeds it
  unconditionally sets change_timestamp (via load_buffer/set_buffer on the object's
  own mv_id row) before save_buffer_db( )/update_basic( ). NOTIFY_CHANGE
  unconditionally executes `GET TIME STAMP FIELD ls_buffer-change_timestamp.` as its
  very first data-mutating statement (not gated behind any object-type or
  content-class check), then manages ts_version and calls set_buffer( )/
  update_basic( ) unconditionally whenever invoked. Both bodies are single,
  non-type-dispatched implementations, consistent with the design's "one shared,
  non-subclassed class" claim. This part of the Cycle 1 fix is real, not a
  restated plausibility argument.
```

```text
ID=AR-2-001
SEVERITY=MAJOR
CLAIM=Design §6/§9/§17 close AR-1-002 by asserting the content-only-edit signature
  risk is resolved via architectural proof, with `AC-07 IT-01` "retained as a
  mandatory pre-implementation acceptance test" as defense-in-depth.
COUNTEREXAMPLE=The architectural proof (verified above) shows that *whenever*
  NOTIFY_CHANGE/SAVE is invoked, the admin row's timestamp changes -- but it does
  NOT show, and cannot show from CL_FDT_ADMIN_DATA/IF_FDT_CONSTANTS alone (the only
  two FDT-internal objects in this review's SOURCE_SCOPE), that every content-only
  setter for every FDT object type -- specifically a decision table's row/cell
  VALUES, as distinct from its structural/admin attributes -- actually calls
  NOTIFY_CHANGE before persisting. That call site lives in a decision-table-specific
  content class outside SOURCE_SCOPE, and neither this review nor the design's own
  Cycle 1 evidence table cites it. The design's own text acknowledges this gap
  exists ("IT-01... was not executed this cycle... it remains an explicit
  pre-implementation gate, not a design blocker") but that acknowledgment is not
  backed by an actual gate: §11 ("Stop conditions... must be resolved before
  implementation, not silently worked around") lists exactly three items for this
  design, and IT-01 is explicitly NOT one of them -- it was removed from the
  Cycle 0 stop-condition list as "resolved". §16 independently states
  "BLOCKING: 0... MAJOR: 0" with no mention of IT-01 as an outstanding gate. §13's
  AC-07 status is "ADDRESSED (planned, not yet written/run - design stage)" --
  "ADDRESSED" is the same status given to every other, fully-closed AC row in that
  same table, so a reader following the design's own status vocabulary would not
  recognize AC-07/IT-01 as still-open. As literally written, nothing in this design
  document would stop an implementer from building `ZCL_ABAPGIT_ORTEC_FDT0_CACHE`,
  activating it, and flipping `set_fdt0_cache_active` to `abap_true` in production
  without ever running IT-01 -- for a design whose own stated worst-case failure
  mode (a false cache hit silently returning stale BRF+ content) is explicitly
  called "the single most severe failure mode this design exists to prevent" in the
  original AR-1-002 finding. The MANDATORY_RECHECK instruction for this cycle offers
  two ways to close AR-1-002: prove the mechanism, OR "design correctly gates launch
  with a mandatory proof test" -- the mechanism is now well-argued (see above), but
  the gate does not actually exist in the design as written.
EVIDENCE=Design §6 ("IT-01 remains a mandatory pre-implementation acceptance test"),
  §9 IT-01 status text, §11 (three stop conditions listed, IT-01 absent), §13 AC-07
  row ("ADDRESSED (planned, not yet written/run)"), §16 ("BLOCKING: 0... Three
  stop-condition items remain" -- IT-01 not among them), §17 AR-1-002 closure ledger
  RESIDUAL_RISK paragraph (explicitly states IT-01 "was not executed this cycle").
IMPACT=process/correctness -- the specific residual risk class (false hit on a
  decision-table-content-only or rule-formula-only edit) remains genuinely unproven,
  and the design's own text could be read by an implementer as "this is done" (per
  the ADDRESSED/CLOSED vocabulary used everywhere else in the same document) rather
  than "this blocks activation until a specific live test passes".
REQUIRED_CHANGE=Before this design may be approved: either (a) execute IT-01's
  decision-table-cell-edit and rule-formula-edit variant on a real IT8 test
  application and record the actual measured signature-change evidence in §1 (the
  original AR-1-002 REQUIRED_CHANGE, still not done), which would fully close this
  without any wording change; or (b), if executing IT-01 is deferred to an
  implementation-phase task, add IT-01 explicitly to §11's Stop Conditions list
  (not just mentioned in §6/§9 prose) with wording that unambiguously blocks
  `set_fdt0_cache_active` from ever being set `abap_true` outside a test/dev session
  until IT-01 has passed, and change §13's AC-07 status from "ADDRESSED" to a status
  distinct from every fully-closed AC row (e.g. "ADDRESSED, GATED -- IT-01 required
  before production enablement") so the open obligation cannot be read as already
  satisfied.
RETEST=Re-review confirms either (a) a new §1 evidence row with actual before/after
  query results for both a decision-table cell edit and a rule formula edit, both
  showing a version/ch_timestamp change, or (b) §11 lists IT-01 as a literal stop
  condition gating `set_fdt0_cache_active( abap_true )` activation, and §13's AC-07
  status is textually distinguishable from a fully-closed AC.
```

### AR-1-003 -- MAJOR (Cycle 1) -- STATUS THIS CYCLE: CLOSED, VERIFIED

```text
ID=AR-1-003
RECHECK_VERDICT=CLOSED_AND_FIXED (independently confirmed)
VERIFICATION=Independently read the full body of zcl_abapgit_hash=>sha1_string this
  session: `IMPORTING iv_data TYPE string`, internally calls
  `cl_abap_message_digest=>calculate_hash_for_char( if_algorithm = 'SHA1' if_data =
  iv_data )`, and on `CATCH cx_abap_message_digest` calls
  `zcx_abapgit_exception=>raise_with_text( lx_error )` -- confirming both the
  parameter shape the design's fixed pseudocode now uses
  (`zcl_abapgit_hash=>sha1_string( lv_concat )`, no xstring conversion) and the
  RAISING zcx_abapgit_exception contract the design wraps in TRY/CATCH. The fixed
  compute_signature pseudocode will compile as written.
RESIDUAL_RISK=none.
```

### AR-1-004 -- MINOR (Cycle 1) -- STATUS THIS CYCLE: RE-VERIFIED, STILL OPEN AT OWNER DISCRETION

```text
ID=AR-1-004
RECHECK_VERDICT=Evidence independently re-confirmed accurate; not blocking.
VERIFICATION=Independently read FDT_ADMN_0000S (key = `include fdt_inc_key_0001`
  only, `@AbapCatalog.deliveryClass : #S`) and FDT_INC_KEY_0001 (`id : fdt_uuid`,
  no MANDT anywhere) this session -- matches the design's citation exactly.
STATUS=Still an open, explicitly non-blocking owner-discretion item per the
  design's own §11/§3 text (keep the defensive client key, or drop it for
  cross-client cache sharing) -- no new evidence changes this cycle.
```

## New-regression scan (mandatory, full revised design)

- `ZCL_ABAPGIT_ORTEC_SER_ORCH` `action=diff` (active -> inactive) was re-run fresh
  this cycle rather than trusted from Cycle 1's citation: the diff is
  byte-identical in *kind* to what Cycle 1 found (the same `dispatch_batch`
  EXPORTING-list column realignment plus the same two `#EC CI_HASHSEQ`
  comment-alignment tweaks in `mark_queued_failures`/`discard_run_state`) -- no
  drift, no new semantic difference, the design's STOP_IF (reconcile the draft
  before applying the `dispatch_batch`/`serialize` anchor edits) remains accurate
  and still necessary.
- `ZCL_ABAPGIT_ORTEC_GIT_SWITCH`'s live source independently confirms the exact
  anchor block the design proposes to mirror (`mv_serial_prefetch_active`
  CLASS-DATA at the end of the private-section list; `is_serial_prefetch_active`/
  `set_serial_prefetch_active` as a public method pair) -- no drift from what the
  design assumes.
- No other new correctness/persistence/performance regressions found in the
  Cycle 1 revision beyond AR-2-001 and the AR-1-001-RESIDUAL note above. The
  DDIC design (§3), hit/miss/corrupt semantics (§7), and lock/LUW/eviction
  reasoning (§8) are unchanged from Cycle 1 and were not newly contradicted by
  anything read this cycle.

## Invariant coverage this cycle

| Invariant | Status |
|---|---|
| FDT0-INV-01 (hit = output-parity) | AT RISK, narrower than Cycle 1 -- AR-2-001 (enforcement gap on the content-only-edit proof, not the proof itself) |
| FDT0-INV-02 (never degrades) | Holds -- AR-1-001 independently confirmed closed |
| FDT0-INV-03 (any graph change invalidates) | AT RISK, narrower than Cycle 1 -- AR-2-001 |
| FDT0-INV-04 (ORTEC-only) | Holds -- independently re-confirmed, no scope creep in the revised design |
| FDT0-INV-05 (bounded memory) | No new finding |
| FDT0-INV-06 (atomic publish) | No new finding |
| FDT0-INV-07 (concurrency) | Holds -- AR-1-001 independently confirmed closed |
| FDT0-INV-08 (local #L runtime-only) | Holds -- AR-1-004 re-confirmed, non-blocking |

## Acceptance criteria coverage this cycle

| AC | Status |
|---|---|
| AC-01 (DDIC) | No new finding |
| AC-02 (exact names/anchors) | Independently re-verified CLEAN this cycle (fresh diff + fresh anchor reads) |
| AC-03 (non-false-hit signature proof) | PARTIALLY BLOCKED -- AR-2-001 (mechanism proof holds, enforcement gate does not exist as written) |
| AC-04 (hit/miss/corrupt/concurrent) | Holds -- reachable per AR-1-001 closure; AR-1-001-RESIDUAL is a minor caveat, not a reachability blocker |
| AC-05 (parity/volatile XML) | No new finding (unchanged from Cycle 1 clean verdict) |
| AC-06 (LUW/locking/bounds) | No new finding |
| AC-07 (tests+IT8) | STILL design-stage; AR-2-001 requires either running IT-01 now or making it a literal stop condition |
| AC-08 (scope/non-goals/stop conditions) | AR-2-001 identifies that §11's stop-condition list is incomplete relative to §6/§9's own claims |

## Verdict (Cycle 2)

REVISE_AND_REVIEW_ONCE. Zero BLOCKERs remain. One MAJOR (AR-2-001) must be closed --
either by actually executing IT-01 with recorded evidence, or by making IT-01 a
literal, unambiguous §11 stop condition instead of prose buried in §6/§9 that §11/§16
do not reflect. AR-1-001-RESIDUAL and AR-1-004 are non-blocking and may be
addressed at the owner's/implementer's discretion. This is a design-text and
verification-sequencing gap, not a request for further design edits -- resolvable in
a single additional pass. One review cycle remains available (max 3).

---

# Cycle 3 (FINAL -- max cycles reached)

```text
CYCLE=3 of max 3 (FINAL)
DESIGN_ARTIFACT_AT_REVIEW=.memory/logs/fdt0_local_cache_design.md
  (TASK_ID=FDT0_LOCAL_CACHE_DESIGN_REVISION_20260812,
  STATUS=DESIGN_REVISED_CYCLE_2_AWAITING_REVIEW)
MANDATORY_RECHECK=AR-2-001: confirm §11/§13/§16 impose an unambiguous no-go until
  IT-01 proves a nested BRF+ content-only edit changes the signature and forces a
  cache re-export.
VERDICT=APPROVE_WITH_MINOR_REVISIONS
OPEN_BLOCKER=0
OPEN_MAJOR=0
CLOSED=AR-2-001 (MAJOR, independently re-verified this cycle)
OPEN_MINOR=2 (AR-1-004, AR-1-001-RESIDUAL -- both non-blocking, carried forward
  unchanged from Cycle 2, no new evidence required)
NEW_FINDING=AR-3-001 (MINOR, non-blocking -- design-internal wording residue, not a
  correctness or enforcement gap)
```

## Scope of verification performed this cycle

Per `SOURCE_SCOPE`, this cycle is a narrow recheck of `AR-2-001`'s closure, not a
broad re-discovery pass. No new live SAP reads were performed against
`Z_ABAPGIT_ORTEC_SER_BATCH`, `ZCL_ABAPGIT_OBJECT_FDT0`, `ZCL_ABAPGIT_ORTEC_SER_ORCH`,
`ZCL_ABAPGIT_ORTEC_GIT_SWITCH`, `CL_FDT_ADMIN_DATA`, `IF_FDT_CONSTANTS`, or
`ZCL_ABAPGIT_HASH` this cycle, because the design's own §17a ledger states the
`AR-2-001` fix is a text-only reconciliation of the design's internal stop-condition
vocabulary (no mechanism/code claim changed) -- consistent with the review's own
Cycle 2 finding, which was itself about documentation enforcement, not about
`CL_FDT_ADMIN_DATA`'s underlying mechanism (independently source-verified as sound
in Cycle 2 and not contradicted by anything in the Cycle 2 revision). The applicable
recheck is therefore a direct, independent re-read of the design's own §6, §9, §11,
§12, §13, §16, and §17a text as currently written -- not re-trusting the design's
`RECHECK_VERDICT`/`FIX` prose at face value. Also independently re-read the full
`.memory/state.md` binding-invariants section (already in `ALLOWED_CONTEXT`) to
confirm no cross-topic invariant newly conflicts with this design; none found.

## Findings

### AR-2-001 -- MAJOR (Cycle 2) -- STATUS THIS CYCLE: CLOSED, VERIFIED

```text
ID=AR-2-001
RECHECK_VERDICT=CLOSED_AND_FIXED (independently confirmed against the live design
  text, not the ledger's own summary of itself)
VERIFICATION=Independently re-read §11, §13, and §16 of the design as currently
  written (not the §17a closure-ledger's paraphrase of them):
  - §11's stop-condition list now opens with a bullet titled "AC-07 IT-01 (AR-2-001
    closure) -- literal, non-waivable stop condition, not prose," stating verbatim
    that `set_fdt0_cache_active( abap_true )` "MUST NOT be enabled in any production
    or live-repository run" and that "implementation/deployment of this cache CANNOT
    PROCEED TO PRODUCTION" until IT-01 has actually run on IT8 for both a
    decision-table cell-value edit and a rule formula-only edit, with both showing a
    measured signature change recorded as a new §1 evidence row. This is unambiguous
    imperative language, not a plausibility/defense-in-depth framing -- it directly
    satisfies the Cycle 2 REQUIRED_CHANGE option (b) verbatim.
  - The same section's closing note ("Resolved this cycle...AC-07 IT-01 is NOT
    resolved -- see the literal stop condition above") explicitly prevents a reader
    from inferring IT-01 was silently resolved alongside AR-1-002/AR-1-003, which was
    the exact misreading risk AR-2-001 identified.
  - §13's `FDT0-AC-07` row status reads "ADDRESSED, GATED" (not the bare "ADDRESSED"
    used by every fully-closed AC row in the same table), with inline text repeating
    the non-waivable production-block wording -- this is textually distinguishable
    from AC-01/02/03/04/05/06/08, closing the specific "reader cannot tell this row
    is still open" defect AR-2-001 raised.
  - §16 ("Open findings") explicitly states "AR-2-001 closed this cycle by making
    `AC-07 IT-01` a literal §11 stop condition" and separately lists, under "Four
    stop-condition items remain," `AC-07 IT-01` by name as "one of them a literal,
    non-waivable production gate" with the same MUST-NOT-enable wording repeated a
    third time. The `BLOCKING: 0 / MAJOR: 0` counts are therefore not standing alone
    (the Cycle 2 defect) -- they are immediately qualified by the still-open gate in
    the same section.
  - §6's and §9's IT-01 prose were independently re-read and now each end by
    pointing to §11 as the authoritative gate ("implementation/deployment of this
    cache CANNOT PROCEED TO PRODUCTION if IT-01 fails or is not executed... see
    §11") rather than restating a softer framing in isolation -- closing the Cycle 2
    concern that §6/§9 prose and §11's actual stop-condition list could drift apart.
  All four locations named in the `MANDATORY_RECHECK` instruction (§11, §13, §16, and
  by extension §6/§9 which §11 is now cross-referenced from) independently confirmed
  consistent and unambiguous. No location found that still uses the pre-Cycle-2
  "mandatory pre-implementation acceptance test"/"defense-in-depth" framing without
  the new non-waivable qualifier attached.
RESIDUAL_RISK=The gate is a documentation/process control (an implementer must read
  and honor §11), not a technical/runtime control (e.g. no code-level assertion or
  separate "production-unlock" flag independent of `set_fdt0_cache_active` itself
  gates activation). This is not a defect in AR-2-001's own closure -- the Cycle 2
  REQUIRED_CHANGE explicitly offered exactly this text-based remedy as option (b) and
  the design delivers it faithfully -- but it means the non-waivable status depends
  entirely on the implementer reading §11 before ever calling
  `set_fdt0_cache_active( abap_true )` outside a test session. Not required for
  approval; noted as a non-waivable *process* condition to carry forward into any
  implementation/handoff artifact verbatim, not merely a design-approval footnote.
```

### AR-3-001 -- MINOR (new this cycle, non-blocking)

```text
ID=AR-3-001
SEVERITY=MINOR
CLAIM=§16 and §12 present the design's invariant/acceptance state as fully current
  after the Cycle 2 revision.
COUNTEREXAMPLE=§1's evidence table still carries the literal token `UNVERIFIED` for
  "Whether `fdt_admn_0000s` is client-dependent," and §11's stop-condition list still
  lists this as an item to "verify via `DD03L` before finalizing" -- yet §17 (Cycle 1
  ledger, "Not part of `REQUIRED_CORRECTIONS`...") states in the design's own words
  that `AR-1-004` "was already closed with evidence by the review itself (confirmed
  client-independent via `FDT_INC_KEY_0001`)," and the review independently
  re-confirmed the identical evidence (`FDT_INC_KEY_0001`, `id` only, no `MANDT`) in
  both Cycle 1 and Cycle 2. The design's own §1/§11 text was never updated to reflect
  its own §17's claim of closure -- a reader consulting only §1/§11 (without also
  finding §17's aside) would reasonably conclude this question is still factually
  open, when the review has twice supplied a definitive answer (client-independent,
  safe either way) using DDIC evidence functionally equivalent to the `DD03L` check
  §11 asks for. This is the same substantive item as `AR-1-004` (already MINOR,
  already flagged non-blocking in both prior cycles) -- not a new mechanism risk --
  but the specific self-contradiction between §1/§11's "UNVERIFIED"/"stop condition"
  wording and §17's "already closed with evidence" wording had not been named as an
  internal inconsistency in either prior review cycle.
EVIDENCE=Design §1 (evidence-table row, token `UNVERIFIED`), §11 (stop-condition
  bullet, "was not confirmed from the rendered DDL this session... verify via
  `DD03L`"), §17 (Cycle 1 ledger aside, "already closed with evidence by the review
  itself"). Cross-referenced against this review's own Cycle 1 `AR-1-004` and Cycle 2
  `AR-1-004` re-verification entries above, both independently confirming the same
  `FDT_INC_KEY_0001` evidence.
IMPACT=documentation clarity only -- `AR-1-004`'s substance was already rated MINOR
  and explicitly non-blocking/owner-discretion in both prior cycles; this finding
  does not change that rating or reopen any correctness question. No implementation
  behavior depends on which of §1/§11 vs. §17's framing a reader trusts, since both
  agree the current DDIC design (`client` key retained, §3) is a safe, valid choice
  either way.
REQUIRED_CHANGE=Non-blocking, owner/implementer discretion, no retest required for
  approval: if the design is revised for any other reason in the future, reconcile
  §1's `UNVERIFIED` token and §11's "verify via `DD03L`" bullet to match §17's own
  "already closed with evidence" statement (either update §1 to `CONFIRMED` with the
  `FDT_INC_KEY_0001` citation already present in §17, or remove the redundant §11
  bullet), so a reader does not have to cross-reference the closure ledger to learn
  the question was already answered.
RETEST=Not required for approval. If addressed, re-review confirms §1/§11 use the
  same closure language as §17 for this item.
```

## New-regression scan (mandatory, full revised design, Cycle 3)

- No source-code or DDIC anchors changed between the design text reviewed in Cycle 2
  and the design text reviewed this cycle -- per the design's own §17a, the only
  edits were to §6 (closing paragraph), §9 (IT-01 status text), §11 (new bullet +
  resolved-list note), §13 (AC-07 row), and §16 (open-findings summary), all
  text-only. Independently re-read all five sections this cycle (not just the
  §17a paraphrase) and confirmed no anchor text, SQL shape, DDIC field list,
  invariant mechanism, or test-plan item silently changed alongside the wording
  fix -- the Cycle 2 architecture (signature algorithm, DDIC, hit/miss/corrupt
  semantics, lock/LUW/eviction reasoning, activation-wiring bracket shapes) is
  byte-identical in substance to what Cycle 2 already independently verified.
- `.memory/state.md` binding invariants re-read in full this cycle: no entry
  added or changed since Cycle 2 that bears on this design (the active-topic
  block remains `OBJ_PERF_FINAL`, an unrelated topic; the binding-invariants list
  at the bottom of `state.md` contains no FDT0/BRF+-specific entry, confirming
  this is genuinely a new, independent design with no prior committed state to
  contradict).
- No new BLOCKER or MAJOR identified this cycle beyond the AR-2-001 recheck itself.

## Invariant coverage (Cycle 3, full re-evaluation)

| Invariant | Status |
|---|---|
| FDT0-INV-01 (hit = output-parity) | HOLDS structurally (§6/§10); production enablement remains gated on IT-01 per §11 (documentation-level gate, AR-2-001 closure) |
| FDT0-INV-02 (never degrades) | HOLDS -- AR-1-001 independently re-confirmed closed, no new contradiction |
| FDT0-INV-03 (any graph change invalidates) | HOLDS structurally via CL_FDT_ADMIN_DATA's shared save/versioning layer (§6); same IT-01 production gate as INV-01 |
| FDT0-INV-04 (ORTEC-only changes) | HOLDS -- no scope creep found this cycle; edit list unchanged from Cycle 2 |
| FDT0-INV-05 (bounded payload/memory) | HOLDS -- no new finding any cycle |
| FDT0-INV-06 (atomic publication) | HOLDS -- no new finding any cycle |
| FDT0-INV-07 (concurrency safety) | HOLDS -- content-addressing argument unchanged and unchallenged this cycle |
| FDT0-INV-08 (local #L runtime-only) | HOLDS -- AR-1-004/AR-3-001 note the client-key wording residue, non-blocking |

## Acceptance criteria coverage (Cycle 3, full re-evaluation)

| AC | Status |
|---|---|
| AC-01 (DDIC) | ADDRESSED -- no new finding |
| AC-02 (exact names/anchors) | ADDRESSED -- independently verified clean in Cycle 2, not recontacted this cycle (no anchor text changed) |
| AC-03 (non-false-hit signature proof) | ADDRESSED -- mechanism proof independently re-confirmed sound; not itself the subject of the IT-01 gate (AC-07 carries that) |
| AC-04 (hit/miss/corrupt/concurrent) | ADDRESSED -- reachable per AR-1-001 closure; AR-1-001-RESIDUAL remains a named, non-blocking caveat |
| AC-05 (parity/volatile XML) | ADDRESSED -- no new finding |
| AC-06 (LUW/locking/bounds) | ADDRESSED -- no new finding |
| AC-07 (tests + IT8) | ADDRESSED, GATED -- confirmed textually distinguishable from every closed AC row this cycle (AR-2-001 closure); IT-01 has still not been executed (design-only session, no productive/test data changes permitted) and remains the one non-waivable pre-production condition |
| AC-08 (scope/non-goals/stop conditions) | ADDRESSED -- AR-3-001 (MINOR) notes a wording residue in §1/§11 vs. §17 for the already-non-blocking client-key question; does not reopen AC-08 |

## Verdict (Cycle 3, FINAL)

APPROVE_WITH_MINOR_REVISIONS. Zero open BLOCKER, zero open MAJOR -- `AR-2-001` is
independently re-verified closed against the live design text (not merely its own
closure-ledger paraphrase): §11 now contains an unambiguous, literal, non-waivable
stop condition blocking `set_fdt0_cache_active( abap_true )` in any non-test
environment until `AC-07 IT-01` passes; §13's `AC-07` row is textually
distinguishable from every closed AC row; §16 restates the same gate a third time
rather than letting a bare `BLOCKING:0/MAJOR:0` count imply the design is fully
clear. This satisfies the `MANDATORY_RECHECK` instruction for this cycle in full.

This is the final cycle (3 of max 3); per `GATE_RULE`, remaining open items are
recorded here as non-waivable conditions rather than deferred to a further review
cycle:

- **Non-waivable, literal, pre-production (from the design's own §11, reaffirmed
  by this review, not a new condition invented here):** `set_fdt0_cache_active(
  abap_true )` must not be enabled in any production or live-repository run, and
  implementation/deployment of this cache must not proceed to production, until
  `AC-07 IT-01` has actually been executed on IT8 -- both a decision-table
  cell-value-only edit and a rule formula-only edit -- with both runs recording a
  measured signature change as a new design §1 evidence row.
- **Non-waivable, pre-implementation (from the design's own §11):** the unactivated
  whitespace-only draft on `ZCL_ABAPGIT_ORTEC_SER_ORCH` must be reconciled
  (activated or discarded) before the `route_to_sequential_fallback`,
  `serialize()`, and `dispatch_batch` anchor edits are applied, so exact anchor
  text matches at implementation time.
- **Non-waivable, carry-forward caveat (from the design's own §6/§11):** the
  cross-application BRF+ reference assumption (no evidence found either way) must
  remain an explicit, visible caveat in any implementation handoff artifact.
- **Non-blocking, owner/implementer discretion (`AR-1-004`, re-confirmed, and
  `AR-3-001`, new this cycle):** whether `ZAOG_FDT_CACHE` keeps `client` in its
  primary key (current design choice, §3) or drops it to widen cross-client cache
  sharing is confirmed safe either way; if the design is touched again for any
  other reason, reconcile §1/§11's stale "UNVERIFIED"/"verify via DD03L" wording to
  match §17's own "already closed with evidence" statement.
- **Non-blocking, inherited, named explicitly (`AR-1-001-RESIDUAL`, re-confirmed):**
  an uncaught non-`zcx_abapgit_exception` inside `Z_ABAPGIT_ORTEC_SER_BATCH`'s
  per-object loop (or inside `store()`'s own unguarded `EXPORT`) could skip the
  line-311-equivalent `set_fdt0_cache_active( abap_false )` reset; symmetric with
  the already-accepted, already-in-production `set_serial_prefetch_active`
  exposure; not required to be fixed for approval.

No further review cycle is available or required. This design is APPROVED for
implementation, subject to the non-waivable conditions above being carried forward
verbatim into the implementation and IT8 handoff artifacts.
