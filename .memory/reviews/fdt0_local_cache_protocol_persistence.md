# FDT0 Local Runtime Cache -- Protocol/Persistence Design Gate

```text
TASK_ID=FDT0_LOCAL_CACHE_PROTOCOL_GATE_20260813
REVIEW_MODE=protocol/persistence design gate (independent of the general
  adversarial-review track)
BASELINE_COMMIT=b4f41e38372a0fe9f67483f71e968b1885b594c1
DESIGN_ARTIFACT_REVIEWED=.memory/logs/fdt0_local_cache_design.md
  (TASK_ID=FDT0_LOCAL_CACHE_DESIGN_REVISION_20260812,
  STATUS=DESIGN_REVISED_CYCLE_2_AWAITING_REVIEW, all 17/17a sections read)
GENERAL_ADVERSARIAL_REVIEW_INPUT=.memory/reviews/fdt0_local_cache_adversarial_review.md
  (Cycles 1-3, FINAL VERDICT=APPROVE_WITH_MINOR_REVISIONS, 0 open
  blocker/major, 2 open minor + 1 new minor this-gate-relevant item carried
  forward: AR-1-004, AR-1-001-RESIDUAL, AR-3-001)
SCOPE=Read-only gate. No design/state/diagram edits made. No DDIC/code/test
  changes made. Output written to exactly the one path specified.
VERDICT=APPROVE_WITH_MINOR_REVISIONS
OPEN_BLOCKER=0
OPEN_MAJOR=0
OPEN_MINOR=3 (PP-01, PP-02, PP-03 -- all non-blocking, all carried/derived
  from the already-recorded AR-1-004/AR-1-001-RESIDUAL/AR-3-001 findings,
  reframed here strictly through a protocol/persistence lens)
NON_WAIVABLE_CARRY_FORWARD=AC-07 IT-01 (design section 11): a live,
  measured signature-change proof on a real decision-table cell-value edit
  and a real rule formula-only edit, on IT8, is required before
  `set_fdt0_cache_active( abap_true )` may ever be enabled outside the
  IT-01 test/dev session itself. This gate does not waive, soften, or
  re-litigate IT-01 -- it is reaffirmed verbatim as a pre-production
  condition, per instruction.
```

## 0. Verification performed this gate (independent, not trusted from the
   design's or the adversarial review's own closure ledgers)

Live reads this session, restricted to `SOURCE_SCOPE` and directly relevant
DDIC:

- `FDT_ADMN_0000S` (DDIC): key = `include fdt_inc_key_0001 not null` only;
  `@AbapCatalog.deliveryClass : #S`, `@AbapCatalog.dataMaintenance : #RESTRICTED`.
- `FDT_INC_KEY_0001` (DDIC): single component `id : fdt_uuid` -- no `MANDT`
  anywhere in the key or the structure.
- `ZCL_ABAPGIT_ORTEC_OBJ_STORE=>store_object` (full method body): plain
  `MODIFY zaog_obj_store FROM ls_row.` with an explicit `IF sy-subrc <> 0.
  zcx_abapgit_ortec_git=>raise(...)`. No `COMMIT WORK`/`ENQUEUE` anywhere in
  the method.
- `ZCL_ABAPGIT_HASH=>sha1_string` (full method body): `IMPORTING iv_data
  TYPE string`, calls `cl_abap_message_digest=>calculate_hash_for_char(
  if_algorithm = 'SHA1' ... )`, `CATCH cx_abap_message_digest` ->
  `zcx_abapgit_exception=>raise_with_text`, `TRANSLATE rv_sha1 TO LOWER
  CASE.` before return.
- `Z_ABAPGIT_ORTEC_SER_BATCH` (full signature via `includeSignature=true`,
  confirms `"processingType": "rfc"`) plus a targeted regex grep across the
  whole FM body for `set_serial_prefetch_active|serialize\(|CATCH
  |LOOP AT|ENDLOOP|MODIFY |COMMIT WORK|ROLLBACK WORK`: confirms line 172
  (`set_serial_prefetch_active( abap_true )` immediately before the DOKIL
  preload block), line 185 (`LOOP AT it_tadir`), lines 285-287 (the
  `zcl_abapgit_objects=>serialize(...)` call site), line 295 (the single
  `CATCH zcx_abapgit_exception INTO lx_error` inside the loop), line 309
  (`ENDLOOP`), line 311 (`set_serial_prefetch_active( abap_false )` reset).
  Zero `MODIFY`/`COMMIT WORK`/`ROLLBACK WORK` statements exist anywhere in
  this FM today -- it currently performs no persistence of its own; the new
  cache would be the FM's first persistence side effect.

All of the above are exact, independent matches for what both the design
(section 1/4/6) and the general adversarial review (Cycles 1-3) already
cited -- no drift, no contradiction found. This gate therefore does not
reopen AR-1-001/AR-1-002/AR-1-003/AR-2-001 (all independently re-confirmed
CLOSED by three prior review cycles); it evaluates the same design strictly
through the protocol/persistence lens listed in `REVIEW_FOCUS`.

## 1. Table key / DDIC identity / client semantics

- **Key shape**: `ZAOG_FDT_CACHE` primary key = `client (MANDT) +
  application_id (char32) + signature (char40)`. This is a
  content-addressed key (signature is a hash over the owned-object graph),
  matching the same identity discipline already used by
  `ZCL_ABAPGIT_ORTEC_OBJ_STORE` (SHA1-keyed, branch/repo-independent object
  reuse) -- **repository/run identity is correctly excluded from the key**
  (no `repo_key` column), which is the right call: `export_xml_application`
  is proven (design section 2/3, independently plausible from the FDT
  admin-table schema read here) to be a pure function of
  `(application_id, signature)`, so keying on anything narrower than that
  would only fragment cache identity without adding correctness, exactly
  the anti-pattern this review mode is chartered to flag ("do not include
  branch in the physical object-store key").
- **Client key**: independently confirmed via `FDT_INC_KEY_0001` that
  `fdt_admn_0000s` (the sole source of every signature input) is
  genuinely client-independent (`id : fdt_uuid`, no `MANDT`). The new
  table's `client` key component is therefore provably a **defensive,
  non-load-bearing dimension** -- safe, but a real, measurable
  hit-rate cost: every client on this system independently populates and
  warms its own copy of what is architecturally identical cache content.
  This does not violate `FDT0-INV-08` (still local, still `#L`) and is not
  a correctness defect -- it is a persistence-design efficiency question
  already raised as `AR-1-004`/`AR-3-001` in the general review and left
  to owner/implementer discretion. Reframed here as **PP-01 (MINOR,
  non-blocking)**: from a pure content-addressing standpoint, `client`
  should not be a *key* component of a store keyed by data proven
  client-independent -- if kept, it must be documented as intentional
  headroom (matching `ZAOG_OBJ_STORE`/`ZAOG_OBJ_INDEX`'s own client-scoped
  convention for a different reason: those tables key content that IS
  genuinely client/repo-scoped in general, so their `client` key is
  load-bearing there, not merely conventional).
- **DDIC identity vs. buffer-table convention**: `obj_name` is
  intentionally non-key (denormalized, for inspection only) -- correct;
  never used for lookup. `payload_size`/`created_at`/`last_used_at` are
  non-key housekeeping fields, consistent with `ZAOG_OBJ_STORE`'s own
  shape. No objection.

## 2. Local `#L` / transport behavior

- `#L` (temporary-data delivery class) + `#RESTRICTED` data maintenance is
  the correct choice to guarantee table **content** is never transported
  and never client-copied, matching the owner's verbatim "never
  transported or shared cross-system" requirement -- this is a stronger
  guarantee than `ZAOG_OBJ_STORE`'s own `#L`/`#ALLOWED` combination (which
  only restricts transport of content, not who may maintain it via
  SM30/SE16). The **table definition itself** (the DDL/TADIR object) still
  requires one normal workbench transport to create it on any downstream
  system -- the design does not conflate these two concerns (§3 already
  scopes the `#L`/`#RESTRICTED` claim to content only), so no finding here.
- The design's own §3 `STOP_IF` ("activation reports the delivery-class/
  data-maintenance combination is rejected") remains the correct, and
  only necessary, gate for this specific risk -- it cannot be resolved by
  design review alone (requires a real `SAPActivate` against the DDIC
  dictionary compiler) and is properly deferred to implementation, not a
  finding against the design.
- No new transport-boundary risk found: `store()`/`try_read()` contain no
  transport-request logic, no `SAPTransport`-adjacent calls, and no path
  by which a cache row could be pulled into a workbench request (the table
  itself, once created, carries no per-row change-recording since content
  changes to an `#L` table are not workbench-recorded).

## 3. Signature canonicalization and full-graph invalidation

- **Canonicalization scheme**: `id;object_type;version;ch_timestamp;
  deleted;tv_state;tv_timestamp;obsolete\n`, one line per row, rows
  ordered `ORDER BY id` (a UUID, unique per admin row) before concatenation
  and hashing via `sha1_string`. This is deterministic and
  order-stable -- the same underlying row set always canonicalizes to the
  same byte string regardless of DB physical row order, which is the
  correct property for a signature meant to be reproducible across
  workers/processes (`FDT0-INV-07`).
- **Delimiter-safety (new observation, not previously raised)**: the
  concatenation uses literal `;` and `\n` as field/row separators with no
  length-prefixing or escaping of field values. This is a real protocol
  hygiene gap in the abstract (a value containing the separator could
  cause two structurally different row sets to canonicalize identically),
  but the actual fields hashed are all fixed-format DDIC types local to
  `FDT_INC_ADMN_0000_DATA`/`FDT_INC_QUERY_0000_DATA`
  (`id`=UUID, `object_type`=fixed short code, `version`=numeric/counter,
  `ch_timestamp`/`tv_timestamp`=timestamp, `deleted`/`obsolete`=single-char
  flags, `tv_state`=fixed short code) -- none of these DDIC domains permit
  free-text content that could embed `;` or a newline. **PP-02 (MINOR,
  non-blocking)**: document this closed-domain assumption explicitly in
  design §6 (one sentence: "canonicalization safety depends on every
  hashed field being a fixed-format/bounded DDIC domain, not free text; if
  any future revision of this signature adds a free-text field, a
  delimiter-safe encoding — e.g. length-prefixing or a stronger structural
  hash — must be used instead of raw string concatenation"). Not required
  for approval; purely a documentation hardening note, since the current
  field list contains no free-text member and no code change is implied.
- **Full-graph invalidation, protocol framing**: the "commit graph
  materialization" analogue here is the `SELECT ... WHERE application_id =
  ... ORDER BY id` result set -- this is unbounded (no `UP TO n ROWS`) but
  correctly bounded in practice by `c_max_signature_rows` (200000, a
  defensive ceiling causing cache-skip rather than a hard failure) and by
  the real per-application cardinality ceiling measured in the design
  (max 5179 rows observed). This is the right shape: a single scoped
  `SELECT`, not a per-object-in-the-graph N+1 loop, and not a
  system-wide/cross-application scan -- satisfies the "row and byte
  batching" and "expected incremental scaling with K vs. N" concerns for
  this mode: signature cost scales with *this one application's own graph
  size*, never with total repository size or total system-wide
  application count.
- **ADD/UPDATE/DELETE proofs**: independently re-derivable from the row-set
  membership + per-row versioning argument already established at the
  `CL_FDT_ADMIN_DATA` source-code level by the general adversarial review
  (Cycle 2 AR-1-002/AR-2-001 closure, out of this gate's `SOURCE_SCOPE` to
  re-read directly, but the mechanism — shared, non-subclassed
  save/versioning layer — is exactly the right kind of evidence for a
  full-graph content-addressing claim: it is a source-level invariant, not
  an empirical correlation). This gate defers to that closure and does not
  re-litigate it, per instruction that IT-01 (the live proof) remains the
  carried-forward non-waivable condition, not a fresh design defect.

## 4. Payload integrity

- Wire format = the same `EXPORT ... TO DATA BUFFER` container already
  used, unmodified, for the existing aRFC batch-result transport in
  `Z_ABAPGIT_ORTEC_SER_BATCH` today (per the design's own §3 field notes)
  -- reusing an already-proven-compatible container is the correct choice
  over inventing a second, parallel serialization format for the same
  data.
- **Integrity checking is delegated entirely to `IMPORT`'s own
  format/mismatch/compression/codepage exception set** (`cx_sy_import_
  format_error`, `cx_sy_import_mismatch_error`, `cx_sy_compression_error`,
  `cx_sy_conversion_codepage`) -- there is no independent checksum/CRC of
  the stored `payload` column itself. This is an acceptable, proportionate
  choice for a same-system, same-release, local-only cache (no
  cross-system transfer, no untrusted producer) — a stronger integrity
  check (e.g. storing `sha1_string` of the payload alongside it) would add
  protection against silent storage-media bit-rot, which is a DB-layer
  concern already handled by the platform, not an application-layer
  concern this cache needs to duplicate. No finding.
- **Self-heal-on-corruption is correctly scoped to the read path only**:
  a corrupt or zero-file row is deleted by `try_read` at read time, never
  proactively scanned/repaired in bulk — matches the "presence versus
  payload access" discipline this mode requires (no unnecessary eager
  full-table integrity sweep; corruption is detected and fixed exactly
  once, on the one read that would have been harmed by it).
- **Structural forward-compatibility**: if `zif_abapgit_objects=>ty_
  serialization`'s shape ever changes in a future standard-abapGit
  upgrade, old rows become unreadable via the same `IMPORT` mismatch path
  already handled above (self-heal delete + miss) — no separate schema-
  version column is needed because the failure mode is already covered by
  the existing corrupt-row handling, and a local `#L` cache has no
  cross-system/cross-release compatibility obligation to begin with.

## 5. Locks / LUW / publication / rollback

- **No `ENQUEUE`**: correct call given the content-addressed key design
  (§3/§8 of the design) — this mirrors the git-object-store precedent
  this review mode is chartered on: content-addressed writes need no
  mutual-exclusion lock because two writers for the same key always
  produce byte-identical payloads by construction. Adding a lock here
  would add latency (and, worse, real deadlock/timeout exposure across two
  independent aRFC-vs-dialog processes) for zero correctness benefit.
- **Publication boundary**: a row becomes visible only after the full
  `EXPORT` + size-check + single-statement `MODIFY` completes — there is
  no earlier partial/placeholder write, so no reader can ever observe a
  half-written row. This satisfies `FDT0-INV-06` without a two-phase
  status column, and is the correct minimal mechanism (status columns are
  needed only when a write spans more than one physically-committed
  statement or more than one row — neither is true here).
- **Transaction/commit boundary**: `store()`'s `MODIFY` durability rides
  the natural DB-LUW boundary of whichever process calls it — aRFC
  function-module return (batch worker) or the dialog step's own commit
  (`route_to_sequential_fallback`), with **no explicit `COMMIT WORK`**
  introduced. This is independently confirmed as the established,
  already-production-proven idiom on `ZCL_ABAPGIT_ORTEC_OBJ_STORE=>
  store_object` (verified above: plain `MODIFY`, no `COMMIT WORK`
  anywhere in that method) — the new design does not need to (and
  correctly does not) invent a different persistence discipline for a
  strictly local, lower-stakes cache table.
- **Rollback**: an unrelated later rollback in the calling process rolls
  the cache `MODIFY` back with it — standard ABAP DB-LUW semantics,
  correctly treated by the design as "indistinguishable from a miss that
  was never stored," which is the right framing (no special-cased
  recovery/replay logic is needed for a cache, unlike for `ZAOG_OBJ_STORE`
  where a lost write would break git-protocol correctness rather than
  merely degrade performance).
- **Divergence from `store_object`'s raise-on-failure**: the new `store()`
  deliberately swallows `MODIFY` failure instead of raising, which is the
  correct, and already explicitly justified (design §4), inversion of the
  `ZCL_ABAPGIT_ORTEC_OBJ_STORE` idiom — a failed *cache* write must never
  turn an already-successful `serialize()` result into a reported failure
  (`FDT0-INV-02`), whereas a failed *git object* write there is genuinely
  fatal to protocol correctness. No finding; this is the right place for
  the two idioms to differ.

## 6. Concurrent aRFC reads/writes

- **Cross-process key sharing**: both interception anchors (aRFC batch
  worker, dialog-process sequential fallback) route through the same
  `ZCL_ABAPGIT_ORTEC_FDT0_CACHE` class against the same table — correctly
  satisfies `FDT0-INV-07`'s requirement that a batch-path write be visible
  to a sequential-fallback-path read and vice versa within one run,
  independently confirmed reachable now that `AR-1-001`'s aRFC
  boundary-crossing flag fix is in place (verified above: the FM's own
  live `processingType: rfc` signature and the exact line-172/285-287/311
  anchor text the fix depends on).
- **Two workers, same key**: harmless by construction (content-addressed,
  §5) — last `MODIFY` wins, both payloads are byte-identical.
- **Two workers, different signatures for the same `application_id`**
  (a live mid-export edit race): each writes its own row under its own
  key; correctly argued as "cannot cause a false hit, at worst an orphan
  cache slot" — this is the right asymmetry for a content-addressed
  design and is analogous to the git-protocol principle that a
  torn/inconsistent read can only ever under-share (an extra miss), never
  over-share (a false hit) across independent fetch attempts.
- **`AR-1-001-RESIDUAL`, reframed through this mode's lens (PP-03, MINOR,
  non-blocking, carried forward unchanged in substance)**: the aRFC batch
  worker's per-object loop has exactly one `CATCH zcx_abapgit_exception`
  (independently confirmed at line 295, this session) — any other
  exception class escaping mid-loop (a raw dump, or an uncaught exception
  from the new cache wrapper's own unguarded `EXPORT` in `store()`, which
  has no `TRY`/`CATCH` in the design's pseudocode) skips the line-311-
  equivalent `set_fdt0_cache_active( abap_false )` reset. Whether this can
  leak `TRUE` into a *subsequent, unrelated* aRFC dispatch depends on RFC
  server-group work-process reuse semantics, which are outside
  `SOURCE_SCOPE` and were not re-verified this gate. This is an inherited,
  already-accepted exposure (symmetric with the existing, already-in-
  production `set_serial_prefetch_active` pattern) — not a new
  persistence defect, and not blocking, consistent with the general
  review's own non-blocking disposition of the identical finding.

## 7. Cache corruption and eviction

- **Corruption handling** (see §4 above): self-heals on the read path via
  `DELETE` + report-as-miss, for both `IMPORT`-exception rows and
  zero-file rows. No dangling "corrupt" state can persist across reads —
  each corrupt row is fixed exactly once, the first time it is touched.
- **Eviction**: none implemented in this slice beyond the per-row
  `c_max_cache_payload_bytes` write-time cap. Row-count growth is bounded
  by a real, external, system-wide ceiling (307 `AP` rows on IT8 today) —
  not by anything this design must actively police — so deferring active
  eviction tooling to a later `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` slice (out
  of `EXACT_SOURCE_SCOPE`/out of this gate's `SOURCE_SCOPE`) is the
  correct, proportionate scope decision, not a persistence gap. No
  finding.
- **Stale-but-valid rows**: a row for an `application_id` that is later
  deleted entirely from `fdt_admn_0000s` (or whose owning object is
  removed from every repository's package scope) is never actively
  purged by this design — it becomes a permanently orphaned, harmless row
  (never matched by any future signature computation, since the `id`
  would no longer appear in any live row set). This is consistent with
  the "harmless orphan, not a wrong answer" framing already applied
  elsewhere in the design (§6) and requires no additional handling for a
  local, small, bounded-cardinality cache.

## 8. Error / fallback behavior

- Every failure mode enumerated in the design's §7 table (unresolved
  `application_id`, empty/oversized signature graph, `sha1_string`
  exception, cache-row `IMPORT` failure, zero-file cache row, `store()`
  `MODIFY` failure) degrades to "call the real serializer / swallow and
  proceed," never to a new exception type the caller must additionally
  handle — this is the correct fallback discipline for a transparent
  cache wrapper (`FDT0-INV-02`) and was independently spot-checked against
  the one thing this gate could verify directly: `sha1_string`'s real
  `RAISING zcx_abapgit_exception` contract (confirmed above), which the
  design's `compute_signature` pseudocode correctly wraps in `TRY`/`CATCH`
  rather than propagating.
- The one gap already named by the general review (`AR-1-001-RESIDUAL`,
  §6/PP-03 above) is the sole place where an *unhandled* exception class
  could bypass the fallback discipline for the flag's own reset —
  everything else in the error/fallback matrix is exception-class-
  exhaustive by construction (either a `TRY`/`CATCH` with a named,
  bounded exception list, or a `sy-subrc` check with no exception
  possible).

## 9. No cache data memory leak

- **Per-call memory shape**: `store()`/`try_read()` each hold at most one
  transient `payload`/`ty_serialization` value at a time, sized against
  `c_max_cache_payload_bytes` (50 MB) before being persisted or discarded
  — no growing internal table, no cross-iteration accumulation across the
  `Z_ABAPGIT_ORTEC_SER_BATCH` per-object loop (each loop iteration's cache
  interaction is fully self-contained; nothing from one object's cache
  read/write is retained for the next). This matches the "peak memory
  model" requirement for this mode (no unbounded recovery/materialization
  without an explicit memory gate) — the memory gate here is
  `c_max_cache_payload_bytes` itself, checked before any row-level write.
- **No held handles/locks across calls**: no `ENQUEUE`, no static/session
  buffer of retrieved payloads, no class-level cache-of-the-cache — every
  `try_read`/`store` call is a fresh, independent DB round-trip. Confirmed
  consistent with `ZCL_ABAPGIT_ORTEC_OBJ_STORE`'s own precedent read this
  session, which does retain one static full-cache-repo-key marker
  (`CLEAR mv_full_cache_repo_key.` after `store_object`) for a *different*
  purpose (repo-scoped read-cache invalidation) that has no analogue
  needed here, since this design has no read-side in-memory cache layer
  of its own to invalidate.

## Invariant coverage (protocol/persistence lens only)

| Invariant | Status |
|---|---|
| FDT0-INV-01 (hit = output-parity) | Mechanism sound (payload = verbatim real output); production enablement remains gated on IT-01 (unchanged, non-waivable, not reopened here) |
| FDT0-INV-02 (never degrades) | HOLDS -- every persistence-layer failure path degrades to real-serializer fallback or silent swallow, independently spot-checked |
| FDT0-INV-03 (any graph change invalidates) | HOLDS structurally (canonicalization + row-set membership argument); IT-01 remains the live-proof gate, unchanged |
| FDT0-INV-04 (ORTEC-only changes) | Not this mode's primary focus; no persistence-layer scope creep found (new table + new class + additive flag pair only) |
| FDT0-INV-05 (bounded payload/memory) | HOLDS -- `c_max_cache_payload_bytes`/`c_max_signature_rows` caps confirmed correctly scoped; no accumulation found (see §9) |
| FDT0-INV-06 (atomic publication) | HOLDS -- single-statement `MODIFY`, no partial-write window (see §5) |
| FDT0-INV-07 (concurrency safety) | HOLDS -- content-addressed key makes concurrent writers safe without locking; both interception anchors independently confirmed reachable (see §6) |
| FDT0-INV-08 (local #L runtime-only) | HOLDS -- `#L`/`#RESTRICTED` correctly scoped to content-transport only; client-key is safe-but-non-load-bearing (PP-01) |

## Acceptance criteria coverage (protocol/persistence lens only)

| AC | Status |
|---|---|
| AC-01 (DDIC) | ADDRESSED -- key/field design independently verified sound; activation-rejection risk correctly deferred to implementation (design's own STOP_IF) |
| AC-03 (signature + proof) | ADDRESSED for canonicalization mechanics (PP-02 documentation note only); underlying content-vs-metadata proof out of this gate's re-verification scope, already closed by the general review |
| AC-04 (hit/miss/corrupt/concurrent/error) | ADDRESSED -- fully exception-exhaustive except the named, inherited PP-03/AR-1-001-RESIDUAL gap |
| AC-06 (LUW/locking/bounds) | ADDRESSED -- no locking needed, correct commit-boundary reuse, correct absence of two-phase publication |
| AC-07 (tests + IT8) | ADDRESSED, GATED -- IT-01 reaffirmed verbatim as the literal, non-waivable pre-production condition; not reopened, not softened, not closed by this gate |
| AC-08 (scope/non-goals/stop conditions) | ADDRESSED -- eviction/admin-tooling deferral correctly scoped; no new stop condition required |

## Findings summary

```text
BLOCKING=0
MAJOR=0
MINOR=3
  PP-01: client key in ZAOG_FDT_CACHE is safe but non-load-bearing given
    fdt_admn_0000s's confirmed client-independence -- owner/implementer
    discretion (restates AR-1-004/AR-3-001 through the protocol-identity
    lens; no new evidence, no new risk).
  PP-02: signature canonicalization (`;`/`\n`-delimited concatenation) has
    no delimiter-escaping, safe today only because every hashed field is a
    fixed-format/bounded DDIC domain -- recommend one documentation
    sentence in design section 6 stating this assumption explicitly so a
    future field addition does not silently reintroduce delimiter risk.
  PP-03: store()'s own EXPORT is unguarded and the aRFC batch loop has a
    single, narrow CATCH -- restates AR-1-001-RESIDUAL through the
    concurrent-aRFC lens; inherited, symmetric with an already-accepted
    production exposure, not required to fix for approval.
NON_WAIVABLE_CARRIED_FORWARD=AC-07 IT-01 (unchanged, not reopened, not
  closed by this gate -- literal pre-production condition per design
  section 11 and general-review Cycle 3 verdict).
```

## Verdict

APPROVE_WITH_MINOR_REVISIONS. Zero blocking or major protocol/persistence
findings. All persistence mechanisms independently re-verified this gate
(DDIC key/client-independence, content-addressed store idiom, signature
hash primitive, aRFC process-boundary/exception-scoping) match what the
design document and the prior three-cycle adversarial review already
established -- no drift, no contradiction, no new blocking risk. Three
non-blocking MINOR items (PP-01, PP-02, PP-03) are documentation/hardening
suggestions only and do not gate implementation. `AC-07 IT-01` remains the
one non-waivable, pre-production condition -- unchanged, unreopened, and
unclosed by this gate, exactly as instructed.