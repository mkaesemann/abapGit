# Variant B Package E – Snapshot Consumer Coherence and Adaptive Materialization

## Status

DRAFT_FOR_DISCOVERY_AND_REVIEW

Package D remains unchanged and must be completed before Package E productive
implementation begins.

## 1. Problem statement

After a successful cold load, remaining on the same branch works. After
switching to another branch, an immediate unfiltered Stage can fail with:

`<n> object(s) still missing after negotiated fetch`

The exact exception is raised by
`zcl_abapgit_ortec_missing_obj=>ensure_available` after a commit-based
`deepen 1` fetch fails to materialize every SHA requested by its consumer.

Package D does not own this path. Its design intentionally reuses
`zcl_abapgit_ortec_missing_obj` unchanged and does not reopen Package C's
cold/warm/branch-switch routing. Package E therefore owns the consumer
coherence and repair contract.

Package E additionally optimizes
`zcl_abapgit_ortec_cold_init=>materialize_tip_snapshot`.

## 2. Non-negotiable invariants

### INV-E-01 – Consumer identity

After successful snapshot publication, every Stage, Stage-by-Transport, Diff,
status and walk consumer uses the same repository key and tip commit that were
certified.

### INV-E-02 – No normal legacy completion

A normal consumer of an unchanged SNAPSHOT_COMPLETE tip must not invoke
`zcl_abapgit_ortec_missing_obj=>ensure_available`.

### INV-E-03 – Certified missing classification

An object missing from a certified current-tip snapshot is
CERTIFIED_BUT_MISSING. It is never remote deletion and never a normal lazy
completion condition.

### INV-E-04 – Invalidate before repair

CERTIFIED_BUT_MISSING invalidates the affected snapshot certificate before
repair starts.

### INV-E-05 – One repair only

One top-level consumer operation may start at most one snapshot repair for the
same repository, branch and tip.

### INV-E-06 – Repair uses snapshot semantics

Repair calls the Package B selected-tip materialization path. It must not use
the legacy `upload_pack_by_commit( deepen 1 )` completion path.

### INV-E-07 – Verify before republish

Repair republishes SNAPSHOT_COMPLETE only after every blob referenced by the
selected tip is present with status READY and object type blob.

### INV-E-08 – Failed repair publishes nothing

If the repair still leaves missing objects, the operation fails with a
structured diagnostic and publishes no certificate or branch pointer from the
failed attempt.

### INV-E-09 – No per-object persistence or network calls

No SQL or HTTP operation may execute once per missing blob, tree or delta base.

### INV-E-10 – Capabilities once

Upload-pack capabilities are discovered once per complete snapshot
materialization operation and reused by every adaptive blob batch.

### INV-E-11 – Adaptive bounded batching

MATERIALIZE_BLOBS starts with 500 wants, never sends fewer than 50 or more than
1000 wants as its normal target, and adapts from measured response bytes.
A successful batch may at most double the preceding target size.

### INV-E-12 – Hard response memory gate

One materialized batch response is limited to 25 MiB. An oversized response is
split recursively. A single-want response still exceeding the limit fails
cleanly.

### INV-E-13 – Metadata-only final verification

Post-materialization verification reads only SHA, readiness and object type.
It never reloads blob payloads and runs once over the complete selected-tip
blob set, not once per network batch.

## 3. Materialization algorithm

1. Derive the complete unique selected-tip blob SHA set.
2. Bulk-subtract locally READY presence.
3. Begin one publication attempt.
4. If nothing is missing, perform no HTTP request.
5. Discover upload-pack capabilities exactly once.
6. Deduplicate missing SHA values while preserving first-seen order.
7. Start with a 500-row request target.
8. Send one MATERIALIZE_BLOBS request.
9. If the response exceeds 25 MiB:
   - split the batch in half;
   - process both children with the same capability set;
   - halve the next top-level target;
   - never use the oversized parent's response size to grow the next batch.
10. If the response succeeds without splitting:
    - calculate the next target as:
      `current_rows * 16 MiB / response_bytes`;
    - clamp to 50..1000;
    - cap growth at 2x.
11. Decode and persist each response.
12. After every adaptive batch completes, run one metadata-only verification
    over the complete selected-tip blob set.
13. Only then mark full-complete and publish the snapshot.

## 4. Certified consumer decision flow

Before invoking any missing-object completion path, the consumer reads:

- repository key;
- branch;
- advertised/current remote tip;
- certified materialized commit;
- snapshot state;
- required SHA set.

### 4.1 Tip mismatch

If consumer tip differs from certified tip:

- issue no fetch;
- infer no deletion;
- publish no state;
- raise a structured consumer-tip mismatch diagnostic.

### 4.2 Snapshot not complete

If the consumer tip is current but SNAPSHOT_COMPLETE is absent, run the normal
Package B/C selected-tip materialization flow before allowing the consumer to
continue.

### 4.3 Snapshot complete and nothing missing

Consume the repository object store directly. Do not call
`MISSING_OBJ=>ENSURE_AVAILABLE`.

### 4.4 Snapshot complete but object missing

Classify CERTIFIED_BUT_MISSING:

1. invalidate the snapshot certificate;
2. begin one new attempt;
3. run selected-tip materialization for the same repository, branch and tip;
4. run complete metadata-only verification;
5. republish only on success;
6. retry the consumer once;
7. if data is still missing, fail without another repair.

## 5. Legacy-path handling

`zcl_abapgit_ortec_missing_obj=>ensure_available` remains physically present
until Package F.

Every productive call site is classified as:

- MIGRATED_TO_SNAPSHOT_REPAIR
- LEGACY_UNCERTIFIED_ONLY
- UNREACHABLE

No certified current-tip consumer may remain in LEGACY_UNCERTIFIED_ONLY.

## 6. Performance model

Let:

- N = all stored repository objects;
- K = missing selected-tip blobs;
- F = tree-frontier count;
- B = adaptive network batch count.

Expected normal shape:

- tree SQL: O(F), chunked set-based;
- missing-set SQL: O(ceil(K / 1000));
- capability HTTP: exactly one GET when K > 0;
- materialization HTTP: B POST requests plus bounded split retries;
- verification SQL: O(ceil(total tip blobs / 1000));
- verification payload reads: zero;
- publication transactions: one;
- normal incremental work must not scan N.

## 7. Required acceptance tests

### E-AC-01 – Cold switch followed by Stage

Cold load branch A, switch to previously unknown branch B, then open
unfiltered Stage.

Expected:

- Stage succeeds;
- consumer repo key equals certified repo key;
- consumer tip equals certified tip B;
- missing count is zero;
- `MISSING_OBJ=>ENSURE_AVAILABLE` call count is zero.

### E-AC-02 – Stage-by-Transport

Cold branch A, switch to B, select a valid transport request.

Expected:

- filtered Stage succeeds;
- no remote-deletion classification;
- no normal legacy missing-object completion.

### E-AC-03 – Certified snapshot corruption

Delete one current-tip blob after successful snapshot publication.

Expected:

- CERTIFIED_BUT_MISSING is detected;
- certificate is invalidated;
- exactly one selected-tip repair runs;
- the consumer retries once;
- publication occurs only after complete verification.

### E-AC-04 – Failed repair

Keep one current-tip blob unavailable after repair.

Expected:

- structured failure;
- no snapshot certificate from the failed attempt;
- no branch-pointer publication from the failed attempt;
- no second repair;
- no deepen-1 legacy fallback.

### E-AC-05 – Consumer-tip mismatch

Expected:

- structured mismatch failure;
- zero HTTP requests;
- zero publication;
- zero deletion classification.

### E-AC-06 – Warm re-entry

Branch A to B to unchanged complete A.

Expected:

- zero capability GETs;
- zero upload-pack POSTs;
- zero rematerialization.

### E-AC-07 – Adaptive large snapshot

At least 40,000 missing blob wants.

Expected:

- initial target 500;
- target always in 50..1000;
- growth at most 2x;
- oversized response triggers bounded splitting;
- capability discovery exactly once;
- no payload reads for verification;
- one logical final metadata verification;
- no SQL or HTTP per blob.

## 8. Required review gates

Package E requires:

- focused discovery;
- correctness review;
- protocol/persistence review;
- performance DESIGN_GATE;
- senior implementation;
- static performance scan;
- performance IMPLEMENTATION_AUDIT;
- regression validation.

Package F must not start until Package E is live-validated.
``