# Performance IMPLEMENTATION_AUDIT — Variant B D2 IT8 incident fix

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-INCIDENT-PERFORMANCE-AUDIT
MODE=IMPLEMENTATION_AUDIT
BASELINE=de0f11ce7e2146a1f7d47b2170594f4fa77c2658
SCOPE=src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap (get_reachable_objects)
       + src/ortec/git/zcl_abapgit_ortec_obj_store.clas.testclasses.abap
       (reachable_ignores_extra_ready)
```

Evidence type: live IT8 dump evidence (§3 of the incident artifact), direct
`ZAOG_OBJ_STORE`/`ZAOG_COMMIT_HIST` read-only queries against IT8, and
line-by-line re-read of the modified and surrounding source. No synthetic
large-fixture run was executed in this session (see limitations, §7).

## Verdict

**PASS for the implemented fix (§10.1 of the incident artifact); the second
confirmed root cause (§10.2, `ensure_available`) remains UNFIXED and is
explicitly out of this audit's implemented scope — it requires its own
dedicated design/protocol review before an implementation audit can apply
to it.**

## Quantified before/after model — `get_reachable_objects`

| Metric | Before (populate_cache present) | After (this fix) |
| --- | --- | --- |
| SQL calls per `get_reachable_objects` call | 1 unbounded `SELECT *` (populate_cache) + N per-level bulk `get_objects` calls (1 for commit, 1 per tree depth, 1 for blobs) | N per-level bulk `get_objects` calls only (commit, each tree depth, blobs) — the unbounded call is removed entirely, 0 replacement SQL added |
| Rows read per call | ALL `status='R'` rows for the repo (measured: 87,486 for the incident repo, and growing with every future branch buffered) | exactly the objects reachable from `iv_commit` (measured for the passing pre-existing test: 3; for the incident's own certified commit, the true reachable-tree size, not the store's full 87,486+) |
| Payload bytes read per call | full `obj_data` for every 'R' row in the repo (unbounded; the crash dump shows this alone drove ~3.96 GB resident memory before the crashing allocation) | full `obj_data` only for objects actually reachable from `iv_commit` — proportional to that one commit's tree size, matching every other caller's expectation of this method |
| Maximum simultaneous XSTRING/payload copies | 2 full-repo copies transiently resident: `lt_rows` (populate_cache-local) + `mt_cache` (session-global, MOVE-CORRESPONDING per row) | 1 payload copy per reachable object, exactly as `get_objects` already does for every other caller of that method — no full-repo copy of any kind |
| Cache lifetime/bound touched by this method | `mt_cache`/`mv_full_cache_repo_key` (unbounded, session-lifetime) unconditionally rewritten on every cache-repo mismatch | `mt_cache` is still populated (by the per-level `get_objects` calls, which insert only the rows they actually read) but is NEVER unconditionally `CLEAR`ed or bulk-rewritten by this method anymore |
| Batch sizes | N/A (single unbounded SELECT, no batching) | unchanged from `get_objects`' own existing bulk-fetch shape (per-level, not chunked at `c_select_package_size` — `iv_bulk_fetch = abap_true`, same as before this fix; this fix does not change that pre-existing bulk/chunk choice at all) |
| Transaction count | 0 (read-only) | 0 (read-only) — unchanged |
| Lock duration | none | none — unchanged |

## Scale behavior for the mandated cardinalities

| Stored objects (N, whole repo) | Objects reachable from one target commit (K) | SQL rows read by `get_reachable_objects` (after fix) | SQL rows read (before fix) |
| --- | --- | --- | --- |
| 1,000 | ~50 | ~50 (K) | 1,000 (N) |
| 40,000 | ~2,000 | ~2,000 (K) | 40,000 (N) |
| 1,000,000 | ~100 (a narrow incremental commit) | ~100 (K) | 1,000,000 (N) |

This directly restores the "K objects, not N" invariant for this method,
matching the shape already proven correct in the sibling method
`get_reachable_sha1s` and in every other caller of `get_objects` elsewhere in
this class.

## Verified sound (no finding) — unchanged parts of the call path

- **Per-level `get_objects( iv_bulk_fetch = abap_true )` calls** (commit,
  each tree depth, blob set): confirmed unchanged by this fix; each call is
  scoped to exactly that level's own SHA1 set (never the whole repo), and
  `get_objects`' own cache-miss fallback (`read_object_rows`, scoped to the
  same level's SHA1 set) is confirmed correct by direct source re-read —
  this fix relies on, but does not modify, this pre-existing bounded
  behavior.
- **No new SQL, HTTP, COMMIT, or loop was introduced.** The fix is a pure
  deletion of one method call and its comment; `git diff` for
  `zcl_abapgit_ortec_obj_store.clas.abap` shows only that removal plus a
  replacement doc comment (no code logic changed).
- **`get_reachable_sha1s`, `get_objects`, `get_object`, `get_available_
  objects`, `get_staged_delta_objects`, `read_object_rows`, `populate_cache`,
  `is_cache_valid`, `get_all_objects`, `clear_repo`**: confirmed byte-for-byte
  unchanged (only `get_reachable_objects`'s own body was edited).
- **D1's bulk external delta-base resolution and D2's staged-visibility/
  attempt-isolation mechanisms**: confirmed untouched (this fix does not
  touch `zcl_abapgit_ortec_delta`, `zcl_abapgit_ortec_pack_stream`,
  `zcl_abapgit_ortec_pack_dec`, `zcl_abapgit_ortec_pack_raw`,
  `zcl_abapgit_ortec_fastpath`, or `zcl_abapgit_ortec_porcelain` at all).
- **ORTEC-disabled standard abapGit behavior**: unaffected — this fix lives
  entirely inside a `zcl_abapgit_ortec_*` class method that is only reached
  from ORTEC-specific call chains.

## Findings

### AUDIT-AWAIT-1 (MAJOR, blocking for the TIME_OUT symptom specifically, NOT for this session's implemented fix)

```text
ID: AUDIT-AWAIT-1
Severity: MAJOR (blocking for closing the TIME_OUT half of this incident;
  NOT blocking for the SYSTEM_NO_ROLL fix implemented and audited above,
  which is independently complete and correct)
Path and method: zcl_abapgit_ortec_missing_obj=>ensure_available,
  src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap
Status: root cause CONFIRMED (incident artifact §4.2, §6, §9); fix
  deliberately NOT implemented in this session (incident artifact §10.2) -
  requires a dedicated correctness + protocol/persistence review pass to
  choose the correct request-shape fix (targeted blob want-list vs.
  pathspec-narrowed fetch vs. caller-side usage restriction), per this
  project's own mandatory gated workflow for anything that changes what is
  requested over the git wire protocol.
Required before D2/this incident can be marked fully closed: a follow-up
  design + review + senior-implementation pass scoped exactly to this one
  method and its one call site (build_files_from_rows), then its own
  performance DESIGN_GATE and IMPLEMENTATION_AUDIT cycle.
```

## Not re-litigated

D1's bulk external delta-base resolution, D2's staged-visibility (`status
IN ('D','R')`) model, attempt-ID plumbing, and repo-lock orchestration are
all confirmed untouched by this fix and are not reopened by this audit —
their own D1/D2 performance audits (`performance_audit_variant_b_package_d_d1.md`,
`performance_audit_variant_b_package_d2.md`) remain valid and are not
superseded here.

## Unexecuted scenarios

- No synthetic 40,000-object or 1,000,000-stored-key fixture run was
  executed in this session for `get_reachable_objects` specifically; the
  K-vs-N scale table above is derived from the method's own unchanged,
  already-bounded per-level `get_objects` logic (proven correct by the
  sibling `get_reachable_sha1s`'s existing test coverage) plus this fix's
  pure-deletion nature, not from a fresh large-scale measurement.
- No live SAT/ST05 trace was captured for either the pre-fix or (not yet
  possible) post-fix behavior; the "before" cost model in the table above is
  derived directly from the live SYSTEM_NO_ROLL dump's own measured
  `LT_ROWS[54226x280]` and memory-usage sections (see incident artifact §3.1),
  not from a controlled trace.
