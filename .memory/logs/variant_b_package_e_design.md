# Variant B Package E — Design (corrective, single active source)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E0-CORRECTIVE-DESIGN
STATUS=CORRECTIVE_DESIGN_COMPLETE -> SEE REVIEW ARTIFACTS FOR PER-SLICE VERDICT
SUPERSEDES=the two prior versions of this document at commit 8eef0b55 (the
  E0-DESIGN draft written this Package E cycle) and, further back in git
  history, the pre-Package-D2-closure "Snapshot Consumer Coherence and
  Adaptive Materialization" draft. Neither prior version is reproduced here;
  both remain readable via `git log -p -- .memory/logs/variant_b_package_e_design.md`.
BASELINE=8eef0b55fb37892c3d6b6428c038886481c73192
PRODUCTIVE_BASELINE=733bb30799886ef8659be7e293b82c3ccfcebbdd
```

This is the ONLY active design document for Package E. It replaces the prior
single-tier draft after ten corrective findings (CR-01..CR-10) were raised
against that draft's design, reviews, and bootstrap handoff. It is grounded
in [variant_b_package_e_discovery.md](variant_b_package_e_discovery.md), the
[memory audit](variant_b_package_e_memory_audit.md), and a further round of
direct source verification performed specifically for this corrective pass
(cited inline below with exact file/line evidence, not restated from the
prior draft's prose).

## 0. Summary verdict per slice (9-slice model)

Authorization is **per-slice**, not a single Package-E-wide yes/no
(correcting the prior draft's overstated blanket framing).

| Slice | Verdict | Authorization this pass |
| --- | --- | --- |
| E0-CLEANUP | This design rewrite itself | DONE (this document) |
| E1-TEST | Regression hardening for `rebuild_index`/`is_index_ready` | AUTHORIZED_NOW (test-only) |
| E1-PERF | Batch-size reduction of round trips (candidate E1-A) | AUTHORIZED_NOW (bounded, low-risk, evidence-backed; final "fast enough" closure remains open, see §2) |
| E2-DIAG | Evidence ladder D0→D3 for the false-MODIFIED symptom | D0/D1 AUTHORIZED_NOW (no persistence); D2/D3 BLOCKED_PENDING_D0_D1_INSUFFICIENCY |
| E2-FIX | Corrective change to the actual defect once found | NOT_AUTHORIZED (gated on a live mismatch proof from D1/D2) |
| E3-TEST | Regression hardening for Cache Admin F4 | AUTHORIZED_NOW (test-only) |
| E4-VERIFY | Scenario-matrix regression coverage for both existing repair mechanisms | AUTHORIZED_NOW (test-only) |
| E4-FIX | New repair code for a certified-repair gap | NOT_REQUIRED (see §4 verdict; residual risk documented, not designed away) |
| E-HARDEN | OF-2 constant-extraction fix + OF-3 regression pin | AUTHORIZED_NOW for the ORTEC-owned constant/test change; the cross-file architecture question it surfaced is NOT_AUTHORIZED to resolve unilaterally (owner decision required, see §6) |

`IMPLEMENTATION_AUTHORIZATION=PARTIAL` for Package E as a whole. No slice
changes runtime behavior of Stage/Diff/Patch/fetch/pull/branch-switch for a
disabled/default configuration; E1-PERF changes only a chunking constant on
an already-write-bound path with no algorithmic change.

---

## 1. Slice E1-TEST — OBJ_INDEX regression-test hardening (test-only)

Unchanged in substance from the prior draft's E1-T; renamed for the 9-slice
model and cross-referenced to E1-PERF below.

### In scope
- `rebuild_index` writes rows only for the exact requested commit's true
  tree (no cross-commit leakage) — construct two commits sharing a subtree,
  index both, assert row sets differ only where trees differ.
- `is_index_ready` STRICT mode correctly rejects a commit with no marker row
  and correctly accepts one with a marker row for the exact commit only;
  add the "marker exists for a DIFFERENT commit" negative case if absent.
- `rebuild_index`'s MODIFY batches are bulk, never per-row, AT WHATEVER
  chunk size E1-PERF sets (test must not hardcode the literal `1000`; it
  must assert "one MODIFY per `N`-row boundary" against the live constant).

### Exact symbols
Test-only changes to `zcl_abapgit_ortec_obj_index.clas.testclasses.abap`. No
production class or DDIC object touched by this slice.

### Invariants
- INV-E1-T-1: a rebuilt index for commit A must never contain a row for a
  different commit's tree state.
- INV-E1-T-2: `is_index_ready` STRICT must reject any marker not matching
  the exact `(repo_key, commit_sha1)` pair.

### Test matrix

| ID | Scenario | Expected |
| --- | --- | --- |
| E1-T-01 | Two commits, shared subtree, both indexed | Row sets correct per-commit, no leakage |
| E1-T-02 | Marker exists for commit A only | `is_index_ready(commit=B)` STRICT = false |
| E1-T-03 | Marker exists for exact commit | `is_index_ready` STRICT = true |
| E1-T-04 | >1 chunk boundary of files in one commit (grep `.testclasses.abap` FIRST for an existing >1000-row test before adding a duplicate; record found/not-found in the implementation handoff) | Multiple MODIFY packages at the live chunk size, all rows present, no duplicate key error |

### IT8 acceptance
Per CR-08: test-only does **not** mean IT8-exempt. Required: real IT8
activation/syntax check of the changed testclasses include, ABAP Unit
execution (`PASS`), and ATC (`PASS`) — no local-only "green" claim is
sufficient. See §7 for the corrected acceptance rule applied to every slice.

### Checkpoint boundary
Independently committable; no dependency on any other slice.

---

## 2. Slice E1-PERF — OBJ_INDEX write-path round-trip reduction (new, CR-04)

### Why E1 is reclassified

The prior draft's `ACCEPTABLE_AS_IMPLEMENTED` verdict is **withdrawn**. The
correct classification is `CORRECT_BUT_PERFORMANCE_OPEN`: `rebuild_index`
produces correct rows (no defect), but its dominant cost —
9,390,412 µs (~9.39 s) of a 31.42 s trace, i.e. ~30% of total elapsed time,
and the single largest ORTEC-attributable cost center — was previously left
unranked and unauthorized with only a one-line "not shown to be a
bottleneck" dismissal. That dismissal is incorrect on direct re-reading of
the SAT evidence's own reconciliation.

### Quantified evidence (source: [variant_b_d2_sat_warm_to_cold_o4h8794.md](../incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md) §4–§6, re-verified this pass)

```text
Phase I (index write): 41 bulk MODIFY @ 1000-row chunk + 82 underlying
  "DB: Exec" round trips (~2 execs/MODIFY, "normal HANA array-upsert
  behavior" per the incident's own §6) = 9,354,131 µs net, ~114 ms/round
  trip average.
Read side (tree fetch for the BFS walk): only 11 total ZAOG_OBJ_STORE reads,
  67,362 µs — NOT the bottleneck, NOT affected by the write-side chunk size.
The incident's own §6 explicitly characterizes the cost as "consistent with
  a fixed per-statement/round-trip overhead, not a payload-size-driven
  cost" — i.e., round-trip COUNT, not row/byte volume, drives the 9.39 s.
```

Row byte size (verified via `zaog_obj_index.tabl.xml`, all fields CHAR,
non-payload metadata): CLIENT(3)+REPO_KEY(12)+COMMIT_SHA1(40)+OBJ_TYPE(4)+
OBJ_NAME(40)+PATH_HASH(40)+FILE_PATH(255)+FILE_NAME(255)+BLOB_SHA1(40)+
TREE_SHA1(40)+IDX_STATUS(1) ≈ 730 bytes/row — small, fixed-width, uniform;
this is metadata, never a blob payload (blob payloads live in
`zaog_obj_store`, untouched by this slice).

### Ranked candidate evaluation (E1-A through E1-F, per CR-04)

| ID | Candidate | SQL/round trips | Bytes/memory | Lock/rollback | DDIC | Correctness risk | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- |
| E1-A | Raise the ABAP-side chunk constant (e.g. 1000→5000) for `rebuild_index`'s MODIFY | 41→~9 MODIFYs, ~82→~18 execs (≈4.5x fewer round trips); read side (11 reads) unaffected | ~3.65 MB peak `lt_rows` at 5000 rows (730 B/row) vs ~0.73 MB today — trivial, no XSTRING/payload growth, no DBSQL_STMNT_TOO_LARGE-class risk (true array bulk DML, not an expanding IN-list) | Repo lock already held for the whole rebuild regardless of chunk size; total hold time is expected to SHRINK (fewer round trips), not grow; no new `COMMIT WORK`; unaffected rollback semantics | None | None — same per-commit walk algorithm, only the chunk-size constant changes | **RECOMMENDED — AUTHORIZED_NOW** |
| E1-B | Adaptive byte-target batching (mirror `zcl_abapgit_ortec_cold_init`'s 500/50/1000/2x/16 MiB algorithm) | Same ballpark as E1-A at best | Adaptive logic pays for itself only when per-row BYTE SIZE varies widely; this table's rows are uniform ~730 B CHAR fields — near-zero variance, so adaptive complexity buys nothing over a well-chosen fixed constant | More code paths = more edge cases to verify | None | Medium implementation risk for no evidenced benefit over E1-A | NOT JUSTIFIED for this cost shape; reconsider only if live remeasurement after E1-A shows real byte variance (not expected) |
| E1-C | Reduce the ~2-execs-per-MODIFY HANA behavior directly | The incident's own §6 labels this "normal HANA array-upsert behavior" — not an ABAP-application-level lever | n/a | n/a | None | No separate actionable design exists | FOLDED INTO E1-A (fewer MODIFYs proportionally reduces total execs even if the ~2x/statement factor itself is unchanged) |
| E1-D | Tree-SHA1-keyed cross-commit row reuse, bare `(repo_key, tree_sha1)` key | Would avoid the walk entirely for a previously-seen subtree — but see correctness finding below | New non-unique secondary DDIC index required | New correctness review required | Additive index | **PROVEN UNSAFE AS A BARE KEY** — see finding below | DEFERRED, not authorized; requires a richer key design |
| E1-E | Incremental tree-diff update against a known prior indexed commit | Highest theoretical ceiling (row-count-similarity evidence: ≤0.03% spread across 4 commits) but requires a parallel-tree-walk diff algorithm, deletion handling, and the SAME `.abapgit`-context safety proof as E1-D, plus a "which prior commit" policy | n/a (design not yet specified) | New design + correctness + performance gates required | None yet designed | High engineering complexity | DEFERRED as the most promising LONG-TERM candidate; start only if E1-A's remeasured impact is judged insufficient |
| E1-F | No code change; accept current cost pending an owner-defined SLA | Zero | Zero | Zero | None | None | Fallback-only; NOT recommended given E1-A is low-risk, low-effort, and evidence-backed |

**Ranking: E1-A (recommended, authorize now) > E1-E (best long-term, own
design cycle) > E1-D (deferred, proven unsafe as originally conceived) ≈
E1-B (not justified) > E1-C (no separate action, folded into E1-A) > E1-F
(fallback only).**

### New correctness finding this pass: why E1-D is unsafe as a bare key (CR-04's explicit verification requirement)

Direct read, `zcl_abapgit_ortec_obj_index.clas.abap` lines ~399–414: the
per-file mapping call

```abap
zcl_abapgit_filename_logic=>file_to_object(
  EXPORTING
    iv_filename = <ls_node>-name
    iv_path     = <ls_work>-path
    iv_devclass = iv_devclass
    io_dot      = io_dot
  IMPORTING
    es_item     = ls_item ).
```

depends on **caller-supplied** `io_dot` (a `zcl_abapgit_dot_abapgit` instance
— the `.abapgit` metadata file's parsed content: starting folder, ignore
patterns, mapping config) and `iv_devclass`, both passed into `rebuild_index`
per invocation, **not fixed per repository**. `.abapgit` is itself a
versioned file inside the repo's own tree and can legitimately differ
between commits/branches of the same repository (e.g. a changed starting
folder or ignore rule committed on one branch but not another). Therefore an
identical raw `tree_sha1` does **not** guarantee an identical
`(obj_type, obj_name)` resolution unless the `.abapgit`/devclass context used
at both build times is also proven identical — the prior draft's framing
("plausible... not yet formally proven") is corrected here to **PROVEN
UNSAFE as a bare `(repo_key, tree_sha1)` key**. Any future E1-D/E1-E design
must key reuse by at minimum `(tree_sha1, hash-of-.abapgit-content,
devclass)`, not `tree_sha1` alone.

### E1-A implementation contract (fixed this pass, bootstrap consistency review)

The prior deferral ("exact target value... to be confirmed at
implementation time") is **withdrawn**. Direct re-read of
`zcl_abapgit_ortec_obj_index.clas.abap` (method `rebuild_index`, chunk
check at the `IF lines( lt_rows ) >= 1000.` line, ~line 448) confirms the
current chunk boundary is a bare, undocumented literal `1000` — not a named
constant. The following contract is now fixed and complete:

```text
CLASS=zcl_abapgit_ortec_obj_index
METHOD=rebuild_index
NEW_CONSTANT=c_index_write_chunk_size TYPE i VALUE 5000
  (new PRIVATE class constant, same section/visibility as the existing
  sibling constants c_status_ready/c_marker_obj_type/c_marker_obj_name;
  replaces the bare literal 1000 at the `IF lines( lt_rows ) >= 1000.`
  check and the following `MODIFY zaog_obj_index FROM TABLE lt_rows.`)
OLD_BATCH_SIZE=1000 rows (undocumented literal)
NEW_BATCH_SIZE=5000 rows (fixed constant, not adaptive, not a range)
ROW_BOUND=5000 rows per MODIFY chunk (hard bound)
BYTE_BOUND=<= 3.65 MB peak `lt_rows` buffer (5000 rows x ~730 bytes/row,
  all CHAR fields per zaog_obj_index.tabl.xml; no XSTRING/blob payload is
  ever held here, payloads live only in zaog_obj_store)
EXPECTED_SQL_PACKAGES_AT_42000_ROWS:
  OLD = 42 MODIFY statements (ceil(42000/1000)), ~84 underlying HANA execs
    at the incident's own observed ~2 execs/MODIFY ratio
  NEW = 9 MODIFY statements (8 full 5000-row chunks + 1 remainder chunk of
    2000 rows), ~18 underlying execs
  REDUCTION = ~4.7x fewer round trips, consistent with the ranked-candidate
    table's "~4.5x" estimate in the section above
MEMORY_MODEL=`lt_rows` is CLEARed immediately after each MODIFY (existing
  pattern, unchanged); peak per-call footprint rises from ~0.73 MB to
  ~3.65 MB (net +2.92 MB), a single method-local internal table, released
  between chunks — trivial against any ABAP work-process memory budget
LOCK_MODEL=unchanged; the existing repo lock
  (zcl_abapgit_ortec_pack_raw=>release_repo_lock) is held for the whole
  `rebuild_index` call regardless of chunk size; total hold time is
  expected to shrink (fewer round trips), not grow
TRANSACTION_MODEL=unchanged; confirmed via a source-wide grep that
  zcl_abapgit_ortec_obj_index.clas.abap issues NO `COMMIT WORK` anywhere —
  every chunk MODIFY remains part of the caller's existing SAP LUW; the
  chunk boundary introduces no new commit point
ROLLBACK_MODEL=unchanged; the method's existing
  `CATCH zcx_abapgit_exception` / `CATCH cx_root` handlers release the repo
  lock and re-raise on any failure at any point in the walk, chunked or
  not — a failure after N of M chunks leaves those N chunks' rows
  uncommitted exactly as today, since the caller's own commit boundary is
  untouched by this slice
IT8_MEASUREMENT=reuse the exact warm-to-cold reproduction from
  [variant_b_d2_sat_warm_to_cold_o4h8794.md](../incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md)
  (same repo/branch/commit set, ~42,000-row scale). BEFORE = the already-
  captured trace (chunk=1000: 41 MODIFYs / 82 execs / 9,390,412 us Phase I
  net). AFTER = a fresh SAT trace of the identical scenario post-change
  (chunk=5000), asserting: (a) MODIFY count <= 10, (b) round-trip count
  drops proportionally, (c) Phase I net elapsed measurably decreases below
  9.39 s and the delta is recorded in a new incident/measurement note, not
  merely assumed from the round-trip-count math
```

E1-A is **CONTRACT_DEFINED**, not `BLOCKED_FOR_DESIGN_COMPLETION` — every
required field above is fixed at design time; no implementation-time choice
remains open for this candidate.

### E1-PERF disposition

```text
E1_PERFORMANCE=CORRECT_BUT_PERFORMANCE_OPEN
AUTHORIZED_NOW=E1-A only, per the fixed contract above, plus regression
  tests asserting: correct chunking at the new 5000-row boundary, no
  duplicate-key errors, identical row content vs. the 1000-chunk baseline
  for the same fixture
NOT_AUTHORIZED=E1-B, E1-D, E1-E (own design/review cycles required)
CLOSURE_CONDITION=E1's final "acceptable now" declaration remains open until
  the IT8_MEASUREMENT above is actually performed and recorded; no owner
  SLA number is required to authorize E1-A itself (strictly non-regressive,
  reversible config-constant change), but an owner-defined SLA may still be
  requested to decide whether E1-E is later worth pursuing.
```

### Checkpoint boundary
Independently committable; depends only on E1-TEST's chunk-agnostic test
design (§1).

---

## 3. Slice E2-DIAG — Consumer-coherence evidence ladder (redesigned, CR-03)

### Why an evidence ladder, not a single-tier persistent log

The prior draft jumped directly to a new, always-available, off-by-default
**persistent DDIC diagnostic table** as the only proposed instrumentation.
CR-03 requires the least invasive tier that could plausibly resolve the
question be tried first. A dedicated search this pass
(`grep -i "bal_log|application_log|diagnostic" src/ortec/**`) found **no**
existing ORTEC-owned generic bounded log mechanism (confirming OQ-1's
"reuse?" question in the negative for ORTEC-owned code) — but this search
was too narrow: **SAP's own standard Application Log (function group `SBAL`,
transaction `SLG1`)** is a system-wide, already-existing, already-bounded
(own retention/deletion job `SBAL_DELETE`) logging mechanism requiring **no
new DDIC table at all**. This materially changes OQ-1's answer from "nothing
exists, build one" to "a suitable existing mechanism exists; use it before
inventing a new table."

### D0 — Owner-supplied reproduction packet (no code, AUTHORIZED_NOW)

Before any instrumentation is written, ask the owner for one concrete,
already-observed occurrence:
- repository key, branch, exact file path;
- the local file's computed SHA1 at the time MODIFIED was reported;
- the remote/index-reported SHA1 for the same path;
- the commit SHA1 the Stage/status operation believed was current.

If this packet already lets us compute an independent answer (e.g. we can
re-derive the true blob SHA1 for that exact commit+path from the persisted
object store without any new code), stop here — no instrumentation is
needed at all.

### D1 — Read-only, on-demand, single-row comparison report (no persistence, AUTHORIZED_NOW)

Per CR-06: this is a targeted **detection**, not a full rebuild-validate-
every-row sweep. A new admin-triggered method (e.g.
`zcl_abapgit_ortec_obj_index=>verify_one_row` or a Cache Admin report
action), given `repo_key` + `commit_sha1` + one file path:
1. Reads the EXISTING `zaog_obj_index` row for that exact key (no write).
2. Independently walks ONLY the tree nodes from the commit's root down to
   that ONE path (bounded by tree DEPTH, not total commit file count —
   O(path depth), not O(~42,000)), re-fetching those specific tree objects
   from `zaog_obj_store` and re-decoding them in memory.
3. Re-computes the blob SHA1 that should exist at that path today from that
   independent walk.
4. Reports MATCH or MISMATCH. **Never repairs automatically** — this is
   read-only detection only, satisfying INV-E2-D-3's "never consulted by a
   correctness decision, write-only" spirit by instead being
   "read-only, decision-free" (stronger — no write path at all in D1).

This single tool directly targets OF-1 (stale-but-present index row) with a
bounded, cheap, on-demand check — it does **not** require enabling anything
on the hot path and requires zero new persistence.

### D2 — Bounded, existing-mechanism logging (BAL/SLG1, gated behind D0/D1 insufficiency)

Only if D0+D1 cannot capture a live occurrence on demand (e.g. the symptom
is transient/timing-dependent and not currently reproducible against a
known path): add an opt-in, `repo_key`-scoped flag (correcting the
correctness review's MINOR finding that a global flag would be
unworkable) that, only when explicitly enabled, causes
`build_files_from_rows` to write one `BAL_LOG_MSG_ADD`-family entry per
served batch (never per row) with `repo_key`, `commit_sha1`, `obj_name`,
`blob_sha1`, timestamp. Retention/capping (OQ-2) is inherited for free from
SAP's own standard `SBAL_DELETE` housekeeping job — no new custom retention
policy needs to be invented. Disabled (default) cost: one cached boolean
check, zero additional SQL/HTTP, byte-identical behavior to
`SAP_VALIDATED_COMPLETE` (INV-E2-D-1, retained from the prior draft).

### D3 — New persistent DDIC sink (last resort, NOT authorized by default)

Only if D2's BAL-based capture proves structurally insufficient (e.g. a
genuine need for relational joins/queries beyond SLG1's own message-based
model). Not designed further here; would require its own DDIC review,
key structure, and a fresh performance DESIGN_GATE if ever proposed.

### OF-1 as an active E2 candidate (CR-06)

OF-1 (stale-but-present index row, no self-heal) is **kept active** as the
leading candidate root cause, not merely documented. D1's `verify_one_row`
tool is the concrete detection design; a **bounded repair** (also gated,
not authorized this pass) would, once D1 proves a specific
`(repo_key, commit_sha1, obj_name)` mismatch, DELETE + re-derive ONLY that
one row from D1's own independently-recomputed value — never a full
`rebuild_index` re-walk. This is specified now so E2-FIX can be implemented
quickly once real evidence exists, but remains NOT_AUTHORIZED until that
evidence exists.

### Invariants
- INV-E2-D-1: disabled/default state (D2 flag off, D1 not invoked) is
  byte-identical to current `SAP_VALIDATED_COMPLETE` behavior.
- INV-E2-D-2 (if D2 is ever implemented): one bounded log write per served
  batch, never per file.
- INV-E2-D-3: no diagnostic tier (D1 read or D2 log) is ever consulted by
  any correctness decision; D1 output is for a human to read, D2 log is
  write-only from the production path.
- INV-E2-D-4 (new, CR-06): a bounded single-row repair, if ever
  implemented, must never re-walk the full commit's tree — only the exact
  reported key.

### Test matrix

| ID | Scenario | Expected |
| --- | --- | --- |
| E2-D-01 | D1 tool run against a row known (by test fixture) to be correct | MATCH reported |
| E2-D-02 | D1 tool run against a row deliberately mutated to be stale | MISMATCH reported, no write occurs |
| E2-D-03 | D2 flag disabled (if implemented) | Zero additional SQL statements, identical return values |
| E2-D-04 | D2 flag enabled, normal serve (if implemented) | One bounded BAL log entry per batch |
| E2-D-05 | D2 flag enabled, log write raises (if implemented) | Original file-serving call still succeeds |

### IT8 acceptance plan
1. D1: deploy the read-only tool; confirm it produces MATCH for every row of
   a freshly rebuilt index (sanity check) before relying on it for a live
   MISMATCH investigation.
2. On the next live report of a false MODIFIED, run D1 against the affected
   path first — if it reports MISMATCH, OF-1 is confirmed and E2-FIX may be
   proposed; if it reports MATCH, the standard-abapGit-serialization-noise
   hypothesis becomes the leading candidate instead (out of ORTEC's remit).
3. Only escalate to D2 if D1 cannot be run against the reported case (e.g.
   the exact path is not known/reproducible on demand).

### Checkpoint boundary
D0 (no code) and D1 (read-only tool + tests) are independently committable
now. D2/D3 are explicitly out of this checkpoint.

---

## 4. Slice E2-FIX — Corrective change to the actual E2 defect (placeholder, gated)

### Status
`NOT_AUTHORIZED`. Per the run brief's hard-stop clause (unchanged from the
prior draft, still correctly applied): no corrective code change is
authorized until D1 or D2 produces a live, reproduced MISMATCH tying a
specific defect class (stale index row vs. serialization noise vs. wrong
commit/tip) to the reported symptom. This slice exists in the 9-slice model
only as a placeholder so the eventual fix has a pre-agreed name and
checkpoint slot; its exact design (bounded single-row repair vs. something
else) depends entirely on what D1/D2 find.

---

## 5. Slice E3-TEST — Cache Admin F4 regression-test hardening (test-only)

Unchanged in substance from the prior draft's E3-T; renamed for the
9-slice model.

### In scope
Add ABAP Unit coverage pinning `get_repo_f4_values`'s three-tier union
(`ZAOG_REPO_STATE` primary, `ZAOG_OBJ_STORE`-orphan fallback,
`ZAOG_COMMIT_HIST`-orphan fallback) — no test currently exercises the two
fallback tiers (discovery OF-4).

### Exact symbols
Test-only changes to `zcl_abapgit_ortec_cache_admin.clas.testclasses.abap`.

### Invariants
- INV-E3-T-1: a repository present only in `zaog_obj_store` appears in F4
  output labeled `<orphaned cache>`.
- INV-E3-T-2: a repository present only in `zaog_commit_hist` appears via
  the third tier.
- INV-E3-T-3: a repository present in `zaog_repo_state` is never duplicated
  by either fallback tier.

### Test matrix

| ID | Scenario | Expected |
| --- | --- | --- |
| E3-T-01 | Repo in repo_state only | One row, real branch/remote_url |
| E3-T-02 | Repo in obj_store only | One row, `<orphaned cache>`/`<no repository state>` |
| E3-T-03 | Repo in commit_hist only | One row via third tier |
| E3-T-04 | Repo in all three | Exactly one row (deduplicated), sourced from repo_state |

### IT8 acceptance
Per CR-08 (corrected): real IT8 activation/syntax check, ABAP Unit
execution, and ATC — same rule as E1-TEST, no test-only exemption.

### Checkpoint boundary
Independently committable; no dependency on any other slice.

---

## 6. Slice E4-VERIFY — Certified-repair scenario coverage (redesigned, CR-05)

### Why a full scenario matrix instead of a blanket verdict

The prior draft concluded `E4_NOT_REQUIRED` from the existence of two
repair mechanisms without systematically checking whether every reachable
state/scenario combination is actually covered by one of them. This slice
builds that matrix directly from a fresh source re-read (including
confirming, this pass, that the two mechanisms are cleanly **dispatch-
exclusive**, not live parallel duplicates — see the dispatch evidence
below) and issues one of the three required verdicts against it.

### Dispatch evidence (new this pass)

Direct read, `zcl_abapgit_git_porcelain.clas.abap` lines ~525–560:
`pull_by_branch` checks `zcl_abapgit_ortec_git_switch=>is_active_for_repo`
FIRST; if true it delegates entirely to
`zcl_abapgit_ortec_porcelain=>pull_by_branch` and **returns immediately** —
the standard file's own embedded `'Walk,'`-catching
`invalidate_all_history` + retry block (lines ~583–610, confirmed to exist
verbatim, including inline ORTEC references and an
`|ORTEC: Walk error on { iv_branch_name } - self-healing retry...|` progress
message) is reached only as the **non-ORTEC-active legacy fallback**, never
concurrently with the ORTEC-active path for the same repository. This
downgrades an initial concern (raised, then verified, during this pass)
that these might be two independently-evolving live implementations of the
same logic — they are not; they are mutually exclusive by construction.
The maintenance-drift risk (two copies of the same literal/comment block
that could diverge on a future edit to only one) is real and is addressed
in E-HARDEN (§7), not here.

### Scenario × property matrix (9 scenarios)

| # | Scenario | Trigger | Mechanism engaged | Retry bound | Invalidation scope | Detects content-wrong (not just missing)? | Test coverage |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | Filtered access, single missing blob, ORTEC active | `get_missing_sha1s` finds a gap | `ensure_available` → bounded `materialize_missing_batches` top-up | One | N/A (object-level, not repo-wide) | No | Existing (D2) |
| 2 | Filtered access, blob still missing after retry | Same, remote also cannot supply it | `ensure_available` raises `zcx_abapgit_ortec_git` | One (no second attempt) | N/A | No | E4-D-03 (confirm existing coverage first) |
| 3 | Unfiltered/porcelain, ORTEC active, `'Walk,'` tree-not-found, first failure | `pull()` raises | `zcl_abapgit_ortec_porcelain=>pull_by_branch`'s `invalidate_all_history` + one retry | One | Whole-repo (deliberate — haves shared across branches) | No | E4-D-01 |
| 4 | Same, `'Walk,'` blob-not-found variant | `pull()` raises | Same mechanism, blob variant | One | Whole-repo | No | Fold into E4-D-01/02 fixture variants |
| 5 | Unfiltered, `'Walk,'` persists after retry (second failure) | Same | Original exception re-raised unchanged, no second repair | Zero further | N/A | No | E4-D-02 |
| 6 | ORTEC inactive for repo | `is_active_for_repo` = false | Standard abapGit's own legacy fallback path (out of ORTEC's remit); confirmed dispatch-exclusive (see above) | Standard behavior, unchanged | N/A | No | Not ORTEC's to test (standard abapGit's own scope) |
| 7 | Certified/complete snapshot tip, a blob is later deleted OUT-OF-BAND (bypassing all ORTEC write APIs, e.g. direct table maintenance) | No normal caller path reaches this without an external precondition | **Neither mechanism explicitly detects "certified tip, data now gone"** — `ensure_available` would still attempt a bounded top-up if reached (filtered path) and either self-heal or raise the same generic exception; no CERTIFIED_BUT_MISSING classification exists | N/A | N/A | N/A | **Gap — see verdict** |
| 8 | Stale-but-present (wrong-content) `zaog_obj_index` row feeding an already-present-but-wrong blob to a consumer | Normal filtered Stage | **Neither mechanism detects this** — both are presence/completion checks, not content-correctness checks | N/A | N/A | No (by design — this is E2-DIAG's job, not E4's, see OF-1) | **Owned by E2-DIAG, not E4 — not a gap in E4's own remit** |
| 9 | Multiple unrelated `'Walk,'` failures within one unfiltered pull's tree walk | Same tree walk, >1 missing node | Same whole-repo invalidate + one unified retry (no per-failure special-casing) | One (unified) | Whole-repo | No | Fold into E4-D-01/02 as a multi-miss fixture variant |

### Verdict

```text
E4_REPAIR=E4_NOT_REQUIRED_CURRENTLY_COMPLETE for all 7 scenarios reachable
  by normal application operation (1-6, 9). Scenario 8 is explicitly
  OWNED_BY_E2-DIAG (content-correctness, not completion-repair — no
  double-counting against E4). Scenario 7 (certified tip whose data is
  later removed by an out-of-band administrative action bypassing all
  ORTEC write APIs) is a genuine, evidence-based residual gap that is NOT
  reachable through any normal consumer operation and has never been
  reported as a live incident. Per CR-05's "issue one of three verdicts"
  requirement applied at the scenario level: scenario 7 alone would merit
  E4_PARTIAL_GAP_REQUIRES_DESIGN if it were reachable by normal operation;
  because it requires bypassing the application's own persistence APIs
  entirely, it is instead recorded as an EXPLICIT, OWNER-VISIBLE ACCEPTED
  RISK (not silently dropped, not a new design manufactured for an
  unreachable-by-normal-means precondition) — tracked in `.memory/state.md`
  as `E4-OOB-DELETION-RISK`, requiring explicit owner risk-acceptance
  (or a future design, if the owner disagrees with this framing).
```

### E4-OOB-DELETION-RISK disposition (fixed this pass, bootstrap consistency review)

```text
DISPOSITION=ACCEPTED_NON_BLOCKING_RISK
```

Rationale: this scenario requires bypassing every ORTEC write API (e.g.
direct table maintenance against `zaog_obj_store`), which is already
outside the application's own security/API boundary — no UI, RFC, or
public class method reaches this precondition. It has never been reported
as a live incident. This is why it is `ACCEPTED_NON_BLOCKING_RISK` rather
than `REQUIRES_E4_FIX`: there is no evidenced live occurrence to design a
fix against, and manufacturing new production code for an unreachable-by-
normal-means precondition would be over-engineering. It is not
`OWNED_BY_E2` (E2-DIAG's remit is content-WRONG rows that are still
present, not rows whose backing blob payload was removed at the DB layer)
and not `OWNED_BY_PACKAGE_F` (Package F is validated legacy-code cleanup,
not a home for new residual-risk ownership). It is not
`NOT_APPLICABLE_WITH_PROOF` because the gap is real, not disproven — it is
simply unreachable through the application's own surface.

**How consumers remain safe**: if this precondition ever occurs, the next
filtered access to the affected blob goes through the existing
`ensure_available` bounded top-up path. If the remote still has the
object, it self-heals transparently (no consumer-visible defect). If the
remote has also lost it (a compounding, independently rare event), the
existing generic exception is raised to the caller — a loud, safe failure,
never a silently wrong diff/stage/status result. No new failure mode is
introduced; the existing missing-object exception path already covers the
observable symptom.

**How recovery occurs**: recovery is the same manual operational action
already available today for any missing-object incident — a targeted
remote re-fetch of the affected commit, or a full repository re-clone/
`rebuild_index` for that repository. No new recovery mechanism is designed
or required for this pass; the risk is accepted specifically because the
existing generic recovery path already applies to its failure mode.

### Regression test matrix (new/confirmed coverage)

| ID | Scenario | Expected |
| --- | --- | --- |
| E4-D-01 | `pull()` raises `'Walk, tree not found'` once, succeeds on retry | `invalidate_all_history` called once, `COMMIT WORK` once, retry succeeds, no re-raise |
| E4-D-02 | `pull()` raises `'Walk,'` twice (include a blob-not-found variant and a multi-node-failure variant per scenarios 4/9) | `invalidate_all_history` called exactly once, original exception re-raised unchanged on the second failure |
| E4-D-03 | `ensure_available`, blob still missing after retry (confirm existing coverage first — grep before duplicating) | `zcx_abapgit_ortec_git` raised |
| E4-D-04 (new) | ORTEC-active repo: confirm `zcl_abapgit_git_porcelain=>pull_by_branch` delegates to `zcl_abapgit_ortec_porcelain=>pull_by_branch` and returns before reaching the standard file's own embedded `'Walk,'` block | Dispatch-exclusivity assertion — pins the finding in §6 against future drift |

### Invariants
- INV-E4-D-1: exactly one retry occurs on a `'Walk,'` exception; a second
  identical failure re-raises the ORIGINAL exception unmodified.
- INV-E4-D-2: `ensure_available` raises `zcx_abapgit_ortec_git` (never a
  silent success) when a blob remains missing after its one bounded retry.
- INV-E4-D-3 (new): the ORTEC-active and non-active `pull_by_branch` code
  paths remain mutually exclusive (one returns before the other is
  reached) — a future edit must not make both reachable for the same call.

### IT8 acceptance
Per CR-08 (corrected): real IT8 activation/syntax check, ABAP Unit
execution, and ATC — same rule as every other slice.

### Checkpoint boundary
Independently committable; no dependency on any other slice.

---

## 7. Slice E-HARDEN — OF-2 and OF-3 decided dispositions (new, CR-07)

### OF-3 — RELAXED absent-strictness mode

Direct read, `zcl_abapgit_ortec_git_switch.clas.abap` lines ~44–58:
`cs_absent_strictness-mode` is a **compile-time CONSTANT**, currently and
by-default set to `mode_strict`. The class's own doc comment states RELAXED
"must never ship as default" and requires "an explicit code change +
redeploy" to ever activate — there is no runtime/session toggle, no config
table, no way for any user (including an administrator) to enable it
without editing and re-transporting source code. An existing test
(`zcl_abapgit_ortec_git_tests`, line ~645) already pins
`cs_absent_strictness-mode = cs_absent_strictness-mode_strict`.

```text
OF-3_DISPOSITION=ALREADY_ADEQUATELY_MITIGATED. No further code change
  required. Action this pass: none beyond confirming the existing pinning
  test remains present (confirmed present, no gap). Closed, not deferred.
```

### OF-2 — `'Walk,'` string-match repair trigger

Direct read (this pass) of `zcl_abapgit_ortec_porcelain.clas.abap` lines
~505–580 and `zcl_abapgit_git_porcelain.clas.abap` lines ~905–944: the
`'Walk, tree not found'`/`'Walk, blob not found'` literals are **ORTEC's own
private `walk` method's own raised exception text**, matched by ORTEC's own
`CS 'Walk,'` check in the SAME class's `pull_by_branch` — not a match
against an external/standard-library exception whose text ORTEC does not
control. The standard file (`zcl_abapgit_git_porcelain.clas.abap`) carries
an **already-shipped, pre-existing copy of the identical
raise-text/catch-text/invalidate-retry pattern** as its own legacy fallback
for ORTEC-inactive repositories (dispatch-exclusive from the ORTEC-active
path, confirmed in §6). This is lower external-coupling risk than the prior
draft assumed (it is not coupled to an SAP-standard or third-party
exception string), but it IS a real, evidenced **duplication-drift risk**:
the same literal + comment block exists in two files/methods, and a future
edit to one copy's exact raised text could silently desynchronize from the
other's `CS 'Walk,'` match.

```text
OF-2_DISPOSITION=DECIDED, ACTIONABLE, LOW-RISK FIX AUTHORIZED_NOW.
Extract the shared literal into one named constant (e.g.
  zcl_abapgit_ortec_git_switch=>c_walk_error_prefix = 'Walk,', since the
  standard file already directly references this class inline and is no
  more coupled to it by also referencing one more constant) referenced by
  BOTH raise sites in zcl_abapgit_ortec_porcelain.clas.abap's own walk()
  method and its own pull_by_branch's CS check. Add a regression test
  pinning that the raised prefix and the matched prefix are the same
  constant (not independently duplicated literals).
NOT_AUTHORIZED_THIS_PASS: touching zcl_abapgit_git_porcelain.clas.abap's own
  embedded legacy copy — that file is standard abapGit's own source, its
  modification carries a different review bar, and the ARCHITECTURE
  QUESTION this finding surfaces (why does a "standard" file carry embedded
  ORTEC-aware branching and duplicate ORTEC logic instead of a single clean
  hook?) is a Package F/architecture-cleanup question requiring explicit
  owner input, not a unilateral decision in this corrective pass. Tracked
  in .memory/state.md as a new item, `E-HARDEN-STANDARD-FILE-COUPLING`.
```

### Test matrix

| ID | Scenario | Expected |
| --- | --- | --- |
| E-HARDEN-01 | `walk()` raises via the shared constant | Raised text matches `c_walk_error_prefix` exactly |
| E-HARDEN-02 | `pull_by_branch`'s `CS` check | Matches against the same shared constant, not an independently duplicated literal |
| E-HARDEN-03 (confirm existing coverage) | `cs_absent_strictness-mode` pinning test | Still present and passing (OF-3) |

### IT8 acceptance
Real IT8 activation/syntax check, ABAP Unit, ATC — same rule as every slice.

### Checkpoint boundary
Independently committable; no dependency on any other slice; excludes the
standard-file architecture question (owner decision required first).

---

## 8. Explicitly deferred, NOT authorized this Package E

### D-1: E1-D/E1-E tree-SHA1-keyed or incremental-diff index reuse
See §2 — proven unsafe as a bare key; requires a richer composite key,
a dedicated correctness review, and its own performance DESIGN_GATE.
Tracked as `E1-TREE-REUSE`.

### D-2: E2-FIX corrective change
Blocked until D1/D2 produce a live reproduction. Tracked as
`E2-REPRODUCTION`.

### D-3: Any new CERTIFIED_BUT_MISSING state machine, per-commit invalidation
granularity, or replacement of `pull_by_branch`'s whole-repo
`invalidate_all_history` strategy
Not authorized — deliberate, already-reviewed, already-SAP-validated design
choice (discovery §E4, re-confirmed §6 of this document).

### D-4: E-HARDEN-STANDARD-FILE-COUPLING (new this pass)
The architecture question of why standard `zcl_abapgit_git_porcelain`
carries embedded, ORTEC-aware branching and a duplicate copy of the
`'Walk,'` retry logic (§7) is explicitly NOT resolved by this design.
Requires owner input before any change to that file is proposed.

### D-5: Diagnostic tiers D2/D3 of E2-DIAG
Gated behind D0/D1 proving insufficient (§3). Not authorized by default.

---

## 9. Outcome-preservation matrix (CR-10)

Mapped against the 7 bulleted scope items in
[variant_b_package_renumbering.md](../decisions/variant_b_package_renumbering.md)'s
"Package E scope" section, with the two multi-part items split out for a
9-row matrix so no original intended outcome can silently disappear via
slice renaming or deferral.

| # | Original intended outcome (source: renumbering decision) | Current status | Evidence | Owning slice |
| --- | --- | --- | --- | --- |
| 1 | One upload-pack capability discovery per complete snapshot materialization | SATISFIED_BY_EXISTING_CODE | SAT trace Phase A: exactly 1 capability HTTP call; `zcl_abapgit_ortec_cold_init` design, byte-identical to old INV-E-10 | Pinned by E4-VERIFY regression (extend if a gap is found) |
| 2 | Adaptive MATERIALIZE_BLOBS want-list sizing | SATISFIED_BY_EXISTING_CODE | `zcl_abapgit_ortec_cold_init.clas.abap` constants (500/50/1000/2x/16 MiB/25 MiB), reused unchanged by `ensure_available`'s bounded top-up | No new slice needed |
| 3 | Metadata-only final selected-tip blob verification | SATISFIED_BY_EXISTING_CODE — verified this pass by direct read: `zcl_abapgit_ortec_obj_store=>verify_ready_blobs` (line ~1448) `SELECT obj_sha1, obj_type` only, no payload column; called from `zcl_abapgit_ortec_cold_init` line ~498 | Direct source read this session | No new slice needed |
| 4 | Consumer repo-key and tip coherence after branch switching | PARTIALLY_VERIFIED — no reproduced defect found (E2-DIAG's evidence ladder is the mechanism to close the remaining uncertainty); the mismatch matrix in discovery §E2 covers all known scenario classes | discovery §E2 mismatch matrix + this pass's E2-DIAG design | E2-DIAG (D0/D1) |
| 5 | Prevention of normal `ENSURE_AVAILABLE` execution for a certified, unchanged snapshot | SATISFIED_BY_ARCHITECTURAL_SCOPING for the unfiltered/certified consumer class (never calls `ensure_available` at all — different repair mechanism entirely); NOT_APPLICABLE for the filtered consumer class, which was intentionally scoped OUT of full-graph certification by Package C (confirmed carve-out) — `ensure_available` reaching a filtered call is correct-by-design, not a violation of this outcome, but is not an *explicit guard* either. Documented honestly rather than claimed as a clean "prevented." | discovery §E4, this pass's §6 scenario 1 | No new slice; documented nuance only |
| 6 | CERTIFIED_BUT_MISSING classification | NOT_APPLICABLE for the 7 normally-reachable E4 scenarios (no unresolved defect); TRACKED_RESIDUAL_RISK for the out-of-band scenario 7 | §6 verdict | E4-VERIFY (documents the gap); no E4-FIX authorized |
| 7 | One bounded repair of CERTIFIED_BUT_MISSING | SATISFIED_BY_EXISTING_CODE for both normally-reachable mechanisms (filtered top-up, unfiltered whole-repo invalidate+retry), both already SAP-validated | discovery §E4, this pass's §6 dispatch evidence | E4-VERIFY pins both mechanisms |
| 8 | Regression coverage: cold branch switch + Stage / Stage-by-Transport | AUTHORIZED_NOW, test-only | §6 test matrix E4-D-01..04 | E4-VERIFY |
| 9 | Regression coverage: Diff / status calculation after cold branch switch | PARTIALLY COVERED — `zcl_abapgit_status_calc` itself is confirmed unmodified/correct by direct read (discovery §E2); a dedicated end-to-end regression exercising Diff/status specifically after a cold branch switch is not yet confirmed to exist — **fixed this pass (bootstrap consistency review, resolves correctness-review MINOR-2): owning slice is hard-assigned to E4-VERIFY, not E1-TEST** (post-branch-switch behavior is E4-VERIFY's remit; grep for an existing test first before adding one) | discovery §E2 code-level confirmation; test-existence NOT yet confirmed | E4-VERIFY (add a status-calc-after-cold-switch fixture if none exists — implementation-time action item, now a hard requirement of this slice, not an either/or) |

No original intended outcome from the owner decision has been dropped by
renaming or deferral; outcome 9 surfaces one genuine, previously-unflagged
test-coverage gap that must be closed as part of implementing E4-VERIFY
(not deferred to Package F, not left ambiguous between two slices).

---

## 10. Design completion matrix

| ID | Root-cause status | Owner slice | Symbols | Invariant(s) | Tests | Perf test | SAP scenario | Evidence | Rollback boundary |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| E1 correctness | CONFIRMED_CURRENT (not a defect) | E1-TEST | `zcl_abapgit_ortec_obj_index` tests | INV-E1-T-1/2 | E1-T-01..04 | N/A | None required | discovery §E1 | Revert test file only |
| E1 performance | CORRECT_BUT_PERFORMANCE_OPEN | E1-PERF | `zcl_abapgit_ortec_obj_index` chunk constant | new (chunk-size regression) | new (chunk-boundary tests) | REQUIRED post-implementation SAT remeasurement | Same warm-to-cold repro | this doc §2 | Revert constant + tests |
| E2 | NOT_VERIFIED | E2-DIAG (D0/D1) | `zcl_abapgit_ortec_obj_index=>verify_one_row` (new, read-only) | INV-E2-D-1/3/4 | E2-D-01/02 | N/A | Live reproduction (owner-scheduled) | discovery §E2, this doc §3 | Revert new method + tests, no data migration |
| E3 | CONFIRMED_CURRENT (not a defect) | E3-TEST | `zcl_abapgit_ortec_cache_admin` tests | INV-E3-T-1/2/3 | E3-T-01..04 | N/A | None required | discovery §E3 | Revert test file only |
| E4 | E4_NOT_REQUIRED_CURRENTLY_COMPLETE (7/9 scenarios); TRACKED_RESIDUAL_RISK (scenario 7); OWNED_BY_E2 (scenario 8) | E4-VERIFY | `zcl_abapgit_ortec_porcelain`/`zcl_abapgit_ortec_missing_obj` tests | INV-E4-D-1/2/3 | E4-D-01..04 | N/A | None required | discovery §E4, this doc §6 | Revert test files only |
| OF-2 | DECIDED | E-HARDEN | `zcl_abapgit_ortec_porcelain`, `zcl_abapgit_ortec_git_switch` (new constant) | new | E-HARDEN-01/02 | N/A | None required | this doc §7 | Revert constant + call-site references + test |
| OF-3 | ALREADY_ADEQUATELY_MITIGATED | E-HARDEN | none (confirmation only) | none new | E-HARDEN-03 (existing) | N/A | None required | this doc §7 | N/A — no change |

## 11. Package F cleanup candidates surfaced by this design

- The `'Walk,'` string-match trigger, once extracted to a shared constant
  in the ORTEC-owned file (E-HARDEN), remains a candidate for a future
  typed-exception refactor — still NOT authorized here (would touch
  standard abapGit's exception class).
- `E-HARDEN-STANDARD-FILE-COUPLING` (D-4, §8): whether
  `zcl_abapgit_git_porcelain.clas.abap`'s embedded ORTEC branching/legacy
  duplicate should be refactored into a single clean hook is a new,
  concrete Package F candidate surfaced by this corrective pass — requires
  owner input before scoping.
- E1-TREE-REUSE / E1-E (§2, §8) remain the largest evidenced future
  performance opportunity, gated on a safe `.abapgit`-aware key design.

## 12. Required review gates

Per the mandatory implementation flow: focused discovery (complete),
correctness review, protocol/persistence review, performance DESIGN_GATE
(all re-run against this corrective design, see the three review artifacts),
senior/junior implementation split per slice risk, static performance scan,
performance IMPLEMENTATION_AUDIT (required specifically for E1-PERF post-
implementation), regression validation. Package F must not start until
Package E is live-validated.