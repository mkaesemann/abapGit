# E1-TREE-REUSE — Performance design gate (real run)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=E1-TREE-REUSE-PERFORMANCE-DESIGN-GATE-V1
MODE=DESIGN_GATE
BASELINE=36839c7faa55b4568c1724cf6232468957f6aac8
REVIEW_MODE=ortec-abapgit-performance-review
STATUS=RUN_COMPLETE (supersedes the prior BLOCKED_UPSTREAM/NOT_RUN stub)
VERDICT=APPROVE
```

This gate runs against the CYCLE=5 design, which reached adversarial
`VERDICT=APPROVE` (all of AR-1-1..AR-1-5, AR-2-1, AR-2-2, AR-3-1, AR-4-1
CLOSED). Every load-bearing claim below was independently re-verified
against the current productive source, not merely accepted from the
design/adversarial artifacts.

## Scope compliance

```text
ALLOWED_CONTEXT_READ=YES (design log CYCLE=5 full; adversarial review all
  5 cycles full; discovery log full; D2 SAT incident full; abap-performance-
  patterns SKILL full; git-partial-clone SKILL read per mode instructions)
PRODUCTIVE_SOURCE_WRITES=NO
STATE_WRITE=NO
DIAGRAM_WRITE=NO
OUTPUT_ARTIFACT=.memory/reviews/variant_b_package_e_e1_tree_reuse_performance.md
  (this file; the ONLY file written this run)
```

## Evidence matrix (independent re-verification, this pass)

```text
PR-SRC-1=src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap read in full
  (definition + all 6 method bodies). Confirms verbatim: c_index_write_
  chunk_size = 30000; rebuild_index contains NO COMMIT WORK / ROLLBACK WORK
  anywhere; lv_lock_id captured from acquire_repo_lock and passed to
  release_repo_lock on the early-ready RETURN, the success path, the
  zcx_abapgit_exception CATCH, and the cx_root CATCH (four exit paths, all
  releasing the SAME captured id); DIR branch does
  `CONCATENATE <ls_work>-path <ls_node>-name '/' INTO lv_next_path` with NO
  length guard before enqueue (AR-1-3's premise is factually current); FILE
  branch guards `strlen( path ) > 255 OR strlen( name ) > 255` AFTER the
  file_to_object call and the empty-obj_type/obj_name check; lt_seen_trees
  is a HASHED TABLE keyed by tree_sha1 only, checked/inserted before
  enqueueing a DIR child; MODIFY zaog_obj_index FROM TABLE lt_rows flushes
  at `lines( lt_rows ) >= c_index_write_chunk_size`; the $IDX/__READY__
  marker MODIFY is unconditional and strictly last before lock release.
PR-SRC-2=src/ortec/git/zaog_obj_index.tabl.xml read (DD02V/DD09L/key DD03P
  rows). Confirms CLIDEP=X, BUFALLOW=N, PK = CLIENT+REPO_KEY+COMMIT_SHA1+
  OBJ_TYPE+OBJ_NAME+PATH_HASH, FILE_PATH/FILE_NAME CHAR255 — matches every
  design citation of this table; no secondary index defined (consistent
  with the design's "no change to zaog_obj_index" additive-only claim).
PR-SRC-3=ZCL_ABAPGIT_ORTEC_PACK_RAW=>acquire_repo_lock/release_repo_lock
  method bodies read directly (not merely cited from the adversarial
  ledger). CONFIRMS: acquire is `DO iv_max_attempts TIMES` with ONE
  `INSERT zaog_fetch_sess` (single-row, session_id = |LOCK_{ iv_repo_key }|)
  per attempt, capped exponential backoff + jitter, `WAIT UP TO` capped at
  2 seconds/attempt, RAISE on exhaustion; release is a single guarded
  `DELETE ... WHERE session_id = iv_lock_id AND status = 'L'`, idempotent
  for an initial/absent id. Both are O(1) SQL (one row, one statement per
  attempt), never a scan, never per-object — the shared mutex primitive
  itself introduces no scalability risk for either rebuild_index or the
  new TR5 clear_tree_memo caller.
PR-DESIGN-1=.memory/logs/variant_b_package_e_e1_tree_reuse_design.md
  CYCLE=5 read in full (§0-§14, all five cycles' revision-response blocks).
PR-ADV-1=.memory/reviews/variant_b_package_e_e1_tree_reuse_adversarial.md
  read in full (cycles 1-5). All 9 findings (AR-1-1..AR-1-5, AR-2-1, AR-2-2,
  AR-3-1, AR-4-1) verified CLOSED in the cycle-5 ledger; cycle-5's own
  E5-SRC-* source citations for pack_raw/obj_index/cache_admin independently
  re-confirmed by PR-SRC-1/PR-SRC-3 above (not merely trusted).
PR-DISC-1=.memory/logs/variant_b_package_e_e1_tree_reuse_discovery.md read
  in full — confirms the call path (get_files_for_filter -> ensure_index ->
  rebuild_index), the existing lock/crash-safety shape, and the measured
  chunk=1000 baseline used as the estimate's basis.
PR-SAT-1=.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md read in
  full — confirms MEASURED chunk=1000 @ ~42,000 rows: Phase I (write) 9.39s
  / 41 MODIFY / 82 DB:Exec; decode_tree 0.97s/340 trees; traversal-compute
  residual ~4-5s; write cost is round-trip-count-driven, not byte-volume-
  driven (incident's own §9 conclusion) — this is the exact number the
  design's ESTIMATE is built from and correctly labels as chunk=1000-only,
  not a chunk=30000 measurement.
PR-SKILL-1=.github/skills/abap-performance-patterns/SKILL.md read in full
  (all 15 sections) — used as the normative checklist below.
PR-SKILL-2=.github/skills/git-partial-clone/SKILL.md read (Variant B
  scaling rules) — not directly load-bearing for this ORTEC-index-only
  slice (no fetch/materialization change), confirms no deepen/shallow or
  per-object-HTTP pattern is introduced, consistent with the design's own
  "ZERO new HTTP" claim.
```

## Design analysis — estimated behavior by cardinality

```text
1 object          : one BFS level, one tree. Reuse adds exactly one header
                     FAE (miss) or one header FAE + one small-child FAE
                     (hit) beyond today's single get_objects + one MODIFY.
                     O(1) added round trips. No behavior change of concern.

1,000 rows        : a handful of BFS levels, small tree/child counts well
                     under both c_tree_lookup_chunk_size (500) and
                     c_tree_child_chunk_size (30000). Every FAE/window stays
                     single-shot. Negligible added overhead either
                     direction (hit or full miss).

40,000-ish rows    : matches the MEASURED real evidence shape (~42,000
  (42k)              rows / ~340 trees / BFS depth ~5-15, chunk=1000
                     baseline 9.39s write + ~4-5s compute residual). Under
                     reuse: SQL-call count per level stays O(1) (1 header
                     FAE per <=500-key driver chunk, plus <=1-2 small-child
                     FAEs per level in the realistic case, since 340 trees
                     is well under both chunk constants); MISS-only
                     get_objects fetch shrinks with hit rate; total index
                     WRITE volume to zaog_obj_index is UNCHANGED (rows are
                     copy-not-alias, reproduced on every build regardless
                     of hit/miss) — the design is explicit and correct that
                     this change targets the COMPUTE residual only, not the
                     write-dominated cost this incident measured. No SQL
                     statement in this design scales with node count at
                     this scale.

1,000,000 rows     : worst case explicitly modeled as EITHER one huge
  (or 1 flat dir of   commit's total node count OR one pathological flat
   1,000,000 files)   directory of 1,000,000 files. Peak resident child
                     rows for ANY one tree is bounded to <=
                     c_tree_child_chunk_size (30000) by construction:
                     - HIT + SMALL (child_count<=30000): peek-then-decide
                       batching (AR-2-2) closes a FAE strictly BEFORE the
                       running child_count sum would exceed 30000 — proven
                       by construction (every SMALL tree's own child_count
                       is <=30000, so an empty batch always accepts the
                       next tree; no starvation, no >1x-chunk case).
                     - HIT + LARGE (child_count>30000, e.g. the 1,000,000-
                       file flat dir): read_large_child_page issues
                       ceil(1,000,000/30000)=34 CHILD_SEQ-windowed SELECTs
                       against the table's own trailing PK field (WHERE
                       tree_sha1=... AND child_seq BETWEEN f AND
                       f+29999 ORDER BY PRIMARY KEY), each bounded to
                       <=30000 resident rows, reproduced+flushed to
                       zaog_obj_index and discarded before the next window.
                       No single SELECT ever returns the whole flat
                       directory.
                     - MISS: unchanged from today — one bulk get_objects
                       fetch of the tree object, one decode_tree, then the
                       EXISTING node loop chunked at c_index_write_chunk_
                       size (30000) exactly as it does today for a
                       1,000,000-file flat directory MISS; the NEW memo
                       write path chunks zaog_tree_child at the SAME 30000
                       bound.
                     Peak memory = one 30000-row index chunk (~21.9MB,
                     unchanged) + at most one 30000-row child window/batch
                     for fixed-width CHAR/INT metadata rows (no XSTRING) +
                     small method-local scalars (dot_sha1/algo_ver/cutoff).
                     Storage grows O(nodes) but is deduplicated across
                     commits (content-addressed) and capped by the 7-day
                     TTL.
```

## Item-by-item verification (per the task's explicit checklist)

```text
CARDINALITY        : CONFIRMED. §9/§12 state 1k/42k/1M with BFS depth
                     ~5-15, matching PR-SAT-1's real measured shape.
HOT PATH            : CONFIRMED against source. get_files_for_filter ->
                     ensure_index -> rebuild_index BFS is the exact,
                     unchanged entry chain (PR-SRC-1); the design's
                     per-level HIT/MISS partition is inserted inside the
                     EXISTING WHILE lt_pending loop, inside the EXISTING
                     lock, with delete-first/marker-last preserved
                     byte-for-byte (§7, TR3 ANCHOR/ACTION=replace scoped to
                     only the per-level body).
SQL SHAPE           : CONFIRMED O(BFS depth), not O(nodes). Per level: 1
                     header FAE per <=500-key driver chunk (TTL predicate
                     folded into the SAME SELECT, no added round trip) +
                     <=1 small-child FAE per <=30000-row SMALL batch
                     (peek-then-decide) + <=(LARGE child_count/30000)
                     CHILD_SEQ window SELECTs for any oversized flat
                     directory + the EXISTING get_objects bulk fetch for
                     MISS-only trees + 2 memo MODIFYs (children-before-
                     header) + the EXISTING index MODIFY. No per-row
                     SELECT, no per-object SQL, no repository-wide scan
                     anywhere in this design.
AR-2-2 RE-VERIFY     : INDEPENDENTLY CONFIRMED SOUND. §9/§11-TR2 text:
  (SMALL batching)     "CLOSE the current batch and issue its FAE BEFORE
                     adding any tree whose CHILD_COUNT would push the
                     batch's running sum of CHILD_COUNT OVER
                     c_tree_child_chunk_size... OR when lines(batch)
                     reaches c_tree_lookup_chunk_size (whichever first)."
                     Reasoning verified independently (not just trusted
                     from the adversarial ledger): every SMALL tree has
                     child_count <= chunk by definition of the SMALL/LARGE
                     split, so an EMPTY batch (running sum = 0) can always
                     accept the next tree regardless of its own count —
                     no starvation case exists, and the batch is closed the
                     instant a further addition WOULD exceed the chunk, so
                     the strict bound is <= c_tree_child_chunk_size with NO
                     residual ~2x case. A full grep-equivalent read of every
                     operative §9/§11-TR2/§11-TR3/§12/§13 occurrence of this
                     rule found no remaining "~2x"/"one additional small
                     tree's worth" wording — only historical/superseded
                     text describing the ORIGINAL cycle-2 defect. SOUND.
AR-1-2 RE-VERIFY     : INDEPENDENTLY CONFIRMED SOUND. LARGE-tree reads use
  (CHILD_SEQ            `child_seq BETWEEN lv_from AND lv_from + c_tree_
   windowing)           child_chunk_size - 1 ORDER BY PRIMARY KEY`, and
                     §5.2's own PK definition places CHILD_SEQ as the
                     TRAILING key component specifically to make this a
                     genuine primary-key range scan (not a sort/filter over
                     an unbounded read) — verified this is architecturally
                     sound (a single-tree-scoped WHERE plus a PK-suffix
                     range predicate is an efficient, bounded access path,
                     not a full scan). This closes a 1,000,000-child flat
                     directory to 34 bounded window reads, matching the
                     cardinality analysis above.
OVERSIZED OBJECT     : CONFIRMED. AR-1-3's DIR >255 guard is applied
  (DIR >255 guard)     identically in the fresh MISS walk, HIT
                     reproduction, and (as a deliberate, disclosed,
                     row-output-identical amendment to CURRENT behavior)
                     the existing unmemoized path — verified the CURRENT
                     source (PR-SRC-1) indeed has NO such guard on the DIR
                     branch today, confirming AR-1-3's premise is still
                     factually true and the fix is still needed/correctly
                     scoped.
CACHE SCOPE / TTL    : CONFIRMED explicit and bounded. dot_sha1/algo_ver
                     are computed ONCE per rebuild_index call, held in
                     method-local variables — no cross-call in-memory
                     cache is introduced; the ONLY persistent cache is the
                     two DB tables, scoped by the full (repo_key, dot_sha1,
                     devclass, algo_ver, tree_sha1, tree_path_hash) key.
                     TTL QUESTION ANSWERED: the `built_at >= lv_cutoff`
                     predicate is folded into the SAME header-eligibility
                     SELECT that must already run for the reuse check —
                     it adds a WHERE-clause condition, NOT a new SQL
                     statement, NOT a per-row check, and NOT a new
                     complexity class. It does not change the O(BFS depth)
                     shape in any way; a TTL-expired header simply fails to
                     appear in the SAME result set, which the design
                     already treats identically to any other MISS (C11).
TRANSACTION OWNER /   CONFIRMED SAFE. Re-verified directly against source
  TR1-TR4 vs TR5      (PR-SRC-1): rebuild_index has NO COMMIT WORK / no
  COMMIT split         ROLLBACK WORK anywhere today, and the design's TR3
                     packet is scoped (ANCHOR/ACTION=replace) to touch only
                     the per-level BFS body, leaving lock/commit handling
                     untouched — TR1-TR4 genuinely introduce zero new
                     commit points. TR5 (clear_tree_memo) is a NEW,
                     standalone, operator-invoked admin action in
                     zcl_abapgit_ortec_cache_admin — the SAME class and
                     SAME category as the ALREADY-VALIDATED clear_repo
                     (Package E3, SAP_VALIDATED_COMPLETE), which itself
                     already commits on success and rolls back+releases on
                     error. TR5's two commit points (success: release-then-
                     commit; error: rollback-then-release-then-commit,
                     AR-4-1) are therefore consistent with an established,
                     already-accepted precedent for this exact class of
                     action, not a novel pattern. This is explicitly NOT a
                     hot path: it is a manually-invoked, low-frequency
                     admin action performing two SET-BASED DELETEs (not a
                     per-object or per-row commit), so SKILL §13's "no
                     COMMIT WORK per object or per small batch" rule is not
                     violated — one commit per ADMIN INVOCATION, covering a
                     set-based operation, is exactly the pattern the skill
                     permits for a documented crash-resume/admin protocol.
                     FRAMING CONFIRMED CORRECT, not a hidden violation.
ACCEPTANCE CRITERIA  : CONFIRMED present and concrete for 1k/42k/1M in both
  (1k/42k/1M)          §9's table and §12's bullet list, with explicit
                     peak-memory figures for each scale (matches mode
                     instructions' mandatory-scale-scenario requirement for
                     a DESIGN_GATE, noting these are DESIGN-TIME estimates;
                     live 42k/1M measurement is TR4's job, not this gate's).
ESTIMATE vs MEASURED : CONFIRMED honest throughout. §1 (Facts ledger), §4
  HONESTY / TR0        (DECISION text), §11/TR0 (STOP_IF), §12 (last
  BLOCK_OWNER_          acceptance bullet), and §13 (compliance checklist)
  DECISION             all independently and consistently label the
                     numeric benefit as a bounded ESTIMATE (elimination of
                     the MEASURED chunk=1000 ~4-5s traversal-compute
                     residual, scaled by an unmeasured unchanged-subtree
                     fraction) until a chunk=30000 SAT trace exists. TR0's
                     STOP_IF is explicit: "ALWAYS stop after reporting...
                     Do NOT decide proceed/park autonomously and do NOT
                     apply any fixed threshold" — this is a genuine
                     BLOCK_OWNER_DECISION, not a disguised autonomous gate;
                     the former "compute >= 30%" figure is retained only as
                     a labeled non-binding reference. This satisfies the
                     DESIGN_GATE bar: the design does not need the benefit
                     already measured, it needs to be honest that it is
                     not, and it is.
```

## Mandatory template sections (SKILL §1 checklist)

```text
[x] Expected production cardinality        — §9/§12 (1k/42k/1M, BFS depth ~5-15)
[x] Entry methods / complete hot path       — §7, TR3 ANCHOR; verified vs PR-SRC-1
[x] SQL statement shape                     — §9 exact statement shapes
[x] HTTP request shape                      — zero new HTTP, stated §9/§12/§13
[x] Row batch limit                         — c_tree_child_chunk_size=30000,
                                               c_tree_lookup_chunk_size=500
[x] Byte batch limit                        — no explicit separate byte constant,
                                               but rows are fixed-width CHAR/INT
                                               metadata with NO XSTRING column,
                                               which SKILL §4 explicitly exempts
                                               from a mandatory separate byte
                                               budget ("...or when the rows
                                               contain metadata without payload
                                               XSTRINGs"). Not a gap.
[x] Oversized single-object behavior        — LARGE CHILD_SEQ windowing (AR-1-2);
                                               DIR >255 guard (AR-1-3)
[x] Internal-table lookup structures        — lt_seen_trees HASHED O(1), unchanged;
                                               driver tables bounded by chunk consts
[x] Cache scope and invalidation            — method-local ctx; DB memo scoped by
                                               full key; TTL + set-based admin clear
[x] Transaction owner / publication boundary — TR1-TR4 caller LUW unchanged; TR5
                                               isolated admin LUW, justified
[x] Max simultaneous payload/XSTRING copies — none new; no blob load in this path
[x] Medium/large acceptance scenarios       — 1k/42k/1M in §9/§12
```

All twelve mandatory DESIGN_GATE input categories are present, concrete, and
independently verifiable against current source — none are invented or
left as "as appropriate"/TBD (a full-document sanity pass in adversarial
cycle 3 already confirmed no placeholder language remains, and this pass
found none either while reading §§1-14 directly).

## Findings

No BLOCKER, MAJOR, or MINOR performance findings were identified this pass.
The 9 adversarial findings (AR-1-1..AR-1-5, AR-2-1, AR-2-2, AR-3-1, AR-4-1)
already cover every performance-relevant angle a DESIGN_GATE would
independently raise (batch bounds, lock/mutex safety and reuse, TTL-bounded
staleness, oversized-object handling, cardinality-integrity checks, commit
boundary correctness) and were closed with source-verified fixes across 5
cycles; this pass's independent re-verification against the actual current
productive source (PR-SRC-1/2/3) found no discrepancy between what the
design claims and what the source actually does, and no new performance
concern in the corrected cycle-5 text.

One non-blocking observation, not rising to a finding: the design does not
carry an explicit standalone "0%-hit-rate / first-ever-build overhead"
number distinct from the hit-rate benefit estimate. This is not a gap
requiring revision — TR4's own validation step explicitly plans a
"run 1 (cold memo)" measurement (full walk + memoize, i.e. the worst-case
0%-hit path) alongside "run 2 (warm memo)", so the downside/overhead
question is already scheduled to be measured before any claim of net
benefit is finalized, consistent with the ESTIMATE-vs-MEASURED discipline
enforced throughout the rest of the design.

## Verdict

```text
VERDICT=APPROVE
RATIONALE=All mandatory DESIGN_GATE inputs are present and concrete. The
  SQL/HTTP/batch/memory model is genuinely O(BFS depth), not O(nodes), at
  every cardinality point evaluated (1/1,000/40,000/1,000,000), independently
  re-verified against current source rather than accepted from the design or
  adversarial artifacts alone. AR-2-2's peek-then-decide SMALL-batch bound
  and AR-1-2's CHILD_SEQ LARGE-window bound both hold up under independent
  re-derivation (no ~2x residual, no unbounded flat-directory read). The
  TR1-TR4 vs TR5 commit-ownership split is performance-safe: TR1-TR4 add no
  new commit point (verified: current rebuild_index has none), and TR5's two
  commit points are an isolated, low-frequency, set-based admin action
  consistent with the already-validated clear_repo precedent — not a hidden
  "commit per object" violation. The TTL check is a WHERE-predicate addition
  to an already-required SELECT, not a new SQL call or complexity-class
  change. The design honestly separates its ESTIMATED benefit from a
  MEASURED one and correctly gates Stage-0/TR0 as BLOCK_OWNER_DECISION with
  no autonomous proceed/park threshold. No BLOCKER/MAJOR/MINOR performance
  finding is raised. This gate approves the DESIGN's performance shape and
  honesty; it does NOT authorize implementation — TR0 (measurement) and the
  separate owner go/no-go on TR1-TR5 remain the design's own stated
  prerequisites, per §11/TR0, §13-AR-1-4, and §14.
BLOCKING_FIXES=none.
```

## Next action

Implementation remains gated exactly as the design itself states: TR0
(Stage-0 chunk=30000 SAT measurement, a BLOCK_OWNER_DECISION that measures
and reports the write:compute split without deciding proceed/park) must run
first, and TR1-TR5 require a separate, explicit owner go/no-go on that
reported number. This performance DESIGN_GATE does not change
`PACKAGE_E_STATUS`'s POSTPONED posture for `E1_OBJINDEX_PERFORMANCE` in
`.memory/state.md` — that remains an orchestrator-owned decision outside
this gate's scope and this file's write authority.
