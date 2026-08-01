# Package E — E1-TREE-REUSE — bootstrap handoff

```text
RUN=PACKAGE_E_E1_TREE_REUSE_DESIGN (design-only convergent-review run)
DATE=2026-07-31
BASELINE=36839c7faa55b4568c1724cf6232468957f6aac8
OWNER_PROMPT="Package E — E1-TREE-REUSE Convergent Design and
  Implementation Specification" (2026-07-31, explicit, this session), plus
  an explicit owner extension "I allow another 3 cycles to address the
  flaws" after the original 3-cycle cap ended in BLOCK_OWNER_DECISION.
PRODUCTIVE_CHANGES=NONE. STATE_CHANGED=NO. COMMIT_CREATED=NO. PUSHED=NO.
IMPLEMENTATION_AUTHORIZED=NO (design is fully converged and gate-approved,
  but a SEPARATE owner go/no-go on the TR0 Stage-0 measurement result is
  still required before any code — this was never in scope for this run,
  which was explicitly design-only throughout).
```

## 0. Postponement reconciliation (read this first if resuming)

`.memory/state.md` records a same-day owner decision postponing
`E1_OBJINDEX_PERFORMANCE` work generally. The owner's explicit prompt for
this run was treated as satisfying the state.md-documented `E1-TREE-REUSE`
re-entry condition itself ("owner approves an additive secondary-index
DDIC change and a dedicated design + performance DESIGN_GATE for it") —
design-only, zero productive/state/commit changes throughout, fully
reversible. `state.md` was deliberately NOT updated by this run; a human
decision is still needed on whether/how to fold this outcome into
`state.md`'s Package E tracking.

## 1. Convergence history (what changed across cycles)

```text
Cycle 1 draft -> adversarial cycle 1: 1 BLOCKER + 4 MAJOR
  (AR-1-1 mutable-SAP-state staleness with no version-stamp signal;
   AR-1-2 unbounded child SELECT for one huge flat directory;
   AR-1-3 DIR names >255 chars not handled by the memo;
   AR-1-4 an unapproved "compute >=30%" autonomous threshold;
   AR-1-5 an overstated C10 integrity-check claim)
Cycle 2 revision -> adversarial cycle 2: all 5 CLOSED; 2 NEW MAJOR found
  (AR-2-1 the new admin memo-clear action used a DIFFERENT lock primitive
   than rebuild_index and could delete rebuild_index's own mutex row;
   AR-2-2 a peak-memory contradiction — SMALL-hit batching could admit
   ~2x the claimed one-chunk bound)
Cycle 3 revision -> adversarial cycle 3 (ORIGINAL 3-cycle cap, final
  automatic cycle): AR-2-2 confirmed FIXED; AR-2-1's fix was itself found
  DEFECTIVE — NEW BLOCKER AR-3-1 (the corrected TR5 packet released the
  lock using the bare repo key instead of the captured lock id, so a
  successful clear would leak the mutex row). VERDICT=BLOCK_OWNER_DECISION
  per the mandatory cap — run paused here, owner asked to decide.
--- owner explicitly granted "another 3 cycles to address the flaws" ---
Cycle 4 revision -> adversarial cycle 4 (extension cycle 1 of 3): AR-3-1's
  success path confirmed fixed, but its error path (a bare `ROLLBACK WORK`
  with no explicit release, justified by an unproven "no intervening
  commit can occur" assumption) was found insufficient — NEW MAJOR AR-4-1,
  with a concrete required change (explicit release_repo_lock + its own
  COMMIT WORK AND WAIT on the error path too).
Cycle 5 revision -> adversarial cycle 5 (extension cycle 2 of 3, used 2 of
  the 3 granted): AR-4-1 CLOSED — TR5's error path now explicitly calls
  release_repo_lock(lv_lock_id) then COMMIT WORK AND WAIT, justified on
  its own terms (clear_tree_memo owns its own admin LUW, unlike
  rebuild_index) rather than by a false "matches rebuild_index exactly"
  claim. VERDICT=APPROVE — 0 open BLOCKER/MAJOR, all 9 findings across 5
  cycles CLOSED.
```

## 2. Post-approval independent gates (all APPROVE)

```text
CORRECTNESS_GATE=APPROVE (High confidence, 1 MINOR documentation-only note)
  .memory/reviews/variant_b_package_e_e1_tree_reuse_correctness.md
PROTOCOL_PERSISTENCE_GATE=APPROVE (0 findings)
  .memory/reviews/variant_b_package_e_e1_tree_reuse_protocol.md
PERFORMANCE_DESIGN_GATE=APPROVE (0 findings; confirmed O(BFS depth) SQL,
  zero new HTTP, strict <= one-chunk child-row bound, honest ESTIMATE vs
  MEASURED benefit framing, TR0 correctly a BLOCK_OWNER_DECISION gate not
  an autonomous threshold)
  .memory/reviews/variant_b_package_e_e1_tree_reuse_performance.md
```

All three gate reviewers were instructed to, and did, independently
re-verify the highest-risk closures (AR-1-1's TTL/admin-clear mechanism,
AR-4-1's final TR5 lock handling) against current source themselves,
rather than trusting the adversarial ledger's labels alone.

## 3. Final design summary

```text
CHOSEN_ARCHITECTURE=Alternative B — per-tree content-addressed
  resolved-child memo (2 additive tables zaog_tree_map/zaog_tree_child),
  integrated into rebuild_index's BFS; index rows stay commit-scoped
  copies, never aliased across commits.
REUSE_IDENTITY=(repo_key, dot_sha1=io_dot->get_signature()-sha1, devclass,
  algo_ver) + tree_sha1 + tree_path_hash, map_status='R', within a TTL
  cutoff (c_tree_memo_max_age_secs).
DDIC_IMPACT=2 new additive transparent tables only; zero change to
  zaog_obj_index/zaog_obj_store.
NEW_ADMIN_ACTION=clear_tree_memo on zcl_abapgit_ortec_cache_admin (TR5),
  using the SAME LOCK_<repo_key> mutex as rebuild_index, never touching
  zaog_fetch_sess rows by repo_key wholesale, with symmetric explicit
  lock-release + durable commit on BOTH its success and error paths.
SLICES=TR0 (owner-decision Stage-0 SAT measurement, no code) -> TR1
  (context fingerprint) -> TR2 (DDIC + memo persistence) -> TR3 (BFS
  integration) -> TR4 (integration/migration/concurrency/SAT validation)
  -> TR5 (admin memo-clear action).
```

## 4. Next action (requires a SEPARATE explicit owner decision)

The design is now fully converged and gate-approved. Per the design's own
Stage-0 gate (closing AR-1-4), and per this run's own instruction to
implement nothing:

1. TR0 (a live SAT measurement at the current chunk=30000 config,
   reproducing the O4H-8794 warm-to-cold scenario) is the next concrete
   step, but it requires the owner's explicit authorization to execute
   (it is a live-system measurement action, not a design task, and this
   run's mandate was design-only throughout).
2. TR0's reported write:compute split then requires a SEPARATE owner
   go/no-go before TR1-TR5 may be implemented — no autonomous threshold
   decides this.
3. Separately, the owner should decide whether/how this run's outcome
   updates `.memory/state.md`'s Package E `E1-TREE-REUSE` deferred-topic
   entry (this run intentionally left `state.md` untouched throughout).

No implementation of any kind (TR0 measurement included) is authorized by
this run.
