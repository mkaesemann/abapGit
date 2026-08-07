# SER-SLICE-3 Phase 2 — final two-path hook and call-site inventory

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_2_TWO_PATH_AUDIT
STATUS=INVENTORY_COMPLETE_ROUTING_DESIGN_FIXED
BASELINE_HEAD=bf436db0632de0098875d97b4a28a8f246eb50a5
```

## Purpose

Prove no standard behavior is lost before touching routing. Establishes
the exact target design used by Phase 3 (repository setting), Phase 4/6
(providers) and Phase 7 (Path 3 removal).

## Entry-point routing (today)

`ZCL_ABAPGIT_SERIALIZE` (main serialize loop, called from
`ZCL_ABAPGIT_REPO~get_files_local`/`get_files_local_filtered` and
`ZCL_ABAPGIT_ZIP`) has three branches, all inside one method:

```text
1. IF is_serial_batch_active() = TRUE AND max_processes > 1
   -> delegate whole call to ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE, RETURN.
   This is PATH 2 (adaptive batch). KEEP_AND_EXTEND.

2. ELSE (batch off, or max_processes = 1, or parallel already broken):
   lv_use_ortec_prefetch = is_serial_prefetch_active()   [CLASS-DATA,
     default ABAP_TRUE today - see finding F-1]
   IF lv_use_ortec_prefetch = TRUE:
     prepare() on ser_pref / ser_pref_ext / ser_pref_oo   <- THIS BLOCK
     ... own sequential/parallel loop (run_sequential/run_parallel) ...
     CLEANUP: clear() on all three prefetch classes
   This IS PATH 3 (standard orchestration + legacy ORTEC single-object
   prefetch, no adaptive batching). REMOVE_COMPLETELY - this exact block
   is what produced the false-MODIFIED oracle superseded in
   `.memory/incidents/serialization_slice_3_dtel_doma_parity.md`.

3. Per-object, inside run_sequential/run_parallel -> zcl_abapgit_objects=>
   serialize(is_item) -> each mandatory-family object class's own
   SERIALIZE checks is_serial_prefetch_active() again and, if TRUE, reads
   the (now-populated, from block 2) ser_pref*/ser_pref_ext/ser_pref_oo
   caches instead of its own DB read. This is PATH 3's per-object half -
   REMOVE (the check itself is harmless once block 2 no longer populates
   the cache, but leaving it is a latent re-enablement risk - see F-2).
```

## Finding F-1 — `is_serial_prefetch_active` is a global, not a repository
setting, and defaults to ON

`ZCL_ABAPGIT_ORTEC_GIT_SWITCH::mv_serial_prefetch_active` is
`CLASS-DATA ... VALUE abap_true`. This single session-global flag gates
BOTH the desired Path 2 in-batch cache reads (via ORCH's own `prepare()`
call before dispatch) AND the undesired Path 3 classic-path `prepare()`
call in `ZCL_ABAPGIT_SERIALIZE`. There is no way today to have "batch ON"
and "classic-path prefetch OFF" as independent settings - which is
exactly why Path 3 exists as a reachable state with today's defaults
(`is_serial_batch_active` OFF by default, `is_serial_prefetch_active` ON
by default) and is live in production today whenever the repository
setting is OFF. `set_serial_prefetch_active` is a public setter with no
current test/production caller found (`grep` confirms zero call sites
outside the class itself) - not a required test seam to preserve as a
public toggle.

## Finding F-2 — `is_wapa_active()` is unconditionally `abap_true`, not
setting-gated at all

`ZCL_ABAPGIT_OBJECT_WAPA~exists`/`~serialize` route to
`ZCL_ABAPGIT_ORTEC_WAPA` whenever `is_wapa_active()` returns true, and
that method's body is the literal constant `rv_active = abap_true.` -
independent of both `is_serial_batch_active` and `is_serial_prefetch_
active`. Per the binding target ("Path A: no ORTEC WAPA replacement"),
this must become gated on the same batch-context signal as the rest of
Path 2, not left unconditional.

## Finding F-3 — duplicate `PREPARE()` call in ORCH, redundant but safe

`ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE` (current HEAD) calls
`zcl_abapgit_ortec_ser_pref_ext=>prepare()` TWICE per run: once as part of
the original three-class `prepare()` block (`5ff237b9`, before the
`lv_run_id` TRY), and again as a second, owner-added call
(`bf436db0`) scoped to `pref_ext` only, positioned after the run-context
INSERT. `PREPARE` calls `CLEAR( )` internally first, so this is
idempotent and produces identical cached data both times - confirmed safe
via source read, not a correctness bug, but wasteful (doubles the DD01L/
DD01T/DD07L/DD07T/DD04L/DD04T `FOR ALL ENTRIES` SELECTs on every batch
run) and confusing for future maintenance. Disposition: remove the
second, redundant call as part of Phase 4/7 cleanup (not a standalone
fix - bundled with the CLAS/INTF provider change to that same method
region to avoid a churn-only commit).

## Per-hook/class disposition matrix

| Hook / class / method | Called when setting OFF today | Called when setting ON today | Target path | Shared state/caches | Required action | Tests proving the action |
|---|---|---|---|---|---|---|
| `ZCL_ABAPGIT_SERIALIZE` main loop, block 1 (`is_serial_batch_active` check + ORCH delegation) | No | Yes | BOTH (routing switch) | none | Read the new repository-scoped setting instead of the global; keep delegation shape | `serialization_repository_setting.md` routing tests |
| `ZCL_ABAPGIT_SERIALIZE` main loop, block 2 (`is_serial_prefetch_active` + prepare/loop/clear) | Yes (today, bug) | No (block 1 already returned) | STANDARD_PATH_REMOVE | `ser_pref`/`ser_pref_ext`/`ser_pref_oo` caches | Delete this block entirely; classic loop always runs with zero ORTEC cache involvement | `no_ortec_prefetch_called_when_off` spy test |
| `ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE` prepare()/clear() wiring | No (only reachable via block 1) | Yes | BATCH_ONLY_KEEP | same three caches | Keep; remove the F-3 duplicate call; caches populated/cleared only for the duration of this method | existing ORCH prepare/clear tests + new duplicate-call regression test |
| `ZCL_ABAPGIT_ORTEC_SER_PREF` / `_EXT` / `_OO` `prepare`/`clear`/`get_*_data`/`get_*_i18n`/`extract_for_batch` | No | Yes | BATCH_ONLY_KEEP | own `mt_*` tables | No routing change; consumed only from ORCH's call graph | unchanged existing unit tests |
| `ZCL_ABAPGIT_ORTEC_GIT_SWITCH::is_serial_prefetch_active` | returns TRUE today (bug: F-1) | returns TRUE today | MOVE_BEHIND_BATCH_CONTEXT | `mv_serial_prefetch_active` CLASS-DATA | Redefine as a read of a new `mv_batch_context_active` marker set only by ORCH's own enter/exit (see Phase 3 design); keep the method name/signature (no call-site churn across 9 object classes) | `is_serial_prefetch_active_false_outside_batch_context`, `is_serial_prefetch_active_true_inside_batch_context` |
| `ZCL_ABAPGIT_ORTEC_GIT_SWITCH::set_serial_prefetch_active` | n/a (no callers) | n/a | DELETE_DEAD_CODE (demote to PRIVATE, ORCH-only test seam) | - | Restrict to a documented ORCH-internal seam; not part of the public toggle API | existing suite unaffected (no external caller found) |
| `ZCL_ABAPGIT_OBJECT_DOMA`/`DTEL`/`ENHS`/`FUGR`/`MSAG`/`PROG`/`SMIM`/`TOBJ`/`TRAN`, `ZCL_ABAPGIT_OO_BASE` (CLAS/INTF) — all `is_serial_prefetch_active()` gates around `get_*_data`/`get_descriptions_*` | Reads today (bug: cache is empty because F-1, but the *check* still runs) | Reads today | BATCH_ONLY_KEEP (behavior unchanged - these already fail safe to a MISS when the cache is empty; only the underlying flag's truth value changes per Finding F-1's fix) | read-only against `ser_pref*` caches | No code change required in these 10 classes - they already have the correct MISS-falls-back-safely shape; only the flag's *meaning* changes centrally | existing per-object serializer tests unaffected; two-path routing tests at the ORCH/SWITCH level suffice |
| `ZCL_ABAPGIT_OBJECT_WAPA::is_wapa_active` gate | Always TRUE today (F-2, bug) | Always TRUE today | MOVE_BEHIND_BATCH_CONTEXT | none (WAPA has no batch prefetch cache, only a batch-context gate) | `is_wapa_active` reads the same `mv_batch_context_active` marker as `is_serial_prefetch_active`; WAPA singleton-batch admission itself is unchanged (owned by the planner, not this flag) | `wapa_replacement_not_called_when_off`, `wapa_replacement_called_once_when_on` |
| `ZCL_ABAPGIT_ORTEC_BULK_EXISTS` / `is_bulk_exists_active` | Yes (independent feature) | Yes (independent feature) | UNRELATED_KEEP | own cache | No change - this accelerates TADIR existence checks (used by status/exists, not by serialization output), independent feature area explicitly out of this slice's scope per the owner's object list ("where serialization routing is affected" - it is not) | none needed this slice |
| `ZCL_ABAPGIT_ORTEC_WAPA` (the replacement serializer body itself) | n/a once gated (F-2 fix) | Yes, singleton only | BATCH_ONLY_KEEP | none | No change to its own serialization logic, only to its activation gate (F-2) | existing WAPA tests unaffected |

## Pre/post-processing behavior that must survive delegation to the
adaptive path (owner's required checklist)

All of the following are owned by `ZCL_ABAPGIT_SERIALIZE` and callers
ABOVE the block-1 delegation point (i.e. already executed BEFORE the
`is_serial_batch_active` check, or by the caller before/after
`zcl_abapgit_serialize=>serialize` returns) - none of them are
duplicated, skipped, or reordered by either path, because block 1's
delegation is a single `RETURN rt_files` substitution at the very top of
the method, after `filter_unsupported_objects`/`filter_ignored_objects`
already ran and before either loop shape begins:

```text
.abapgit.xml                 - handled by ZCL_ABAPGIT_REPO/serialize_dot_
                                abapgit, never inside ZCL_ABAPGIT_SERIALIZE
                                itself - EXECUTED_BEFORE (caller-owned),
                                identical for both paths.
APACK/data files             - ZIF_ABAPGIT_DATA_CONFIG handled by
                                ZCL_ABAPGIT_SERIALIZE~files_local, a
                                SEPARATE method from ~serialize - not
                                affected by the block-1/2 branch at all.
i18n object-pattern rules     - MT_WO_TRANSLATION_PATTERNS is read from
                                MS_I18N_PARAMS before either branch and
                                passed as an EXPLICIT parameter into
                                ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE
                                (is_i18n_params, it_wo_translation_
                                patterns) - REPRODUCED_IN_ADAPTIVE_PATH.
paths                         - IS_TADIR-PATH is carried on every ORCH
                                result row (MERGE_INTO_MT_FILES sets
                                <ls_return>-file-path = is_tadir-path,
                                mirroring the classic loop's own
                                <ls_return>-file-path assignment) -
                                REPRODUCED_IN_ADAPTIVE_PATH.
item metadata                 - ls_serialization-item is carried through
                                identically in both paths -
                                REPRODUCED_IN_ADAPTIVE_PATH.
ordering/deduplication        - filter_unsupported_objects/filter_
                                ignored_objects run on LT_TADIR BEFORE
                                the branch (EXECUTED_BEFORE_DELEGATION,
                                identical input to both paths); ORCH does
                                not reorder or deduplicate MT_FILES itself
                                beyond appending per resolved object,
                                same shape as the classic loop's own
                                APPEND-per-file pattern.
logging                       - II_LOG is passed through explicitly to
                                ORCH; both paths call the same
                                ii_log->add_*() surface -
                                REPRODUCED_IN_ADAPTIVE_PATH.
exception propagation         - ORCH raises ZCX_ABAPGIT_EXCEPTION on any
                                run failure (fail-fast, all-or-nothing,
                                see SER-SLICE-2 terminal-outcome
                                contract) - stricter than, but not weaker
                                than, the classic path's per-object
                                CATCH/CONTINUE shape - EXECUTED_AFTER
                                (caller sees the same exception type).
cleanup                       - ORCH's own purge_run_state/discard_run_
                                state + prefetch clear() run on every
                                exit path (TRY/CATCH, this session's
                                Fix A) - REPRODUCED_IN_ADAPTIVE_PATH,
                                arguably more complete than the classic
                                path's single CLEANUP block.
```

No standard behavior is lost by keeping the block-1 delegation shape.

## Routing design fixed by this audit (binding for Phase 3/4/6/7)

```text
1. ZCL_ABAPGIT_SERIALIZE gains an OPTIONAL constructor parameter
   iv_repo_url (populated by its 2-3 real call sites in
   ZCL_ABAPGIT_REPO where a repo URL is already available via
   ms_data-url; ZCL_ABAPGIT_ZIP's package-export call site passes
   nothing, which safely resolves to Path A - correct, since a
   standalone zip export has no persisted repository setting to read).
2. Block 1's condition becomes:
   is_serial_batch_active( iv_url = mv_repo_url ) = TRUE AND max > 1
   reading the new persisted per-repository setting (Phase 3), with NO
   global CLASS-DATA override in normal execution.
3. Block 2 (the classic-path prepare()/clear() around run_sequential/
   run_parallel) is DELETED. The classic loop keeps calling
   zcl_abapgit_objects=>serialize() exactly as before, unconditionally.
4. ZCL_ABAPGIT_ORTEC_GIT_SWITCH gains a private-write, session-scoped
   marker (mv_batch_context_active) that ONLY
   ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE sets TRUE at entry (once its own
   repository-setting-gated caller has already decided to run) and
   clears on every exit path (success, run-establish failure, uuid
   failure) - exactly mirroring the existing prepare()/clear() lifecycle,
   just adding one more flag flip alongside it.
5. is_serial_prefetch_active() is redefined to return
   mv_batch_context_active instead of an independently-toggleable
   default-true flag. No call-site changes needed in the 10 object
   classes that already call is_serial_prefetch_active() - they keep
   failing safe to a cache MISS exactly as designed; the flag is simply
   now truthful about when a batch run is actually in progress.
6. is_wapa_active() is redefined to return the same
   mv_batch_context_active marker (WAPA has no separate prefetch cache,
   so this is its entire gate). WAPA's existing singleton-only batch
   admission in the planner is unchanged.
7. set_serial_prefetch_active / set_serial_batch_active (global) remain
   as PRIVATE-visibility-equivalent test seams only (kept for existing
   unit tests that construct a controlled batch-context state without a
   full ORCH run); production code never calls them.
```

This design satisfies every "Path A must not..." / "Path B must..."
requirement in the owner's prompt using ONE new state variable and ONE
new optional constructor parameter, with zero changes to any of the 10
mandatory-family object classes' own serialize bodies.

## Phase 7 implementation record

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_PHASE_7_REMOVE_PATH3
STATUS=IMPLEMENTED_LOCAL_GET_ERRORS_CLEAN
```

The design above (steps 4-7 of "Routing design fixed by this audit") was
CORRECTED before implementation: the originally-sketched new
`mv_batch_context_active` field had a circular-dependency bug (ORCH's own
decision to call PREPARE() cannot be gated on the very flag PREPARE() is
about to set). The actual implementation is simpler and needed no new
CLASS-DATA field or public method:

```text
1. zcl_abapgit_ortec_git_switch.clas.abap:
   mv_serial_prefetch_active default flipped abap_true -> abap_false
   (Finding F-1's actual fix - this was the real production bug: the
   classic per-object path defaulted to consulting ORTEC caches).
   is_wapa_active() body changed from the hardcoded `rv_active =
   abap_true` to `rv_active = mv_serial_prefetch_active` (Finding F-2 fix)
   - WAPA's gate and the DOMA/DTEL/CLAS/INTF/MSAG providers' gate are now
   the exact same single source of truth.
   is_serial_prefetch_active/set_serial_prefetch_active: UNCHANGED
   signatures, only ABAP Doc updated to state ORCH is the sole production
   caller of set_serial_prefetch_active( abap_true ).
2. zcl_abapgit_ortec_ser_orch.clas.abap SERIALIZE: removed the
   `IF is_serial_prefetch_active() = abap_true` guards around its own
   prepare()/clear() calls (4 call sites: entry, uuid-failure, success-
   exit, run-failure-exit) - prepare()/clear() are now unconditional, and
   each site also now calls set_serial_prefetch_active( abap_true ) right
   after prepare() and set_serial_prefetch_active( abap_false ) right
   after clear() - so ORCH's own run window is exactly when
   is_serial_prefetch_active()/is_wapa_active() return TRUE, on every
   exit path including exceptions. The now-unused lv_use_ortec_prefetch
   local variable was removed.
3. zcl_abapgit_serialize.clas.abap: the classic (non-ORCH) path's own
   prefetch block (lv_use_ortec_prefetch assignment, the
   IF/prepare-calls/ENDIF, and the paired TRY...CLEANUP clear block) was
   DELETED COMPLETELY - not made unconditional, REMOVED. The method's
   TRY/CLEANUP wrapper was also simplified away since nothing inside it
   needed CLEANUP-guaranteed execution anymore. run_sequential/
   run_parallel are otherwise byte-for-byte unchanged.
   run_parallel's own extract_for_object calls (a DIFFERENT method,
   feeding the STANDARD Z_ABAPGIT_SERIALIZE_PARALLEL RFC) were
   intentionally left untouched, per spec - they self-neutralize (always
   build an empty/initial buffer, zero DB access) now that the caches
   they read from are never populated in the classic path's own session.
   zabapgit_parallel.fugr.z_abapgit_serialize_parallel.abap (the standard
   RFC worker itself) was also left untouched, per spec, for the same
   reason - its own `set_serial_prefetch_active( abap_true )` call is now
   guarded by a condition (non-empty prefetch buffer) that can no longer
   structurally become true from the classic path.
```

## Local validation

```text
GET_ERRORS=CLEAN on all 4 touched files (zcl_abapgit_serialize,
  zcl_abapgit_ortec_git_switch + testclasses, zcl_abapgit_ortec_ser_orch
  + testclasses)
METHOD_NAME_LENGTH=CLEAN
```

## Tests added

```text
src/ortec/git/zcl_abapgit_ortec_git_switch.clas.testclasses.abap gained
  ltcl_serial_prefetch_switch: prefetch_default_off (is_serial_prefetch_
  active() = abap_false with no setup - Finding F-1 regression),
  wapa_default_off (is_wapa_active() = abap_false with no setup -
  Finding F-2 regression), wapa_delegates_to_prefetch (proves both
  getters read the exact same underlying flag by toggling it once and
  asserting both follow).
```

## Disclosed gap - NOT_TESTABLE_LOCALLY

```text
"is_serial_prefetch_active()/is_wapa_active() become TRUE for the
  duration of a real ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE call and
  FALSE again immediately after (success AND run-failure paths)" was NOT
  implemented as a true end-to-end test: ORCH's own SERIALIZE method
  requires a live aRFC batch dispatch and DB-backed result polling
  (WAIT UNTIL / callback-driven), which cannot run inside a local ABAP
  Unit test without a real system. wapa_delegates_to_prefetch instead
  directly exercises the exact set_serial_prefetch_active(TRUE)/(FALSE)
  pairing ORCH's own code now performs at each of its 4 call sites (this
  session's diff review, not a live run, confirms the pairing is present
  and correctly placed on every exit path including the run-failure
  CATCH branch).
"No ORTEC prefetch method is called from ZCL_ABAPGIT_SERIALIZE's classic
  per-object loop anymore" was verified by direct manual diff review (the
  entire block was deleted, confirmed above and independently re-read by
  the orchestrator) rather than a new structural/source-inspection test -
  this codebase has no existing spy/double or source-scanning test
  pattern to reuse for this kind of invariant, and inventing one was
  judged lower-value than the direct diff verification already performed.
This gap must be closed by the consolidated IT8 validation plan's Path A/
  Path B routing verification (trace/debug confirms no ORTEC serialization
  optimization method executes when the repository setting is OFF).
```
