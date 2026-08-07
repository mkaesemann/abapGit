# SER-SLICE-3 incident — DTEL/DOMA output-parity failure (Feature ON)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_3_DTEL_DOMA_PARITY_INCIDENT
STATUS=SUPERSEDED_FALSE_ORACLE
SUPERSEDED_BY=OWNER_IT8_DEBUG_EVIDENCE
BATCH_SERIALIZER_DATA=CORRECT
LEGACY_ORTEC_NON_BATCH_DELTA=INCORRECT_FALSE_MODIFIED
ROOT_CAUSE_ANALYSIS_REQUIRED=NO
DO_NOT_RESUME=YES
BASELINE_HEAD_AT_REPORT=40934176deee5935f1ab62b556aaf42044c46d15
BASELINE_HEAD_AT_SUPERSEDE=bf436db0632de0098875d97b4a28a8f246eb50a5
```

## SER-SLICE-3 correction (2026-08-07, owner IT8 debug evidence)

The apparent Feature-OFF/Feature-ON Stage mismatch investigated below was
not a lost-output defect in the batch serializer. The owner established
through IT8 activation, tests, runtime execution, and manual debugging:

> The removed hybrid ORTEC non-batch path (Path 3: standard abapGit
> orchestration with legacy ORTEC serialization optimizations but without
> adaptive batching) exhibited false MODIFIED results - it incorrectly
> reported many unchanged DTEL/DOMA files as MODIFIED in Stage. The
> adaptive batch serializer (Path 2) returned the complete, correct
> DTEL/DOMA serialized data all along; the smaller Feature-ON Stage result
> was correct because those files were identical to Remote and therefore
> correctly absent from the MODIFIED list. The Feature-OFF hybrid ORTEC
> result was never a valid serializer-parity oracle. It is intentionally
> removed rather than repaired (see `.memory/logs/
> serialization_final_two_path_audit.md` and SER-SLICE-3 Phase 7).

DOMA/DTEL provider status: `DOMA_DTEL_PROVIDER=IMPLEMENTED_AND_OWNER_
DEBUG_VALIDATED`. Do not reopen DOMA/DTEL as broken without new direct
serializer evidence (not Stage/MODIFIED-list evidence from the removed
path). The fixes recorded below (Fix A-F, plus the owner's own follow-up
commit `bf436db0` adding a second, redundant `PREPARE()` call and
tightening `MERGE_INTO_MT_FILES`'s zero-file guard) are retained as
genuine defensive/lifecycle hardening - they are safe, non-regressive,
and independently justified regardless of which hypothesis explained the
original symptom. Open a new incident only if equivalent false-MODIFIED
or data-loss behavior appears in one of the two retained paths (Path 1
pure standard, or Path 2 adaptive batch).

The original investigation, hypotheses, and fixes are retained below
unchanged as historical record.

## Reproduction (owner-reported, primary evidence)

```text
REPOSITORY=OS4 6.0 / development/6.0.x, path /LOT/OS (per screenshots)
ACTIVATION=PASS
ABAP_UNIT=PASS
ATC=PASS
CLAS_FEATURE_ON=PASS
DTEL_DOMA_OUTPUT_PARITY=FAIL

FEATURE OFF (mv_serial_batch_active = abap_false):
  Stage result: 113 files (.abapgit.xml + many DTEL files + at least 1 DOMA
  file, e.g. /LOT/CA_BGTYP.dtel.xml, /LOT/CA_GLOB_KEY_NAME.dtel.xml,
  /LOT/CA_VERSION.dtel.xml, /LOT/CA_VGTYP.dtel.xml, many /ORTEC/TLO_*.dtel.xml,
  /ortec/tlo_repository/#ortec#tlo_strategy_proc_group.doma.xml)

FEATURE ON (mv_serial_batch_active = abap_true):
  Stage result: 2 files (.abapgit.xml + 1 DOMA file only)
  ~111 DTEL files absent
```

No ST22 dump correlates with the reported failure window (confirmed via
live `SAPDiagnose action=dumps` - most recent dumps predate the owner's
"Fix Syntax Issues for SER_SLICE-3" commit and are all pre-existing
RPERF_ILLEGAL_STATEMENT/CALL_FUNCTION_SEND_ERROR entries already resolved
in the prior SER-SLICE-2 RPERF fix).

## Phase 0 — owner corrections since the last SER-SLICE-3 checkpoint

Owner commit `40934176` "Fix Syntax Issues for SER_SLICE-3" (after IT8
import/activation/correction, pulled back into the local repo):

```text
src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap
  mv_serial_batch_active: abap_true -> abap_false (default OFF)
  CLASS=SEMANTIC_CHANGE, plausible owner safety-net revert while
  investigating this exact incident - NOT reverted by this fix pass (the
  owner's evidence shows they manually re-enabled it to abap_true for the
  Feature-ON reproduction; the shipped default staying OFF pending this
  incident's resolution is appropriate and left as-is).

src/ortec/serial/core/zaog_ser_dd_bentry.tabl.xml
  PRESENT field ROLLNAME: ABAP_BOOL -> ABAP_BOOLEAN (+ VALEXI/SHLPORIGIN
  SE11-generated metadata, BOM added to all 3 new DDIC XML files)
  CLASS=DDIC_COMPATIBILITY - live-verified ABAP_BOOLEAN is a real SAP
  Basis domain (package SABP_COMMON, CHAR1, true/false semantics) with the
  SAME length as ABAP_BOOL - confirmed NOT a type-incompatibility risk for
  the wire format (EXPORT/IMPORT is self-describing and both EXPORT and
  IMPORT reference the same DDIC structure on one system).

src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap (+1 trailing
  blank line), src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap
  (+1 trailing blank line), zabapgit_ortec_serial.fugr.z_abapgit_ortec_
  ser_batch.abap (FUNCTION statement lowercased, +1 trailing blank line)
  CLASS=SYNTAX_ONLY (SAP's own re-serialization of an activated object,
  no logic change - confirmed via diff)

NEW: src/ortec/serial/rfc/z_abapgit_ortec_ser_batch     rf.sush.xml
  CLASS=OTHER - a stray SUSH ("short text") object export artifact with a
  garbled/multi-space filename, harmless (short-text container for the
  function module), left untouched - not implicated in this incident and
  out of scope for this fix (a future housekeeping pass may rename/remove
  it; not done here per the incident's narrow scope).

NEW: src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.locals_imp.abap
  CLASS=SYNTAX_ONLY, but load-bearing: `CLASS ltcl_dd_batch_wire
  DEFINITION DEFERRED.` - the REQUIRED forward declaration for the
  testclasses include's `LOCAL FRIENDS ltcl_dd_batch_wire` reference, which
  the prior implementation pass never generated. Without this the class
  pool could not compile at all - a real gap in local tooling (get_errors
  does not model the locals_def/locals_imp/testclasses split of a real
  ABAP class pool the way the live compiler does). No revert needed - this
  is a necessary correction, not a defect to investigate.
```

None of the owner's corrections caused or masked the parity failure - all
are either cosmetic/DDIC-metadata or a required-for-activation syntax fix.

## Phase 1/2 — pipeline comparison and root-cause investigation

Tooling constraint (disclosed): this session had READ-ONLY SAP ADT access
(`mcp_arc-12_*`). No interactive debugger or breakpoint tool was available.
`SAPDiagnose(action="trace_start")` was attempted to arm a live profiler
trace on `Z_ABAPGIT_ORTEC_SER_BATCH` for the owner's next reproduction, but
failed: `allowWrites=false blocks mutations` - trace arming requires write
scope, which this connection does not have. All investigation below is
static source analysis plus READ-ONLY live queries (SAPRead/SAPQuery),
not a live debug session of an actual failing run.

### CONFIRMED defects (proven via source reading + live read-only checks)

```text
DEFECT A: ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE never called
  zcl_abapgit_ortec_ser_pref/_ext/_oo=>prepare() anywhere in its call
  graph - unlike the standard ZCL_ABAPGIT_SERIALIZE=>SERIALIZE (classic
  path), which calls prepare() before its own per-object loop. This meant
  mt_doma/mt_dtel (and the MSAG/OO caches) were ALWAYS EMPTY in the
  ORCH-owning (main/caller) session for the ENTIRE adaptive-batch path -
  EXTRACT_FOR_BATCH could never see a real cache entry, so no provider HIT
  could ever occur on the batch path, defeating the entire point of
  SER-SLICE-3's DOMA/DTEL provider (100% MISS rate in production).

DEFECT B: ZCL_ABAPGIT_ORTEC_SER_PREF_EXT=>EXTRACT_FOR_BATCH appended a
  ZAOG_SER_DD_BENTRY row (PRESENT = ABAP_FALSE) for EVERY DOMA/DTEL object
  in a batch even when NOTHING was cached (a direct consequence of Defect
  A always being true) - so it built and exported a non-empty envelope
  (header + all-miss entries, empty DOMA/DTEL payload tables) on every
  DTEL/DOMA-bearing dispatch, violating its own documented "return INITIAL
  with zero DB access when nothing to send" contract and forcing
  INJECT_BATCH_FROM_BUFFER to run needlessly on every such dispatch.
```

### Live read-only verification that RULED OUT several hypotheses

```text
H8 (name truncation/collision) - RULED OUT. Live SAPRead confirmed
  SOBJ_NAME = CHAR40, TROBJTYPE = CHAR4, ZAOG_SER_TADIR-OBJ_NAME =
  SOBJ_NAME (CHAR40), ZAOG_SER_DD_BENTRY-OBJ_NAME = SOBJ_NAME (CHAR40) -
  every namespaced object name (e.g. /LOT/CA_BGTYP, /LOT/CA_GLOB_KEY_NAME,
  well under 40 chars) round-trips without truncation at every layer.
"predicate doesn't match" - RULED OUT for the sampled objects. Live
  SAPQuery confirmed /LOT/CA_BGTYP and /LOT/CA_GLOB_KEY_NAME BOTH have
  real, active (AS4LOCAL='A', AS4VERS='0000') rows in DD04L right now -
  ZCL_ABAPGIT_OBJECT_DTEL's standard fallback SELECT predicate is
  identical to and would find these rows.
No new ST22 dump - RULED OUT a hard runtime abend as the mechanism.
```

### The mechanism identified as STRUCTURALLY POSSIBLE but NOT empirically
proven for the exact 111-object loss ratio (H5)

`ZCL_ABAPGIT_OBJECT_DTEL~SERIALIZE`'s own standard (UNMODIFIED) fallback
has a silent-empty-success shape:
```abap
SELECT SINGLE * FROM dd04l ... WHERE rollname = lv_name AND as4local = 'A' AND as4vers = '0000'.
IF sy-subrc <> 0 OR ls_dd04v IS INITIAL.
  RETURN.   " no exception - the caller sees RC = 0, output_file_count = 0
ENDIF.
```
This is the ONLY code path in the whole call graph capable of producing a
"successful, zero-file" result for an ordinary DTEL object without an
exception. Given the fail-fast contract's all-or-nothing terminal
accounting (either every object resolves and the run succeeds, or ANY
failure discards the whole run with a visible error), a clean "2 files"
success with ~111 objects missing is only explainable if those ~111
objects were marked TERMINALLY SUCCESSFUL while contributing zero files -
i.e., this exact branch (or an equivalent one) firing en masse inside the
RFC worker. WHY it would fire for real, active objects (ruled out as a
data-existence problem above) was NOT conclusively proven - the two
CONFIRMED defects (A, B) do not by themselves force this branch to fire;
they only guarantee the WORKER never gets prefetch data (a safe MISS in
isolation). A live debugger/SAT trace on an actual reproduction would be
needed to prove the EXACT trigger empirically; this was not available in
this session (write scope disabled).

### Independent adversarial review found 3 additional, real, latent defects
NOT part of the original reported symptom but capable of causing an
EQUIVALENT silent-loss failure mode, found by actively attacking the
fixed source (cycle 1 verdict: REJECT, 2 blockers, 1 major):

```text
AR-3-001 (BLOCKER, FIXED): the RFC worker only cleared mt_doma/mt_dtel as
  a SIDE EFFECT of INJECT_BATCH_FROM_BUFFER running, which only happened
  when iv_prefetch_buffer_dd was non-initial. A pooled/reused worker
  session could carry a PRIOR dispatch's real cached DOMA/DTEL data into a
  LATER dispatch whose own buffer was legitimately empty (exactly the
  case Fix B makes MORE common) - a stale-cache cross-batch leak.
AR-3-002 (BLOCKER, FIXED): ROUTE_TO_SEQUENTIAL_FALLBACK - the LAST-RESORT
  recovery path, used both directly (forced-sequential objects) and as
  the recovery mechanism for a suspicious batch/merge result - itself
  unconditionally called mark_object_success after zcl_abapgit_objects=>
  serialize returned without exception, even with zero files. The
  "safety net" was not actually safe.
AR-3-003 (MAJOR, FIXED): MERGE_INTO_MT_FILES returned rv_merged = TRUE
  after a successful IMPORT without checking the imported file list was
  non-empty, allowing a metadata/payload mismatch to slip through.
```

Cycle 2 (after fixes D/E/F below) re-verified all three CLOSED, 0/0/0,
VERDICT=APPROVE - full detail in
`.memory/reviews/serialization_slice_3_adversarial.md`.

## Corrections applied (this pass)

```text
Fix A - ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE now calls
  zcl_abapgit_ortec_ser_pref/_ext/_oo=>prepare() at entry (gated by
  is_serial_prefetch_active(), mirroring the classic path's own call
  exactly) and =>clear() on the success path (after purge_run_state), the
  run-established failure/discard path (after discard_run_state), AND the
  pre-run-context uuid-generation-failure path (DR-002 fix) - clear() now
  runs on every exit path.
Fix B - EXTRACT_FOR_BATCH now returns an INITIAL buffer when
  `lt_doma IS INITIAL AND lt_dtel IS INITIAL` (nothing genuinely cached
  for ANY DOMA/DTEL object in the batch), regardless of whether
  lt_entries has present=false rows.
Fix C - new pure helper IS_ZERO_FILE_SUCCESS_BAD(is_row, iv_key_found);
  ON_END_OF_BATCH's per-row loop checks this FIRST and routes any
  RC=0/zero-file requested row through ROUTE_TO_SEQUENTIAL_FALLBACK
  instead of accepting it as a bare success.
Fix D - new public ZCL_ABAPGIT_ORTEC_SER_PREF_EXT=>CLEAR_DD_CACHE
  (unconditional `CLEAR mt_doma. CLEAR mt_dtel.`); the RFC worker
  (Z_ABAPGIT_ORTEC_SER_BATCH) now calls this UNCONDITIONALLY at the top
  of every invocation, before the conditional iv_prefetch_buffer_dd
  injection - closes AR-3-001.
Fix E - ROUTE_TO_SEQUENTIAL_FALLBACK now checks
  `IF ls_serialization-files IS INITIAL.` immediately after a successful
  zcl_abapgit_objects=>serialize call; treats it exactly like an
  exception (mark_object_failures, logged warning, CONTINUE) - closes
  AR-3-002. This is an ORTEC-owned wrapper method, not a standard
  abapGit class, so this tightening does not touch any standard object
  serializer.
Fix F - MERGE_INTO_MT_FILES now checks
  `IF ls_serialization-files IS INITIAL. RETURN. ENDIF.` (rv_merged stays
  ABAP_FALSE) immediately after a successful IMPORT, before the
  file-append loop - closes AR-3-003.
```

Every fix is additive/tightening only - no existing method's SUCCESS-path
behavior for a genuinely non-empty result changed; only previously-silent
FAILURE-shaped results (zero files, stale cache) now correctly surface as
failures or are prevented from occurring at all.

## Binding requirements checklist

```text
Feature OFF behavior unchanged            YES (classic path untouched)
Feature ON byte-identical to Feature OFF  PENDING_IT8 (local logic proven
                                           via reviews; needs owner retest)
Provider failure/miss falls back safely   YES (unchanged MISS contract +
                                           Fix D closes the stale-cache gap)
No successful partial output              YES (Fix C/E/F close every
                                           identified silent-zero-file path)
No object-name truncation                 YES (live-verified CHAR40
                                           throughout)
No cross-batch leakage                    YES (Fix D + existing
                                           no_cross_batch_leakage test)
No unbounded buffer growth                UNCHANGED (not in scope for this
                                           incident, no new risk introduced)
Actual-byte admission remains enforced    UNCHANGED (BEFORE_DISPATCH logic
                                           not touched by this incident fix)
Minimal standard hook unchanged           YES (zcl_abapgit_serialize's own
                                           hook body untouched)
WAPA singleton behavior unchanged         YES (not touched)
```

## Local validation state

```text
GET_ERRORS=CLEAN on every touched file (re-verified after every fix)
METHOD_NAME_LENGTH=CLEAN (full re-scan after every rename)
LIVE_SYNTAX_DRY_RUN=NOT_RE-RUN this pass (same EXPECTED_DDIC_FAILURE
  boundary as before - the 3 new DDIC objects are not yet on IT8)
UNIT_TESTS_ADDED=10 new methods this pass: EXTRACT_ALL_MISS_STILL_EMPTY,
  CLEAR_DD_CACHE_CLEARS_BOTH (ser_pref_ext); ZERO_FILE_SUCCESS_FLAGGED,
  NONZERO_FILE_NOT_FLAGGED, ZERO_FILE_BUT_FAILED_ROW_OK,
  ZERO_FILE_UNMATCHED_ROW_OK, FALLBACK_ZERO_FILES_FAILS (ser_orch) - none
  executed live yet (no IT8 DDIC objects, no live ABAP Unit run this
  session)
CORRECTNESS_REVIEW=APPROVE_WITH_MINOR_REVISIONS (0 blocker/1 major/1
  minor - DR-001 accepted as a disclosed, pre-existing, performance-only
  scope boundary [MSAG/OO providers still not wired into the RFC batch
  path itself, unchanged from before this incident - they DO still
  benefit from prepare() for the forced_seq/WAPA/fallback in-process
  subset]; DR-002 FIXED this pass)
ADVERSARIAL_REVIEW=cycle 1 REJECT (2 blocker/1 major) -> fixes D/E/F ->
  cycle 2 APPROVE (0/0/0)
```

## Owner action required

Run the retest plan in `.memory/logs/serialization_slice_3_it8_validation_
plan.md` (updated this pass) against the EXACT SAME repository/branch/
filter scope as the incident screenshots. Do not mark this incident
resolved until Feature ON produces the SAME 113 files (paths, filenames,
byte-identical payloads) as Feature OFF for that exact scope.
