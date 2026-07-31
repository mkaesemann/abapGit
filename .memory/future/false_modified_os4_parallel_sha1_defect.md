# PARKED — OS4 parallel-serialization worker computes a wrong SHA1 for otherwise byte-identical content ("Bug A")

```text
STATUS=PARKED_FOR_DEV_SYSTEM_REPRODUCTION
OWNER_DECISION=2026-07-31 owner asked to park this and resume it in the
  development system if reproducible there.
ROOT_CAUSE=NOT FOUND. Reproduced twice (same exact wrong hash both times)
  but not yet isolated at the byte/mechanism level. This is the harder,
  still-genuinely-open half of the "false MODIFIED" investigation.
RELATED_PARKED_ISSUE=.memory/future/false_modified_os4_checksum_baseline.md
  (a separate, ROOT-CAUSED, high-confidence-fix issue that turns any
  one-off trigger of THIS bug into a permanent, unrecoverable blanket
  MM instead of a normal, self-correcting one-sided mismatch — read both
  files, they share the same reproduction environment/object).
SEVERITY_NOTE=Two prior "fixes" attempted this incident (FUGR includes
  RS_GET_ALL_INCLUDES params, and an ORTEC prefetch-buffer stale-cache
  CLEAR fix) were both real, legitimate defects and remain in place, but
  NEITHER explains this specific symptom — confirmed by reproducing the
  wrong-SHA1 behavior AFTER both fixes were applied and activated. Do not
  re-attempt those two as a "fix" for this issue; they're independent.
```

## 1. Problem statement

During PARALLEL serialization (the ORTEC performance feature that
dispatches per-object serialization to background RFC worker tasks via
`CALL FUNCTION 'Z_ABAPGIT_SERIALIZE_PARALLEL' STARTING NEW TASK ...`),
the worker's own computed local-file SHA1 for `/LOT/GC_GEOLON`
(`DTEL`, package `/LOT/GC`) came out as
**`55f8cfe1eb28ffe5b074e39452129a6320a9f333`**, which does **not** match
the known-good, independently-verified-correct remote git blob SHA1
**`0a8df7887b9decf9d7e79f89e53df651cd6a6d9d`** — despite every piece of
underlying content that was checked being independently verified
correct and byte-identical to the remote reference (see §3). This was
reproduced **twice**, in separate debug sessions, with the exact same
wrong hash value both times, ruling out a one-off fluke in favor of a
real, reproducible (if situational) defect.

This is Variant B / Package E / "E2" investigation, OS4 (large-repo)
incident — the same incident as the companion checksum-baseline parked
issue; read that file too for the environment/repro-recipe/security
gotcha, not duplicated in full here.

## 2. Repro environment (exact identifiers — see companion file for full detail)

```text
SAP_SYSTEM=IT8, client 100, connection id "it8"
Repo: "OS4 6.0", ORTEC repo_key=288c81fc1cad, abapGit persistence
  key=000000000002, branch refs/heads/development/6.0.x, package /LOT/OS
Test object: DTEL /LOT/GC_GEOLON
  file: /src/#lot#gc/#lot#gc_geolon.dtel.xml
  correct/reference blob sha1: 0a8df7887b9decf9d7e79f89e53df651cd6a6d9d
    (3783 bytes, confirmed via ZAOG_OBJ_INDEX.blob_sha1 AND independently
    via a full live byte-for-byte capture — see §4)
  wrong worker-computed sha1 (reproduced twice):
    55f8cfe1eb28ffe5b074e39452129a6320a9f333
Repro trigger: "Stage by Transport" with transport IT8K900025 (fast,
  ~236 objects) — this always dispatches GC_GEOLON's serialization to a
  parallel worker (confirmed via call-stack inspection, see §3).
```

## 3. What has been proven correct (do NOT re-investigate these — ruled out)

All of the following were independently verified correct/byte-identical
via live debugging across multiple sessions, for this exact object:

1. **DD04V core fields** (rollname, ddtext="Geographical Longitude",
   domname="GEOLONLAT", scrtext_s/m/l, reptext, datatype, etc.) —
   correct, matches DB, matches reference bytes 0-750.
2. **i18n/translation texts** — `ZCL_ABAPGIT_ORTEC_SER_PREF_EXT=>
   get_dtel_i18n` correctly returns exactly 1 additional language ('D',
   German), with `DDTEXT="Geographische Länge"`, `REPTEXT="Längengrad"`
   etc. — UTF-8 encoding of "ä" (bytes `C3 A4`) verified correct at the
   byte level. Confirmed this is NOT session/parallel-dependent: the
   language filter comes from `ZCL_ABAPGIT_ENVIRONMENT~
   GET_SYSTEM_LANGUAGE_FILTER`, which only returns EXCLUDE entries for
   pseudo-translation languages (1Q/2Q) — with none configured, the
   filter is empty, and an empty ABAP ranges table in `... IN
   lt_language_filter` matches everything (classic ABAP semantics,
   NOT "matches nothing" like a naive SQL reading might suggest) — so
   the actual set of included languages is simply "whatever rows exist
   in DD04T for this rollname", deterministic regardless of process.
3. **DOKIL longtext entries** — `ZCL_ABAPGIT_ORTEC_SER_PREF=>get_dokil`
   for longtext id 'DE' (main documentation) returns exactly 2 rows
   (German + English versions) for this object, both independently
   confirmed correctly scoped (no stale/cross-object contamination) via
   `lines(mt_dokil)=2` live checks, and both versions' text content
   verified byte-identical to the reference at their respective offsets
   (~1000-3625 bytes, including German umlaut text "elektronischen
   Kartenmaterial", "zugehörigen Geo-Referenzpunktes/Kartenreferenz",
   etc.).
4. **`<DD04L_EXTRA>` XML element is correctly OMITTED** (not a bug): its
   only field, `abap_language_version`, is legitimately blank for object
   type `DTEL` (no ABAP-language-version concept applies to data
   elements; `CL_DD_ABAP_LANGUAGE_VERSION=>get_abap_language_version`
   correctly returns blank for `DTEL`), so the fully-initial `ty_extra`
   structure gets suppressed by the standard SAP asx XML transform. **The
   REFERENCE blob ALSO omits this tag at the identical byte offset** —
   confirmed identical, not a divergence.
5. **UTF-8 BOM presence**: both local (worker) and reference start with
   `EF BB BF` — identical.
6. **Bytes 0-1000 of the actual serialized XML, byte-for-byte**: captured
   live from the worker's `<ls_file>-data` via
   `abap_debug_variable` expressions `rs_files_and_item-files[1]-data+
   OFFSET(125)` walking offset 0→1000, and independently captured the
   full 3783-byte REFERENCE the same way from a different breakpoint
   (see §4) — the first 1000 bytes match exactly between worker output
   and reference.
7. **No stale-cache/cross-object contamination** in the ORTEC prefetch
   buffers (`ZCL_ABAPGIT_ORTEC_SER_PREF`, `..._EXT`, `..._OO`) — a real,
   separate defect (unconditional `INSERT INTO TABLE` on hashed tables
   without `CLEAR` between per-object worker invocations) was found and
   FIXED earlier this incident (all three classes' `inject_from_buffer`
   methods now `CLEAR` before merging) — confirmed working exactly as
   designed via live `lines(mt_dtel)=1`/`lines(mt_dokil)=2` checks. **This
   fix is real, correct, and should stay in place, but it is NOT the
   cause of this SHA1 defect** — the wrong SHA1 was reproduced again
   AFTER this fix was active.
8. **The RFC/EXPORT-IMPORT transport of the result is NOT the corruption
   point**: captured the SAME wrong SHA1 (`55f8cfe1eb28ffe5b074e39452129a6320a9f333`)
   both immediately before `EXPORT data = ls_files TO DATA BUFFER
   ev_result.` inside the worker (`Z_ABAPGIT_SERIALIZE_PARALLEL`), AND
   after `RECEIVE RESULTS FROM FUNCTION`/`IMPORT ... FROM DATA BUFFER` in
   the main process's `on_end_of_task`/`add_to_return`
   (`ZCL_ABAPGIT_SERIALIZE` line ~267) — the value is identical on both
   sides of the RFC boundary, filename correctly tagged
   (`#lot#gc_geolon.dtel.xml`). The corruption (if it is data corruption
   at all, vs. a hashing-mechanism flake — see §5) happens INSIDE the
   worker's own serialize+hash call, before the EXPORT.
9. **abapGit's own "diff" feature reports "no differences"** for this
   exact file while its status still shows `MM` in the same session —
   i.e. SOME serialize pass (not necessarily the same one that produced
   the wrong SHA1 — see §5.3) produces content that a real line-by-line
   comparison finds identical to remote.

## 4. Key technique: full byte-level capture of an xstring blob via live debugging

`execute_data_query` (the SQL/ABAP-SQL MCP tool) truncates ANY
xstring/rawstring field value to ~125 bytes / 250 hex chars, regardless
of `SUBSTRING()`/`download_to_file`/display mode (`SUBSTRING()` is also
flatly rejected by Open SQL for xstring/rawstring columns: "not allowed
in this position"). This is a hard limit in how the tool retrieves
LOB-like values, not a display truncation — do not waste time fighting
it with query variations.

**Working technique**: set a live ABAP debugger breakpoint at the exact
line where the blob content you need gets read into a plain local
variable/field-symbol, with a condition matching the specific object
(sha1, object name, etc.), let it fire, then walk the FULL content via
repeated `abap_debug_variable` calls of the form
`some_variable+OFFSET(125)` with OFFSET = 0, 125, 250, 375, ... up to
the known/expected total length. Each individual call is capped at
~125 bytes of returned hex, but there is no limit on how far you can
walk this way.

Concretely, for the REFERENCE (remote) blob, the breakpoint used was:
- File: `ZCL_ABAPGIT_ORTEC_OBJ_STORE.clas.abap`
- Method: `get_objects` (called by the public `get_object` wrapper)
- Line (verify current line number with `get_abap_object_lines` before
  reuse — line numbers can drift): the `APPEND ls_object TO rt_objects.`
  statement inside the final `LOOP AT lt_rows ASSIGNING <ls_row>.` block
  (was line 606 this session).
- Condition: `ls_object-sha1 = '<target sha1, e.g.
  0a8df7887b9decf9d7e79f89e53df651cd6a6d9d>'`
- This fires for ANY caller that fetches that blob's content by SHA1 —
  in practice, clicking the abapGit "diff" UI action for the file is a
  reliable, repeatable way to trigger it (the diff view must fetch the
  remote blob to render a line comparison).
- Once paused, `ls_object-data+OFFSET(125)` was walked from 0 to 3750 to
  recover the complete, verified 3783-byte reference blob for
  `/LOT/GC_GEOLON`.

**IMPORTANT — this exact full-length capture has NOT yet been done for
the WORKER's (local/parallel) side of a run that actually reproduces the
wrong SHA1.** Only bytes 0-1000 of the worker's output were captured
during the session(s) that reproduced the defect (via
`rs_files_and_item-files[1]-data+OFFSET(125)` breakpointed at
`ZCL_ABAPGIT_OBJECTS.clas.abap`, the `LOOP AT rs_files_and_item-files
ASSIGNING <ls_file>.`/`<ls_file>-sha1 = zcl_abapgit_hash=>sha1_blob(
<ls_file>-data ).` lines — was line 1299/1300 this session, condition
`is_item-obj_name = '/LOT/GC_GEOLON'`). **This is the single most
valuable next step** — see §6.1.

Other debugger technique notes worth carrying forward:
- Table row access via `abap_debug_variable` works with `tab[1]-field`
  (NO spaces inside the brackets); `tab[ 1 ]-field` (WITH spaces) fails
  with `CodeExpectedError`. This corrects an earlier, wrong "can't expand
  table rows" limitation note from an even earlier session.
- The debug expression evaluator does **not** support method/function
  calls of any kind (confirmed failing for both a zero-argument instance
  method call and a static method call with named parameters) — only
  plain variable/field-symbol access, component access (`-`), and
  offset/length slicing (`+OFFSET(LENGTH)`) work.
- Parallel RFC worker threads ARE catchable in the same debug session as
  the main dialog process: start the session with default (User) mode,
  set your breakpoint, trigger the action, then use `abap_debug_status`
  to see additional threads appear (e.g. `Thread 2:
  ZCL_ABAPGIT_OBJECT_DTEL`) with call stacks rooted at
  `SAPMSSY1.prog.abap` (the RFC dispatcher) →
  `Z_ABAPGIT_SERIALIZE_PARALLEL.fugr.abap` → ... — pass the correct
  `threadId` to `abap_debug_stack`/`abap_debug_variable`/
  `abap_debug_step` for that thread.
- The debug session/hook can go silently stale (0 threads reported even
  during genuine active background computation, or a step command
  returning "Canceled" with the session dropping to "No active
  debugging session"). Always do a `stop` + `start` cycle and re-arm
  breakpoints after any such gap; also expect OLD breakpoints from
  earlier in the same overall investigation to still be registered
  server-side and fire unexpectedly — just `continue` past them (or
  remove them if you know the exact file/line, but note that removing a
  breakpoint the debugger is CURRENTLY stopped at will auto-continue
  that thread as a side effect).
- Clicking a per-file "diff (N)" link for one specific file in the repo
  object list, when the local side was already computed by an earlier
  full-repo serialize pass, triggers a fast, targeted single-object
  reserialize (observed as "Serialize /LOT/GC_GEOLON, 1 thread" in the
  UI, completing in ~0.01-37s) — but a breakpoint on
  `ZCL_ABAPGIT_OBJECTS.clas.abap`'s SHA1-computation line did **not**
  fire during that fast path, suggesting the diff view's "local" content
  may come from an already-cached `it_local` from the prior full
  serialize rather than a fresh call to `zcl_abapgit_objects=>serialize`
  — this is relevant to interpreting §3 point 9 and needs confirming in
  the dev system (see §6.2).

## 5. Leading hypotheses for the actual defect (NOT confirmed — pick one to test first in the dev system)

### 5.1 A genuine byte-level content divergence beyond byte 1000 (most testable)
The worker's own data was only verified byte-identical to the reference
for the FIRST 1000 of 3783 bytes. It is entirely possible the divergence
(extra/missing/reordered content — e.g. in the longtext `<LINES>` items,
or the second DOKIL entry, or the closing tags) lives somewhere in bytes
1000-3783 and simply wasn't checked yet on the worker side during a run
that actually produced the wrong hash. **This is the cheapest hypothesis
to rule in/out and should be tried first** (see §6.1).

### 5.2 A hashing-mechanism flake under parallel RFC load, not a data problem
`ZCL_ABAPGIT_HASH=>sha1_blob`/`sha1`/`sha1_raw` implement the standard
git blob hash (`sha1("blob " & length & "\0" & data)`) via
`cl_abap_message_digest=>calculate_hash_for_raw` (a single kernel call,
chosen specifically to avoid a previous double-SHA1-via-HMAC bug — see
the method's own code comment). If this kernel call is ever unreliable
under heavy parallel RFC load specifically (e.g. some kind of buffer
reuse/threading issue at the kernel/ICM level, not visible from ABAP),
that would explain a defect that's real, reproducible in aggregate, but
NOT explainable by inspecting ABAP-level data — which is consistent with
every individual field checking out correct so far. This would be a much
harder (possibly SAP Basis/kernel-version-specific) issue to pin down
and may not be fixable at the ABAP code level at all — if bytes 0-3783
are proven fully identical between worker and reference (§6.1) yet the
hash still differs, this becomes the leading remaining explanation.

### 5.3 The "diff" feature and the buggy status calculation may not be using the same local content
Per §4's last bullet: the per-file "diff" UI action may reuse an
ALREADY-CACHED local file entry from an earlier full-repo serialize
pass, rather than recomputing fresh. If so, "diff says no differences"
and "the cached SHA1 tag on that same entry is wrong" are not
necessarily contradictory — they could both be true if the ORIGINAL
full-repo parallel run that computed the (wrong) SHA1 also happened to
produce byte-identical XML content, and the hash mismatch really is
isolated to the hashing step itself (reinforcing 5.2), OR if the diff
view's comparison is itself not as literal/complete as it appears (e.g.
whitespace-insensitive, or comparing normalized/parsed XML rather than
raw bytes) and could mask a genuine small byte-level difference that
5.1 would still find. Worth clarifying which of these is true before
concluding definitively.

## 6. Next steps to resume in the development system

### 6.1 Highest priority: full byte-for-byte worker-vs-reference diff
1. Reproduce the wrong SHA1 for `/LOT/GC_GEOLON` (or a fresh object if
   this one happens to be fixed by then) using "Stage by Transport" +
   a breakpoint at `ZCL_ABAPGIT_OBJECTS.clas.abap`'s
   `<ls_file>-sha1 = zcl_abapgit_hash=>sha1_blob( <ls_file>-data ).` line
   (condition `is_item-obj_name = '<object name>'`), confirming via the
   call stack that you're in the `Z_ABAPGIT_SERIALIZE_PARALLEL` worker.
2. This time, walk the ENTIRE `<ls_file>-data` xstring (not just the
   first 1000 bytes) via `+OFFSET(125)` from 0 to its full length (get
   the length first, e.g. by locating the closing `</abapGit>` tag or by
   noting the reference's known length of 3783 bytes as a starting
   guess for this same object).
3. Compare byte-for-byte against the reference capture recipe in §4 (or
   reuse the reference bytes already recorded in
   `.memory/logs/variant_b_package_e_false_modified_os4_d1.md` §24.2 if
   this is literally the same object/commit — otherwise recapture fresh
   in the dev system, since the dev system's DB content/commit tip will
   likely differ).
4. If a divergence is found: identify which serialization step produces
   it (DD04V post-processing clears, a specific longtext entry, i18n
   text, XML tag ordering, etc.) and design a targeted fix.
5. If NO divergence is found (all 3783 bytes identical) yet the SHA1
   still differs: this rules out a data problem entirely and points at
   §5.2 (hashing mechanism itself) — escalate as a potential SAP kernel/
   Basis-level issue rather than continuing to chase it as an ABAP logic
   bug; consider re-testing on a different kernel patch level/instance
   if available, or engaging SAP Basis.

### 6.2 Clarify what the "diff" UI action actually compares
Determine (via a fresh breakpoint on `zcl_abapgit_objects=>serialize` at
its very first line, condition on object name) whether clicking a
per-file "diff (N)" link after an earlier full serialize pass triggers a
genuinely fresh `zcl_abapgit_objects=>serialize` call for the local side,
or reuses cached `it_local` data from the earlier pass. This determines
whether "diff says no differences" is directly informative about the
SAME serialize run that produced the wrong SHA1, or a separate,
potentially-different run — needed to correctly interpret §3 point 9 and
§5.3.

### 6.3 Try to correlate the defect with load/timing
If reproducible enough, try to determine whether the wrong-SHA1 behavior
correlates with: number of parallel workers active at once, size/
position of the object in the dispatch queue, memory pressure, or
specific RFC server group settings (`mv_group` in
`ZCL_ABAPGIT_SERIALIZE`). If it's purely load-dependent, that supports
§5.2 (kernel/thread-level flake) over §5.1 (a deterministic ABAP logic
bug, which should reproduce the SAME way every single time regardless of
load, aside from RFC race timing being a longshot fourth hypothesis
between clean data-bug and clean kernel-bug).

## 7. Read also

- `.memory/future/false_modified_os4_checksum_baseline.md` — the
  companion, ROOT-CAUSED, higher-confidence-fix issue. Fixing that one
  does NOT fix this one, but it changes this defect's visible impact
  from "permanent blanket MM" to "an occasional, self-correcting
  one-sided M" — worth doing regardless of whether this file's
  investigation ever concludes.
- `.memory/logs/variant_b_package_e_false_modified_os4_d1.md` — full
  investigation log, sections §14-§24 cover this specific defect's
  history (stale-cache fix design/implementation, the two reproductions
  of the wrong SHA1, the DD04L_EXTRA ruling-out, the reference-blob byte
  capture, and the pivot to the checksum-baseline finding).
- `.memory/incidents/variant_b_package_e_false_modified_os4_d0.md` and
  `.memory/handoffs/variant-b-package-e-e2-os4-diagnostic.md` — D0
  reproduction packet and handoff summary, kept in sync with this
  incident's status.
