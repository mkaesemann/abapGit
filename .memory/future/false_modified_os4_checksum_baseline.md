# PARKED — OS4 false MODIFIED/MODIFIED: near-empty checksum baseline

```text
STATUS=PARKED_FOR_DEV_SYSTEM_REPRODUCTION
OWNER_DECISION=2026-07-31 owner asked to park this and resume it in the
  development system if reproducible there, instead of applying the fix
  directly on IT8 right now.
ROOT_CAUSE=ROOT_CAUSED_WITH_HIGH_CONFIDENCE (see below) — this is NOT a
  content/serialization bug. It is a status-calculation bug triggered by
  a persisted checksum baseline that was never populated for this repo.
FIX_IDENTIFIED=YES, standard (unmodified) abapGit feature, NOT YET APPLIED.
RELATED_PARKED_ISSUE=.memory/future/false_modified_os4_parallel_sha1_defect.md
  (a separate, still-unresolved defect that can TRIGGER the symptom this
  baseline gap turns into a permanent blanket MM — read both files, they
  share the same reproduction environment/object).
```

## 1. Problem statement (original symptom)

In the ORTEC abapGit fork on SAP system IT8 (client 100), the online
repository **"OS4 6.0"** shows a large number of files with status
`MM` (locally Modified **and** remotely Modified) in the repo overview,
even though — when you open the actual per-file diff for one of those
files — abapGit reports **"There are no differences to show. The local
state completely matches the remote repository."** i.e. the file content
is provably identical, but the status list still shows `MM`. This is a
*false positive* "modified" status, not a real content difference.

This is Variant B / Package E / "E2" investigation, OS4 (large-repo)
incident. It supersedes/extends an earlier retracted small-repo
reproduction attempt (see
`.memory/handoffs/variant-b-package-e-e2-diagnostic.md`,
`STATUS=SUPERSEDED_INVALID_REPRODUCTION` — ignore that one, it was a stale
local clone, not a real bug).

## 2. Repro environment (exact identifiers)

```text
SAP_SYSTEM=IT8, client 100, ABAP-FS connection id "it8"
Repo display name: "OS4 6.0"
ORTEC git tables repo_key: 288c81fc1cad
abapGit persistence key (table ZABAPGIT, type=REPO): 000000000002
Repo URL: https://ORTEC-SAP@dev.azure.com/ORTEC-SAP/ORTEC%20for%20S4HANA/_git/OS4
Branch: refs/heads/development/6.0.x
Package: /LOT/OS
Tip commit (as of this session): 81157b1448b4183f38403ec63caad1291a2226a4
Repo CREATED_AT: 20260506113653 (2026-05-06 — "New Online" link, NOT a
  file-by-file git clone through abapGit; the ~17000+ objects almost
  certainly arrived in this system via transport import, not via an
  abapGit Pull — see §5 for why this matters)
Test object used throughout: DTEL /LOT/GC_GEOLON
  (file: /src/#lot#gc/#lot#gc_geolon.dtel.xml)
  known remote blob sha1 (CORRECT): 0a8df7887b9decf9d7e79f89e53df651cd6a6d9d
  blob size: 3783 bytes
A second object, DTEL /LOT/GC_GEOLAT, was used in earlier session slices
  and shows the same symptom class.
```

### Fast reproduction recipe (on IT8, ~2 minutes)
1. Open transaction `ZABAPGIT` (WebGUI or SAP GUI).
2. Open repo "OS4 6.0".
3. Repo menu → **Stage** → **"Stage by Transport"** → enter transport
   `IT8K900025` (filters to ~236 objects in package `/LOT/GC`, MUCH faster
   than a full repo operation on this ~17000-object repo).
4. Observe the object list: most rows show `MM`.
5. Click the **"diff (N)"** link on any `MM` row (e.g. `/LOT/GC_GEOLON`) →
   it will say "There are no differences to show. The local state
   completely matches the remote repository." while the row still shows
   `MM`. This is the core reproduction of the bug.

### Slow/full reproduction (only if the fast path is unavailable, ~9 minutes on IT8)
Open the repo directly (not via Stage by Transport) → **Diff** menu
action. This forces a full re-serialize of all ~17000 local objects
(reported as "NN%: Serialize <object>, 14 threads" progress, took ~533s
on IT8) followed by "Fetch remote files", then renders the same
per-object `MM` list with `diff (N)` links. Functionally equivalent to
the fast path but much slower — prefer Stage by Transport for repro.

**Gotcha (found this session, cost significant time)**: clicking into the
repo/opening it can trigger an abapGit **remote login dialog**
("Login: ORTEC-SAP@dev.azure.com") if the cached remote credential/token
session has lapsed. On at least one occasion this session, the dialog's
password/token field was **auto-filled by the browser with a live
Azure DevOps PAT**, and that value was captured verbatim in an
accessibility snapshot returned to the agent. **Do not click "Continue"
on that dialog, and be careful with full-page snapshots/screenshots when
it's open** — click "Cancel" instead, and if a live credential is ever
captured in tool output, immediately tell the owner to rotate it. (This
happened once this session; the owner was informed and said they would
rotate the token after the investigation — this is not this file's
concern, just a process hazard to avoid repeating.)

## 3. Root cause (confirmed via direct evidence, not inference)

### 3.1 The status algorithm's baseline-fallback
`ZCL_ABAPGIT_STATUS_CALC=>build_existing` (called from `process_local`,
called from `zif_abapgit_status_calc~calculate_status`, called from
`ZCL_ABAPGIT_REPO_STATUS=>calculate`):

```abap
rs_result-match = boolc( is_local-file-sha1 = is_remote-sha1 ).
IF rs_result-match = abap_true.
  RETURN.                                   " truly unchanged, done
ENDIF.

READ TABLE it_state INTO ls_file_sig
  WITH KEY path = ... filename = ... BINARY SEARCH.
IF sy-subrc = 0.
  " compare local-vs-baseline and remote-vs-baseline INDEPENDENTLY —
  " this is the "healthy" path: a one-sided mismatch only flags one side
  IF ls_file_sig-sha1 <> is_local-file-sha1.
    rs_result-lstate = c_state-modified.
  ENDIF.
  IF ls_file_sig-sha1 <> is_remote-sha1.
    rs_result-rstate = c_state-modified.
  ENDIF.
ELSE.
  " "This is a strange situation. As both local and remote exist the
  "  state should also be present. Maybe this is a first run of the code.
  "  In this case just compare hashes directly and mark both changed..."
  rs_result-lstate = c_state-modified.
  rs_result-rstate = c_state-modified.       " BLANKET fallback — the bug
ENDIF.
```

`it_state` is the persisted "checksum baseline" (`ii_repo->checksums( )->
get_checksums_per_file( )`) — abapGit's memory of "what did local and
remote both look like the last time they were known to be in sync"
(normally kept current by `checksums( )->update(...)` calls after every
Pull and Push — see §3.3, this part of the code is fine).

**The bug's effect**: whenever `local sha1 <> remote sha1` for *any*
reason (a genuine change, OR a transient/wrong SHA1 computation — see
the companion parked issue) AND the file has no baseline row, BOTH sides
get blanket-marked Modified, with no way to tell "this file actually
differs" apart from "we simply don't know, so we're guessing both
changed." There is also no self-healing: since there's still no baseline
row after the "bad" run, the NEXT refresh will make the exact same
guess again, forever, even if the underlying transient cause never
recurs.

### 3.2 The actual persisted baseline for this repo is almost empty
Queried directly via SQL (table `ZABAPGIT`, `type = 'REPO_CS'`,
`value = '000000000002'`):

```
#repo_name#OS4 6.0
@
/|.abapgit.xml|9ef91905a8d4ca37adcca6f5c03605080ea0bd01
DEVC|/LOT/OS|/LOT/OS
/src/|package.devc.xml|dc6780fbdc12024c7e176772bd1a87789e945362
NSPC|/LOT/|/LOT/OS
/src/|#lot#.nspc.xml|3dcebce1faea97d47c54aff5397dfaf535389509
```

**Only 3 rows exist**: `.abapgit.xml`, the `/LOT/OS` package's
`.devc.xml`, and the `/LOT/` namespace's `.nspc.xml`. **None of the
~17000+ real code objects (DTEL/CLAS/FUGR/etc.) have a baseline row.**
This is Finding B from earlier session slices, now confirmed by directly
reading the persisted data (not inferred).

Query recipe to reproduce this check on another system:
```sql
-- 1. find the repo's persistence key
SELECT value, data_str FROM zabapgit WHERE type = 'REPO' AND data_str LIKE '%OS4%'
-- 2. read its checksum baseline blob using that key
SELECT value, data_str FROM zabapgit WHERE type = 'REPO_CS' AND value = '<key from step 1>'
```
(Use the `execute_data_query`/ABAP-SQL MCP tool; call `get_abap_sql_syntax`
first per its own instructions.)

### 3.3 Why the baseline is empty: NOT a recurring code bug (best current theory)
Checked whether the code that's *supposed* to keep the baseline current
has a gap:
- `ZCL_ABAPGIT_REPO~DESERIALIZE` (normal Pull) calls
  `checksums( )->update( lt_updated_files )` after every pull — correct.
- `ZCL_ABAPGIT_REPO_ONLINE~PUSH` calls `checksums( )->update( ... )`
  after every push — correct.
- `ZCL_ABAPGIT_SERVICES_REPO=>NEW_ONLINE` calls
  `li_repo->checksums( )->rebuild( )` immediately after creating the repo
  link, with the comment "Make sure there're no leftovers from previous
  repos" — correct, and this almost certainly explains the 3 bootstrap
  rows: at the exact moment "New Online" ran, only `.abapgit.xml` +
  the package + namespace metadata existed locally (nothing else had
  been pulled/created yet), so `rebuild()`'s `get_files_local()` honestly
  found only those 3 things.

**Working theory** (not 100% proven, but consistent with all evidence):
the ~17000 real objects for `/LOT/OS`/`/LOT/GC` etc. were **not** brought
into this SAP system via an abapGit Pull at all — they most likely
already existed (or were transported in) independently of abapGit, and
the repo was simply *linked* on top of an already-populated package.
Because that population never went through `ZIF_ABAPGIT_REPO~DESERIALIZE`,
the `checksums( )->update(...)` call was never triggered for those files,
and nobody has ever run the manual "Update Local Checksums" action since.
**This looks like a one-time operational/setup gap for this specific
repo, not a systemic defect in the ORTEC codebase.** No code fix is
believed to be required for the baseline mechanism itself — see §4.

If reproduction in the dev system finds the SAME near-empty baseline
pattern on a **freshly, purely abapGit-pulled** repo (i.e. one that was
never touched by transport import), that would contradict this theory
and point to a real code-level gap in the pull/parallel-serialize path
instead — worth explicitly testing for in the dev system if convenient.

## 4. Fix identified (standard abapGit feature, not applied yet on IT8)

**UI path**: Repo overview page → **Advanced** menu → "Very Advanced"
section → **"Update Local Checksums"**.

Traced end-to-end:
- Menu label added in `ZCL_ABAPGIT_GUI_PAGE_REPO_VIEW=>
  BUILD_ADVANCED_DROPDOWN` (`'Update Local Checksums'`, gated by
  authorization object check `zif_abapgit_auth=>c_authorization-
  update_local_checksum`).
- Routes via `ZCL_ABAPGIT_GUI_ROUTER`'s action
  `zif_abapgit_definitions=>c_action-repo_refresh_checksums` →
  `ZCL_ABAPGIT_SERVICES_REPO=>REFRESH_LOCAL_CHECKSUMS`.
- That method shows a confirmation popup first:
  *"This will rebuild and overwrite local repo checksums. The logic: if
  local and remote file differs then: if remote branch is ahead then
  assume changes are remote, else (branches are equal) assume changes
  are local. This will lead to incorrect state for files changed on both
  sides. Please make sure you don't have ones like that."* — then calls
  `li_repo->checksums( )->rebuild( )` + `COMMIT WORK AND WAIT`.
- `rebuild()` itself (`ZCL_ABAPGIT_REPO_CHECKSUMS=>
  ZIF_ABAPGIT_REPO_CHECKSUMS~REBUILD`):
  ```abap
  lt_local  = mi_repo->get_files_local( ).
  remove_non_code_related_files( CHANGING ct_local_files = lt_local ).
  lt_checksums = build_checksums_from_files( lt_local ).
  save_checksums( lt_checksums ).
  mv_cache_valid = abap_false.
  CLEAR mt_checksums_cached.
  ```
  i.e. it re-derives the ENTIRE baseline from whatever is locally present
  right now.

**This is unmodified, standard abapGit code** — no ORTEC customization
involved in the fix path itself.

### Caveats before running it (read the popup text above carefully)
1. It adopts **whatever is currently local** as the new source of truth
   for every field. If the repo genuinely has real, not-yet-reviewed
   local modifications mixed in among the false-positive `MM` rows,
   rebuild will silently fold them into the new baseline instead of
   surfacing them for a normal stage/commit review. **Spot-check a
   sample of the `MM` files' diffs first** (as done for `/LOT/GC_GEOLON`
   in this investigation) before running it on a repo you haven't
   reviewed.
2. `rebuild()` calls `get_files_local()`, which goes through the SAME
   serialize path as the companion parked SHA1 defect
   (`.memory/future/false_modified_os4_parallel_sha1_defect.md`). If that
   defect fires DURING the rebuild pass for some file, that file's new
   baseline entry would be poisoned with a wrong value. This is still
   strictly better than today (no baseline at all: a wrong SHA1 baked
   into the baseline only misaligns ONE side for that one file on
   subsequent checks, not a blanket M/M for everything), but it means
   the rebuild result should be spot-checked afterward too, not assumed
   perfect.

## 5. Next steps to resume in the development system

1. **Check whether the same repo/object exists there** and whether it
   shows the same `MM`-with-empty-diff symptom for `/LOT/GC_GEOLON` (or
   any other file). If the dev system's OS4 repo was created differently
   (e.g. via a real abapGit Pull instead of linking onto a
   transport-populated package), the baseline might already be healthy
   there — in which case this specific issue would NOT reproduce, which
   would be useful evidence supporting the "one-time setup gap" theory
   in §3.3.
2. **If it reproduces**: confirm via the SQL queries in §3.2 that the
   checksum baseline is similarly near-empty there.
3. **Decide on/authorize the fix** with the owner: run "Update Local
   Checksums" (§4) after spot-checking a few `MM` files' diffs. Verify
   afterward that:
   - the checksum baseline SQL query now shows one row per code file;
   - the previously-`MM` files now show as unchanged in the repo
     overview;
   - a plain refresh (no full reserialize) keeps them unchanged (i.e.
     the fix is durable, not just a one-time artifact of the rebuild
     pass itself).
4. **If it does NOT reproduce** in the dev system (i.e. that repo's
   baseline is already properly populated), that's useful confirmation
   that this is a one-off IT8 setup artifact rather than a product
   defect — document that finding and close this file out as
   `NOT_REPRODUCIBLE_ELSEWHERE_LIKELY_ONE_OFF_SETUP_GAP`.
5. Optionally (not required, but would close the remaining theoretical
   gap in §3.3): if a *fresh* "New Online" + full Pull can be performed
   safely in the dev system against a repo of comparable size, check
   whether the checksum baseline ends up fully populated afterward
   (proving the pull/checksums-update path is fine end-to-end) or still
   comes out mostly empty (which would indicate a real code-level gap in
   the parallel pull path, likely worth escalating with its own
   dedicated investigation).

## 6. Read also

- `.memory/future/false_modified_os4_parallel_sha1_defect.md` — the
  separate, still-unresolved SHA1 computation defect that can trigger
  the mismatch this baseline gap turns into a permanent blanket `MM`.
  Both files describe the same underlying incident from two angles.
- `.memory/logs/variant_b_package_e_false_modified_os4_d1.md` §24 — full
  blow-by-blow investigation log (DD04L_EXTRA ruled out, full byte-level
  reference blob capture technique, the decisive "diff says no
  differences" test, the `build_existing`/baseline read, the SQL queries
  and their exact results).
- `.memory/incidents/variant_b_package_e_false_modified_os4_d0.md` — D0
  reproduction packet (owner-authored/updated repro steps and running
  status header).
- `.memory/handoffs/variant-b-package-e-e2-os4-diagnostic.md` — handoff
  summary for this incident (kept in sync with the D0/D1 status headers).
