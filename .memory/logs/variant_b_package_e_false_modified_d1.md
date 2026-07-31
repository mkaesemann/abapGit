# Variant B / Package E — D1 Diagnostic Log — False Local+Remote MODIFIED (abapGit-testing, SMALL REPO)

```text
STATUS=SUPERSEDED_INVALID_REPRODUCTION
INVALIDATED_BY_OWNER_EVIDENCE=2026-07-30 (owner ran `git fetch origin` on the
  local abapGit-testing clone used to compare demo-branch and
  demo/demo-branch)
REASON=stale local Git clone; after git fetch origin the compared branches
  had different tips; small repository does not reproduce false MODIFIED
ROOT_CAUSE_ID=RETRACTED
R12=NOT_ESTABLISHED
E2_FIX_AUTHORIZED=NO
```

The successor active incident is the OS4 repository:
[.memory/logs/variant_b_package_e_false_modified_os4_d1.md](.memory/logs/variant_b_package_e_false_modified_os4_d1.md).

## Claims validity split (post-retraction)

**INVALID:** the root-cause matrix in §6 below and the "leading candidate
R12" framing in §5 — both were conclusions drawn from the now-invalidated D0
reproduction and must not be cited as current evidence.

**STILL USEFUL (static source facts, reusable for OS4):** the call graph in
§1, the comparison-input matrix in §2 (repo-GUID-scoped checksums,
live/uncached branch resolution, content-addressed object store,
dead-code status of the legacy pull/blob buffer), and the elimination
reasoning behind R1/R2/R3/R5/R9/R10 in §6, which rest on direct source reads
independent of the specific (invalid) reproduction and may inform — but must
be independently re-checked against — the OS4 call graph, since OS4 uses a
different access mode (repository overview / Full Stage vs. this file's
unfiltered-Stage-only trace) and may traverse additional ORTEC paths (e.g.
OBJ_INDEX) not exercised here.

---

Scope (historical, INVALID reproduction): D0+D1 only (read-only). No fix, no D2/D3 instrumentation, no
`.memory/state.md` write, per Package E E2 diagnostic-ladder authorization.

## 1. Verified call graph (Full unfiltered Stage path)

```
Stage page render
 └─ zcl_abapgit_repo_status=>calculate( ii_repo )
     ├─ ii_repo->get_files_local( )                     [standard, live re-serialize, branch-independent]
     ├─ ii_repo->get_files_remote( )
     │   └─ zcl_abapgit_repo_online=>fetch_remote( )     [standard]
     │       IF mv_request_remote_refresh = abap_false → RETURN (serve mt_remote)
     │       ELSE:
     │       └─ zcl_abapgit_git_porcelain / ORTEC hook
     │           └─ zcl_abapgit_ortec_porcelain=>pull_by_branch(
     │                 iv_url, iv_branch_name = get_selected_branch() )
     │               ├─ lv_target_commit = zcl_abapgit_git_transport=>branches(iv_url)
     │               │                       ->find_by_name(iv_branch_name)-sha1   [LIVE HTTP call, exact-match lookup]
     │               ├─ zcl_abapgit_ortec_have_policy=>classify_operation(
     │               │     iv_repo_key, iv_target_commit )                         [keyed by (repo_key, commit) ONLY — branch-agnostic by design]
     │               │   → WARM_UNCHANGED | INCREMENTAL_UPDATE | COLD_BRANCH
     │               └─ pull( iv_commit = lv_target_commit, ... )                  [content-addressed: walk_tree + materialize_from_manifest from zaog_obj_store]
     └─ li_instance = zcl_abapgit_status_calc=>get_instance( root_package, dot_abapgit )
         rt_results = li_instance->calculate_status(
             it_local, it_remote,
             it_cur_state = ii_repo->checksums( )->get_checksums_per_file( ) )     [keyed by repo GUID only — branch-independent]
             └─ build_existing: match = (local_sha1 = remote_sha1); if match, no flags;
                else compare each side to baseline → lstate/rstate
```

Branch switch path (only mechanism found that changes `ms_data-branch_name` on
an existing repo entity):

```
zcl_abapgit_ortec_branch_list=>perform_switch
  li_repo_online = zcl_abapgit_repo_srv=>get_instance( )->get( mv_key )
  li_repo_online->select_commit( '' )
  li_repo_online->switch_origin( '' )
  li_repo_online->select_branch( is_branch-name )     [calls reset_remote() FIRST, then set()]
  COMMIT WORK AND WAIT
```

`zif_abapgit_repo_online~select_branch` (confirmed by direct read,
`zcl_abapgit_repo_online.clas.abap:377-386`):
```abap
METHOD zif_abapgit_repo_online~select_branch.
  reset_remote( ).
  IF zcl_abapgit_user_branch=>is_user_branch_active( ).
    zcl_abapgit_user_branch=>select_branch( iv_url = get_url( ) iv_branch = iv_branch_name ).
  ELSE.
    set( iv_branch_name = iv_branch_name iv_selected_commit = space ).
  ENDIF.
ENDMETHOD.
```
`reset_remote()` clears `mt_remote` and sets `mv_request_remote_refresh =
abap_true` — this is CORRECT, standard cache invalidation on branch switch.

## 2. Comparison-input matrix

| Input | Scope key | Branch-dependent? | Verified correct for this repro? |
|---|---|---|---|
| `it_local` (local files) | SAP system objects, live re-serialize | No (same devclass, same objects regardless of selected branch) | Yes — deterministic, standard, unmodified |
| `it_cur_state` (checksums baseline) | Repo GUID (`mv_repo_key`) only | No | Yes — `zcl_abapgit_repo_checksums` keyed by `ty_repo-key`, not branch; confirmed by source read |
| `lv_target_commit` | Live HTTP branch-list lookup per Stage render | Yes (by design) | Live call confirmed (`zcl_abapgit_git_transport~branches`), exact-match `find_by_name` (table key lookup, not pattern match) — no slash-in-name ambiguity found |
| Object-store reconstruction (`pull()`) | `(repo_key, commit_sha1)` content-addressed | No (intentionally shared across branches with identical commit) | Correct-by-construction; confirmed via source read of `walk_tree`/`materialize_from_manifest` |
| `classify_operation` | `(repo_key, commit_sha1)` | No (intentionally shared) | Correct-by-construction for identical commits |
| `zcl_abapgit_pull_buffer` / `zcl_abapgit_blob_buffer` | `(url, branch)` | Yes | **Dead code** — calls in `zcl_abapgit_git_porcelain.clas.abap:557,640` are commented out (`**`). Not in active call path. Ruled out. |

## 3. Persisted-index vs independent-tree result

Not applicable — the reported access mode is "Full unfiltered Stage", which
does not use `zaog_obj_index`/OBJ_INDEX at all (that path is exclusive to
filtered/Stage-by-Transport access via `zcl_abapgit_ortec_filter_walk`). OF-1
(stale OBJ_INDEX row) is therefore **NOT_APPLICABLE** to this specific
reported occurrence (it remains a separate, valid, previously-documented
concern for the filtered path only).

## 4. Live DB evidence (IT8, read-only `SELECT`, this session)

`ZAOG_REPO_STATE` for `repo_key = '2296bfba73b0'` (derived from the Azure
DevOps `abapGit-testing` URL):

| BRANCH_NAME | CURR_COMMIT / FETCH_COMMIT | IS_SHALLOW | FETCH_TS |
|---|---|---|---|
| refs/heads/newest-branch | fce5f4d0b7246a8dd5b6a9034aa10325d4f97831 | (blank) | 2026-07-28 14:08:04 |
| refs/heads/demo-branch | **7350d6c36fb38f642d598be6eb555fcccac3a4c1** | X | 2026-07-28 14:08:15 |
| refs/heads/demo/alternative-branch | 8c0a56f3dd36fc29bb280bf66c98338b7f12816a | X | 2026-07-30 11:01:29 |
| refs/heads/demo/demo-branch | **cacac3bb4e9f5505a4c0d24ffe70631b3c0571e5** | X | 2026-07-30 11:03:09 |

`ZAOG_COMMIT_HIST` for the same `repo_key`: all four commits above are
recorded with `HIST_LEVEL = 'F'` (FULL_COMPLETE).

`ZABAPGIT` (standard abapGit repo persistence) for this URL: **exactly one**
repo entity —
`VALUE=000000000004, BRANCH_NAME=refs/heads/demo-branch,
PACKAGE=ZOR_ABAPGIT_TEST, DESERIALIZED_AT=20260722155508` (last actually
deserialized/pulled into the SAP system 2026-07-22 — 6-8 days before either of
the `demo-branch`/`demo/demo-branch` fetch timestamps above).

### Key observation (unexplained asymmetry — the evidence gap)

The ORTEC-cached `fetch_commit` for `demo-branch` (`7350d6c3...`, cached
2026-07-28) is **not the same SHA1** the owner asserts both branches currently
share (`cacac3bb...`, only certified via `demo/demo-branch`'s fetch on
2026-07-30). This cached field is not itself used as the reconstruction commit
(the live `find_by_name` result is used instead — see call graph above), so it
does not by itself prove a bug. But it also means **this diagnostic session
has no independent, live confirmation that `demo-branch`'s current remote tip
is actually `cacac3bb...` today** — that claim rests entirely on the owner's
own observation.

## 5. First wrong decision or evidence gap

No single incorrect ABAP decision could be pinpointed and proven via static
source reading + read-only DB query alone. Every cache/comparison mechanism
inspected (local serialization, checksum baseline, live branch resolution,
content-addressed object store, classify_operation) is either:
- provably branch-independent and correct (checksums, object store,
  classify_operation), or
- provably live/uncached at the point that matters (branch-list HTTP call,
  `find_by_name` exact-match), or
- provably not in the active call path (legacy blob/pull buffer).

**Evidence gap (blocks CONFIRMED verdict):** this diagnostic cannot
independently verify, without live runtime instrumentation (D2, not
authorized) or a fresh live git branch-list check taken at reproduction time,
whether `refs/heads/demo-branch` and `refs/heads/demo/demo-branch` **actually**
resolved to the identical commit SHA1 at the moment each Stage view was
rendered. If they did not (e.g. `demo-branch` was still effectively serving
`7350d6c3...`'s content at that moment, or `demo/demo-branch`'s live tip
differed from what the owner checked), the reported result set is not
proof of a false-MODIFIED bug — it would be a correct result for two branches
whose live tips genuinely differed at that instant.

Given the content-addressed design is correct-by-construction whenever the
resolved commit truly is identical, the **leading candidate mechanism if the
commits are confirmed identical** is a gap the current code inspection did
not localize to a specific statement — i.e. **ROOT_CAUSE=PARTIAL**, not a
confirmed line-level defect.

## 6. Root-cause matrix (R1–R12)

| ID | Description | Verdict | Evidence |
|---|---|---|---|
| R1 | Stale-but-present OBJ_INDEX row | NOT_APPLICABLE | Access mode is unfiltered Stage; OBJ_INDEX not consulted (§3) |
| R2 | OBJ_INDEX row wrong commit | NOT_APPLICABLE | Same as R1 |
| R3 | Remote-file-reconstruction changes identity | CONTRADICTED | `pull()`/`walk_tree`/`materialize_from_manifest` content-addressed by `(repo_key, commit)`; correct-by-construction (§1, §2) |
| R4 | Local checksum differs despite same content | NOT_VERIFIED | Requires live runtime capture of actual `local_sha1`/`remote_sha1` per file (D2) |
| R5 | Stale baseline (`it_cur_state`) | CONTRADICTED (as sole cause) | Baseline is repo-GUID-scoped, identical for both branch views of the same entity (§2); can only affect which *side* is flagged, not whether a mismatch exists |
| R6 | Path/filename normalization mismatch | NOT_VERIFIED | Not traceable without live file-list capture for both branches (D2) |
| R7 | chmod/mode mismatch | NOT_VERIFIED | Not inspected this session; low prior probability for TABL/PROG objects |
| R8 | Line-ending/serialization difference | NOT_VERIFIED | Not traceable without live byte-level capture (D2) |
| R9 | Partial filtered access compared as full | NOT_APPLICABLE | Confirmed unfiltered access mode (owner-stated) |
| R10 | NOT_BUFFERED/UNKNOWN becomes MODIFIED | NOT_VERIFIED | Legacy `zcl_abapgit_pull_buffer`/`blob_buffer` ruled out as dead code (§2); no other NOT_BUFFERED-style state found in the active path |
| R11 | UI rendering maps state incorrectly | NOT_VERIFIED | Not inspected this session (would require UI-layer trace) |
| R12 | Concurrent/sequential branch switch mixes inputs | PARTIAL / LEADING CANDIDATE | `select_branch()` correctly calls `reset_remote()` before switching (§1) — the one concrete switch path found is NOT visibly defective. But the fundamental unresolved question (§5) — whether the two branches' live tips were truly identical at reproduction time — remains open, and this is the category the owner's own hypothesis ("previously opened branch") points to |

## 7. Ownership

Mixed: the call graph crosses both ORTEC-owned code
(`zcl_abapgit_ortec_porcelain`, `zcl_abapgit_ortec_have_policy`,
`zcl_abapgit_ortec_branch_list`) and standard abapGit code
(`zcl_abapgit_repo`, `zcl_abapgit_repo_online`, `zcl_abapgit_repo_checksums`,
`zcl_abapgit_status_calc`, `zcl_abapgit_git_branch_list`,
`zcl_abapgit_git_transport`). No standard-abapGit defect was found or is
suspected at this point (`select_branch`/`reset_remote`/`find_by_name`/
checksums scoping are all confirmed correct by direct source read) — the
open question is scoped to ORTEC's own branch-switch-to-commit-resolution
timing/consistency, not the comparison algorithm itself.

## 8. Minimum safe corrective design (if confirmed) — NOT AUTHORIZED THIS PHASE

Not produced. E2_FIX_AUTHORIZED=NO for this phase; a corrective design
requires a separate, explicitly authorized E2-FIX design/review cycle once
the evidence gap in §5 is closed.

## 9. Required tests / live retest plan (for the NEXT authorized phase)

1. At the moment of reproduction, capture the LIVE branch-list result for the
   `abapGit-testing` URL (via the existing branch-picker UI, which is
   confirmed to make a live call — see §1) for both `demo-branch` and
   `demo/demo-branch`, and record their SHA1s side by side.
2. If the SHA1s differ at that moment: this is not a defect — document the
   real (non-bug) explanation and close the incident.
3. If the SHA1s are confirmed identical: authorize D2 (bounded runtime trace)
   to capture the actual `lv_target_commit` value and the actual per-file
   `local_sha1`/`remote_sha1` pair used by `zcl_abapgit_status_calc` for both
   Stage renders, to localize the exact statement producing the divergence.
