# Variant B Slice 2 reconciliation report

## 1) ORTEC fastpath evidence

### 1.1 build_upload_pack_buffer

Source: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap)

- Method anchor: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1297](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1297)
- The method first computes whether thin is advertiseable from the caller and the existence of verified haves: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1308-L1317](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1308-L1317)
- The first want line uses capability string `side-band-64k no-progress multi_ack thin-pack ofs-delta` when thin is allowed and haves exist: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1317-L1325](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1317-L1325)
- It emits `shallow <sha>` lines before the flush pkt when `it_ortec_haves` is non-initial and `iv_force_full = abap_false`: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1330-L1337](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1330-L1337)
- The method explicitly sends a `deepen` line whenever there are no haves at all, defaulting to `deepen 1` when the caller passed `0` or blank: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1338-L1366](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1338-L1366)
- The method then appends `have <sha>` lines after the flush pkt only when `iv_force_full = abap_false` and haves are present: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1368-L1376](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1368-L1376)
- The method’s comments explicitly document the current deepen-when-no-haves behavior and why it exists: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1338-L1358](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1338-L1358)

### 1.2 upload_pack_by_branch and upload_pack_by_commit

Source: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap)

- Branch entry point: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L752](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L752)
- Commit entry point: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L928](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L928)
- Branch flow uses a three-tier retry structure:
  1. thin attempt with `iv_allow_thin = abap_true`: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L823-L836](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L823-L836)
  2. non-thin retry with `iv_allow_thin = abap_false`: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L843-L864](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L843-L864)
  3. progressive haves-free retry loop when either prior failure is marked retry-without-haves: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L879-L919](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L879-L919)
- The branch method catches `zcx_abapgit_ortec_git` and `zcx_abapgit_exception` for both the thin and non-thin tiers: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L835-L842](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L835-L842) and [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L860-L867](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L860-L867)
- The progressive loop also catches the same exception types in each retry attempt: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L894-L917](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L894-L917)
- Commit flow mirrors the same shape: thin tier [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L959-L967](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L959-L967), non-thin tier [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L973-L982](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L973-L982), progressive tier [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L989-L1018](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L989-L1018)
- The progressive deepening constants and helper methods are defined at [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L221-L239](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L221-L239)

### 1.3 is_retry_without_haves and iv_force_full usage

Source: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap)

- `is_retry_without_haves` inspects `zcx_abapgit_ortec_git` instances and reads `mv_retry_without_haves`: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1275-L1282](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1275-L1282)
- `iv_force_full` is declared on the public `build_upload_pack_buffer` entry point and on the private `upload_pack` helper: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L180-L190](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L180-L190) and [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L248-L258](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L248-L258)
- `upload_pack` uses `iv_force_full` to skip have negotiation entirely and to force the haves-free path: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1055-L1076](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1055-L1076)
- The comment in `complete_missing_object` documents the current semantics of `iv_force_full = abap_true`: no haves, no shallow lines, a single-object request with `deepen 1`: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L413-L437](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L413-L437)

### 1.4 remote-tip unchanged fast-path shortcut reading FETCH_COMMIT

Source: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap)

- The shortcut is inside `pull_by_branch`: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L555](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L555)
- It loads the repo-state row via `get_state`, then checks `ls_state-fetch_commit` and compares it with the remote tip SHA: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L679-L686](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L679-L686)
- When the tip is unchanged, it reconstitutes from the object store and returns without a new remote fetch: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L686-L737](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L686-L737)

### 1.5 capability advertisement parsing

Source: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap)

- `fetch_tip_commits` parses the server advertisement before sending the request. It extracts the null byte position, then the capability substring after it, and checks whether it contains `filter`: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L323-L355](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L323-L355)
- `try_filtered_commit_fetch` performs the same parsing logic for `filter` before emitting `want + deepen + filter blob:none`: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L462-L500](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L462-L500)
- The current ORTEC implementation does not parse `thin-pack`/`ofs-delta` capabilities inside the fastpath class itself; the capability choice is made by the local buffer builder based on the verified-have state instead: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1308-L1325](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1308-L1325)

### 1.6 current public entry points relevant to a Slice 2 explicit-fetch-mode API

Source: [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap)

- Public methods currently available:
  - `pull_by_branch` at [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L16-L32](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L16-L32)
  - `upload_pack_by_branch` at [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L34-L45](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L34-L45)
  - `upload_pack_by_commit` at [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L47-L57](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L47-L57)
  - `persist_pull_result` at [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L59-L84](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L59-L84)
  - `fetch_tip_commits` at [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L86-L108](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L86-L108)
  - `complete_missing_object` at [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L111-L128](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L111-L128)
  - `try_filtered_commit_fetch` at [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L143-L161](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L143-L161)
  - `build_upload_pack_buffer` at [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L178-L191](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L178-L191)
  - `parse` at [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L194-L206](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L194-L206)

## 2) ORTEC fetch-negotiation evidence

### 2.1 get_have_commits

Source: [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap)

- Method anchor: [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L87](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L87)
- It resolves the repo key from the URL and uses `get_complete_commits` as the base candidate list: [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L90-L100](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L90-L100)
- The method caps the candidate list at 100, removes the want hashes, expands with an ancestor walk, and then caps the combined list at 200: [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L101-L132](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L101-L132)

### 2.2 is_commit_complete

Source: [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap)

- Method anchor: [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L159](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L159)
- It calls `get_reachable_sha1s` and treats any exception as incomplete: [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L183-L194](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L183-L194)
- It then checks `has_dangling_delta_base` and only returns true when that check is clean: [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L196-L204](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L196-L204)
- The comments explicitly say this is the completeness gate for offering a commit as a have candidate: [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L159-L181](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L159-L181)

## 3) Filter-walk default lookup evidence

Source: [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap)

- The branch-based default lookup is in `get_remote_files_for_stage`: [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L90](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L90)
- When no explicit commit is selected, it reads `ls_state-fetch_commit` from `repo_state` as the default commit: [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L118-L124](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L118-L124)
- The method then uses that commit (and, if necessary, the current remote tip) to trigger a filtered cold-fetch attempt via `try_filtered_commit_fetch`: [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L132-L156](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L132-L156)

## 4) Standard abapGit transport hooks and capability handling

Source: [src/git/zcl_abapgit_git_transport.clas.abap](src/git/zcl_abapgit_git_transport.clas.abap)

- The class already exposes branch-list and branch-resolution entry points through `branch_list`, `find_branch`, and `find_branch_ortec`: [src/git/zcl_abapgit_git_transport.clas.abap#L139-L170](src/git/zcl_abapgit_git_transport.clas.abap#L139-L170), [src/git/zcl_abapgit_git_transport.clas.abap#L253-L309](src/git/zcl_abapgit_git_transport.clas.abap#L253-L309), and [src/git/zcl_abapgit_git_transport.clas.abap#L573-L586](src/git/zcl_abapgit_git_transport.clas.abap#L573-L586)
- The class builds the smart-HTTP request URI via `get_request_uri`: [src/git/zcl_abapgit_git_transport.clas.abap#L270-L272](src/git/zcl_abapgit_git_transport.clas.abap#L270-L272)
- It already owns a capability-advertisement parsing path in `branch_list` through `check_smart_response` plus `get_cdata`/branch-list parsing; there is no dedicated `filter`/`thin-pack`/`ofs-delta` parser owned by this class, however: [src/git/zcl_abapgit_git_transport.clas.abap#L139-L170](src/git/zcl_abapgit_git_transport.clas.abap#L139-L170)
- The standard upload-pack request builder in the class is a simple, single-purpose builder that emits a basic `want` sequence and optional `deepen`: [src/git/zcl_abapgit_git_transport.clas.abap#L363-L411](src/git/zcl_abapgit_git_transport.clas.abap#L363-L411)
- The class already exposes the `upload_pack_by_branch` and `upload_pack_by_commit` wrappers that delegate to ORTEC fastpath when the repo switch is active: [src/git/zcl_abapgit_git_transport.clas.abap#L412-L488](src/git/zcl_abapgit_git_transport.clas.abap#L412-L488) and [src/git/zcl_abapgit_git_transport.clas.abap#L490-L548](src/git/zcl_abapgit_git_transport.clas.abap#L490-L548)

## 5) Full ORTEC tree inventory of fetch-mode-relevant call sites

Source tree scanned: [src/ortec/git](src/ortec/git)

- [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L369-L383](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L369-L383): `filter tree:0` request in `fetch_tip_commits`
- [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L504-L511](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L504-L511): `filter blob:none` request in `try_filtered_commit_fetch`
- [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1317-L1325](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1317-L1325): thin capability advertisement (`thin-pack`, `ofs-delta`)
- [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1330-L1337](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1330-L1337): `shallow` lines emission
- [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1338-L1366](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1338-L1366): `deepen` emission when no haves exist
- [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1368-L1376](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1368-L1376): `have` lines emission after the flush pkt
- [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1434-L1440](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1434-L1440): parsing of `shallow`/`unshallow` response lines
- [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L98-L132](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L98-L132): negotiation candidate generation from complete-commit history and ancestor walk
- [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L191-L204](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L191-L204): completeness gate based on reachable SHA1s and dangling delta-base checks
- [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L142-L156](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L142-L156): filtered cold-fetch attempt based on current remote tip and default commit state
- [src/ortec/git/zcl_abapgit_ortec_repo_state.clas.abap#L166-L169](src/ortec/git/zcl_abapgit_ortec_repo_state.clas.abap#L166-L169): repo-state fields that currently carry `fetch_commit`, `is_shallow`, `deepen_lvl` (relevant to existing fetch-mode state, not a wire serializer)

### Gaps for Slice 2 design

- No current equivalent in source for the owner-approved explicit fetch-mode set `INCREMENTAL_THIN`, `INCREMENTAL_SELF_CONTAINED`, `INITIAL_BRANCH_BLOBLESS`, `MATERIALIZE_BLOBS`, and `RECOVERY_BRANCH_FULL`.
- No current equivalent in source for a single validated request serializer that owns both request construction and capability validation/unsupported-capability reporting.
- No current equivalent in source for a structured unsupported-capability result object that the serializer can return instead of only emitting a raw buffer or falling back implicitly.
- What already exists and mostly needs adaptation is the current explicit buffer-builder logic in [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap), the have-candidate logic in [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap), and the default commit selection path in [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap).
