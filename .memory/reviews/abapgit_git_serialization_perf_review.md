# abapGit — Git Handling and Object Serialization Performance Discovery/Backlog — DESIGN_GATE Review

```text
PACKET=COMPACT_HANDOFF_V1
TASK=ABAPGIT_GIT_SERIALIZATION_PERFORMANCE_DISCOVERY (review of discovery+backlog pair)
MODE=DESIGN_GATE
REVIEWED_ARTIFACTS=.memory/logs/abapgit_git_serialization_perf_discovery.md,
  .memory/logs/abapgit_git_serialization_perf_backlog.md
```

## Scope note

Read-only review. No candidate (F-1..F-5) proposes an implementation slice;
"design" here is the backlog's own candidate definitions, evidence, scale
variables, and measurement prerequisites. Verdict answers: is this backlog
accurate and evidence-disciplined enough to safely hand off as-is?

## 1. Evidence-classification discipline

Followed correctly throughout both files. Hedged statements (`UNKNOWN`,
`NOT_INVESTIGATED`, `MEASURE_FIRST`) are used honestly where the doc admits
it did not trace something (e.g. §2.5, §3.6, the M-4 refresh()-trigger
question, F-4's conditional "IF M-4 confirms" phrasing). No statement was
found that is flatly presented as settled fact while actually being an
unverified hypothesis — the closest case (§2.2's second/third `branches()`
GET inside `upload_pack_by_branch`) is itself qualified in-line with "not
re-traced to exact line this pass," and that qualifier turns out to be
warranted (see Finding 1). No violation of evidence discipline serious
enough to mislabel a candidate's readiness.

## 2. Spot-checks against cited source (6 performed, minimum 3 required)

| # | Claim | File:line cited | Result |
|---|-------|------------------|--------|
| 1 | TADIR `select_objects` SELECTs whole package tree; `it_filter` applied as post-SELECT in-memory `LOOP...DELETE` | `zcl_abapgit_tadir.clas.abap:271-330, 499-525` | **Confirmed.** `select_objects` at line 273, `FOR ALL ENTRIES IN et_packages` (no object/obj_name restriction); filter loop at line 499 exactly as described. |
| 2 | `zcl_abapgit_serialize=>add_objects` passes already-narrowed `lt_tadir` into `serialize()` | `zcl_abapgit_serialize.clas.abap:228-254` | **Confirmed.** `add_objects` at line 226; `read(...) → lo_filter->apply(...) → serialize( it_tadir = lt_tadir )` exactly as described. |
| 3 | `zif_abapgit_repo~refresh` unconditionally sets `mv_request_local_refresh = abap_true`; `get_files_local`'s cache short-circuit is bypassed by that flag | `zcl_abapgit_repo.clas.abap:796-810, 662-720` | **Confirmed.** `refresh` at 796 (flag set at 798, unconditional); `get_files_local` short-circuit `IF lines(mt_local)>0 AND mv_request_local_refresh=abap_false` at line 667. |
| 4 | `zcl_abapgit_ortec_obj_index` write-chunk constant is live at 30000, not the design's documented 5000 | `zcl_abapgit_ortec_obj_index.clas.abap:75` | **Confirmed exactly.** `CONSTANTS c_index_write_chunk_size TYPE i VALUE 30000.` at line 75; matches state.md's own POSTPONED note verbatim. |
| 5 | `rebuild_index`/`get_files_for_filter` do a fresh level-by-level tree walk on first touch, then reuse via a `FOR ALL ENTRIES` filtered SELECT | `zcl_abapgit_ortec_obj_index.clas.abap` (rebuild_index, get_files_for_filter) | **Confirmed.** `get_files_for_filter` calls `ensure_index(...)` then `select_rows_for_filter` (`FOR ALL ENTRIES IN it_filter` at line 510); `rebuild_index` gates on `is_index_ready`, deletes stale rows, then walks `lt_pending`/`lt_next` level by level. |
| 6 | `zcl_abapgit_git_transport=>branches(iv_url)` "always performs a fresh info/refs HTTP GET" is the mechanism behind pull_by_branch's own call and filter_walk's own call | `zcl_abapgit_git_transport.clas.abap:131-166`; `zcl_abapgit_ortec_filter_walk.clas.abap:132`; `zcl_abapgit_ortec_porcelain.clas.abap` (comment ~191, cited ~196-199) | **Confirmed as to the callers and the end effect** (both call sites do invoke the static `branches()` wrapper, which forwards to `zif_abapgit_git_transport~branches` → `branch_list` → real HTTP GET; the "disclosed-but-accepted" comment is at line 191, not 196-199, a trivial ~5-line citation drift). **Not confirmed as to the "second GET inside upload_pack_by_branch" causal path — see Finding 1.** |

## 3. Finding 1 (MAJOR, not blocking) — §2.2 / F-2 evidence chain misattributes the second/third redundant GET

`§2.2` and `F-2`'s `CURRENT_PATH`/`SCOPE` state that a second branch
resolution "happens again inside `zcl_abapgit_git_transport=>upload_pack_by_branch`"
for `INCREMENTAL_UPDATE`, citing that method's "standard implementation
calls `find_branch`." Reading `zcl_abapgit_git_transport.clas.abap:412-460`
shows this is **not reachable for ORTEC-active repos**: the method's
`TRY` block around `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch`
either `RETURN`s on success or `RAISE`s the original exception on
failure — there is no fallthrough to the standard `find_branch` call for
any repo where `zcl_abapgit_ortec_git_switch=>is_active_for_repo` is true.
The standard fallthrough only fires for non-ORTEC repos, which is not the
scenario F-2 is scoped to.

Separately, `zcl_abapgit_ortec_fastpath.clas.abap` contains its own,
distinct `pull_by_branch` method (different class from
`zcl_abapgit_ortec_porcelain=>pull_by_branch`, which is the only entry
point Workstream A's "Branch pull" section documents) that performs its
own independent `zcl_abapgit_git_transport=>branches(iv_url)` call
("Discover remote branch tip early"), plus several more
`find_branch_ortec`/`branches` call sites elsewhere in that class. None of
this is mentioned in the discovery's Workstream A entry-point table or in
F-2's `SCOPE`.

Net effect: the qualitative conclusion ("branch/ref requests are
repeated," §2.2/mandatory-Q&A) still holds and is plausibly an
**understatement** rather than an overstatement — but F-2's `SCOPE` names
"the standard `zcl_abapgit_git_transport=>upload_pack_by_branch` call
chain" as one of three classes needing cross-class design work, when the
actual redundant-call mechanism for the ORTEC-active path this candidate
cares about appears to live inside `zcl_abapgit_ortec_fastpath` instead
(including an entirely separate `pull_by_branch` method not referenced
anywhere in either document). A future F-2 design that trusts this
citation literally would scope its investigation to the wrong class.

**Required fix (minor revision, not a re-review trigger):** before F-2
design begins, correct `CURRENT_PATH`/`SCOPE` to name
`zcl_abapgit_ortec_fastpath` (both its `upload_pack_by_branch` delegate
and its own `pull_by_branch` method) as the mechanism to trace, and drop
or re-qualify the "standard `upload_pack_by_branch` call chain" framing
for the ORTEC-active case. This does not change F-2's `DESIGN_REQUIRED`
disposition or any other candidate's ranking.

## 4. Finding 2 (MINOR) — Workstream A HTTP_CALLS bucket conflates two different "warm" cases

The single-object Diff/Stage HTTP_CALLS row ("0 warm/already-indexed
commit... or 1 info/refs GET... cold/moved branch") does not distinguish
a caller with an already-pinned commit (`get_selected_commit()` non-blank
→ genuinely 0 GETs, `get_remote_files_for_stage` skips the whole tip-
resolution block) from a caller resolved via branch name whose index
happens to already be fresh (still issues exactly 1 GET to *establish*
freshness, per the code path read at
`zcl_abapgit_ortec_filter_walk.clas.abap:95-165`). Cosmetic only —
doesn't change any candidate's evidence or ranking.

## 5. Cross-reference and postponement fidelity (requirements 3-5)

- **No `IMPLEMENT_NEXT`**: confirmed justified. Every candidate's ranking
  row shows `UNKNOWN` measured cost share, and each has a live
  `REJECT_IF` condition tied to a not-yet-run measurement (M-1..M-4). Not
  overly conservative (concrete, falsifiable rejection criteria exist,
  e.g. F-1/F-5's "negligible at scale" outs) nor overly permissive (no
  candidate is elevated past its own open questions).
- **F-3 → E1-TREE-REUSE**: confirmed correctly deferred, not new work.
  Backlog's quoted constraint ("PROVEN UNSAFE as a bare tree-SHA1 key...
  composite key of at minimum (tree_sha1, hash-of-.abapgit-content,
  devclass)") matches `.memory/state.md`'s Deferred-topics entry verbatim.
- **E1_OBJINDEX_PERFORMANCE / E2_CONSUMER_COHERENCE**: confirmed treated
  as cited-evidence-only, not reopened. Backlog's "REJECT for this
  backlog... explicit owner-postponed topics as of 2026-07-31" matches
  `.memory/state.md`'s "2026-07-31 owner decision" section exactly,
  including the 30000-vs-5000 discrepancy framing (verified against
  source in spot-check #4 above, not re-litigated).

## 6. Risk-rating check against Variant B invariants (requirement 6)

No candidate's `CORRECTNESS_RISK`/`PERSISTENCE_OR_PROTOCOL_IMPACT` looks
understated against `.github/skills/git-partial-clone/SKILL.md`'s
invariants (no deepen/shallow, no per-object SQL/HTTP, bounded bulk
windows). F-1/F-5 are local-DDIC-only and correctly rated LOW.
F-3/F-4 are correctly rated HIGH/MEDIUM-HIGH given DDIC/broad-consumer
impact. F-2's `MEDIUM` correctness rating is defensible (it already
requires a dedicated design + correctness review "given the branch-move
detection role of these calls" and explicitly excludes touching
`ZAOG_REPO_STATE`) — reducing to fewer info/refs GETs is itself aligned
with, not in tension with, the "no per-object HTTP" spirit of the
invariants. One observation, not a required fix: given F-2 sits adjacent
to the still-open `E2_CONSUMER_COHERENCE` class of stale-remote-state
bugs, its eventual design should explicitly cross-check against that
failure mode once E2 resumes — worth a one-line note in F-2's
`PREREQUISITES`, not a rating change.

## 7. Verdict

```text
VERDICT=APPROVE_WITH_MINOR_REVISIONS
```

Rationale: evidence discipline is sound, postponed-topic handling is
faithful to `.memory/state.md`, no candidate is prematurely authorized,
and 5 of 6 spot-checked claims matched cited source exactly. One MAJOR,
non-blocking finding (Finding 1) misattributes the causal mechanism for
part of F-2's evidence to the wrong call chain and omits a whole sibling
method (`zcl_abapgit_ortec_fastpath=>pull_by_branch`) from the discovery's
own entry-point inventory; this should be corrected before F-2 enters
design, but does not change any candidate's decision, ranking, or this
backlog's overall safety to hand off.
