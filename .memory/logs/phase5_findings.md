# Phase 5 findings — delta-base completeness + protocol hardening (2026-07-11)

- Model: large-reasoning tier (orchestrator), per `git-protocol`/`persistence-schema` skill
  routing (`Git protocol correctness design` is explicitly large-tier, not delegable).
- Research method: delegated read-only fact-finding to a cheap-model `Explore` subagent
  (`MAI-Code-1-Flash`) first, then personally verified the two claims that change the Phase 5
  scope decision (capability-string exclusions) directly against source, since those are the
  load-bearing facts for a correctness/risk trade-off.

## Headline: the original Phase 5 assumption in `target_design.md` §6.2 gap 1 was wrong

`target_design.md` said: *"No table currently records 'object X is an OFS/REF delta whose base
is Y.' Required to accept thin packs safely..."* This is **incorrect** — the schema and code
already have this:

- `ZAOG_PACK_IDX` already has a `DELTA_BASE CHAR(40)` column (12 fields total today).
- `zcl_abapgit_ortec_pack_dec.clas.abap` already populates it for `OBJ_REF_DELTA` entries
  (`ls_idx-delta_base = lv_ref_delta`) and already has a working (if coarse) missing-base
  repair path: a targeted bulk SELECT on the SHA1s actually referenced by the pack, falling
  back to one full non-targeted `zaog_obj_store` reload only if a base is still absent.
- Standard `src/git/zcl_abapgit_git_delta.clas.abap=>delta()` (the actual byte-level delta
  APPLICATION algorithm, shared by both standard and Ortec decode paths) already has an
  ORTEC-added fallback: if a ref-delta's base SHA1 isn't in the current in-memory object batch,
  it calls `zcl_abapgit_ortec_obj_store=>get_object` before giving up. This is a narrow,
  already-existing, already-shipped touch of nominally-standard code.

So `REF_DELTA` (git pack type 7) already has base tracking, a repair path, and a working apply
algorithm end-to-end. There is no `ZAOG_DELTA` table and none is needed for what's actually
missing (see below) — reuse `ZAOG_PACK_IDX.DELTA_BASE`, do not add a new table.

## The real gap: `OBJ_OFS_DELTA` (git pack type 6) is not implemented anywhere - but is currently unreachable, not a live bug

- `zcl_abapgit_git_pack=>get_type` and `zcl_abapgit_ortec_pack_dec`'s equivalent `get_type`
  both mask the pack-entry type byte and only branch on commit(16)/tree(32)/blob(48)/tag(64)/
  ref_delta(112). The ofs-delta bit pattern (96 = `6 << 4`) falls into `WHEN OTHERS` and raises
  `Todo, unknown git pack type` - in **both** the standard and the Ortec decoder. This is a
  pre-existing, standard-abapGit-wide gap, not something the Ortec rework introduced or can be
  blamed for.
- **Verified directly (not just inferred):** `src/git/zcl_abapgit_git_transport.clas.abap`
  line ~378 sends capability string `'side-band-64k no-progress multi_ack'` for every upload-
  pack request, standard or Ortec. **`ofs-delta` is not advertised, and neither is
  `thin-pack`.** Per the Git smart-HTTP protocol spec, a compliant server MUST NOT send
  `OBJ_OFS_DELTA`-encoded entries or a thin pack (delta bases outside the transmitted pack) to
  a client that didn't advertise support for them. This means the `OBJ_OFS_DELTA` gap is
  **dead code today, not a live/silent bug** - it is not why performance gains have been
  historically inconsistent, and fixing it delivers **zero** benefit on its own.
- Consequence for the existing `DELTA_BASE`/missing-base repair path: since `thin-pack` is
  also not advertised, every pack the server sends today should already be self-contained
  (every ref-delta's base included in the same pack). The existing "missing base -> fallback"
  code path is therefore understood to primarily serve the **crash/timeout resume** scenario
  (a prior decode of the *same* pack was interrupted after persisting some objects; on resume,
  a later ref-delta needs a base that was already persisted before the interruption) rather
  than genuine cross-pack thin-pack resolution. This matches its own doc comment ("edge case
  after incomplete earlier fetches").

## Why this matters for scope: enabling thin-pack is a real opportunity, but larger than assumed

D3 (Michael's decision) asked for thin-pack to be allowed once base completeness is verified -
this is a genuine, real bandwidth optimization opportunity for "remote Git buffering"
(incremental branch-switch/pull fetches would no longer re-transmit objects the client's
`have` lines already proved it possesses). But requesting `thin-pack` capability and having a
server actually honor it makes `OBJ_OFS_DELTA` a **live** code path for the first time (real
Git servers overwhelmingly prefer ofs-delta over ref-delta for pack compactness once any
delta reuse against existing objects is in play). So:

**Enabling thin-pack without first implementing OFS_DELTA decode would very likely turn
previously-dead code into an active decode failure for most real repositories** - the opposite
of the intended performance win, and on the Ortec write/protocol path specifically (guarded by
the opt-in), so it would look like "the fastpath is broken" rather than "standard fallback",
unless the failure is caught and falls back cleanly (need to verify this too before shipping).

## Good news: the OFS_DELTA implementation itself is smaller than it first appears

`zcl_abapgit_git_delta=>delta()` (the byte-level apply algorithm: skip 2 header varints, then
loop over copy-from-base / insert-literal instructions) operates purely on `lv_base` (the
base's already-decoded raw bytes) and is **completely agnostic to how the base was located**.
The only new work for OFS_DELTA support is:
1. A new pack-entry type constant (git type 6) and a `get_type` branch for bit-pattern 96.
2. A new varint decoder for git's ofs-delta-specific negative-offset encoding (base-128,
   MSB-first, continuation bias) - a small, self-contained, spec-defined algorithm, not
   currently present anywhere in this codebase.
3. Tracking pack-offset -> object identity during decode so `current_offset - negative_offset`
   can be resolved to a base object. `ZAOG_PACK_IDX.PACK_OFFSET` already exists and is already
   populated per object in the Ortec incremental decoder, so this is close to "read a column
   that already exists" for the Ortec path; standard `zcl_abapgit_git_pack=>decode` has no
   persisted offset map today and would need an in-memory one for the same pack decode pass.
4. Once the base is resolved to a SHA1/data, everything downstream (the `delta()` apply call)
   is unchanged.

This is real, scoped, protocol-correctness work (git pack format spec compliance), not
guesswork - but it is materially more than "add two columns to a table," and a mistake here
has genuine data-corruption risk (the whole reason this project treats delta-base handling as
a correctness-critical area).

## Recommendation (presented to Michael, not yet decided)

Given the size/risk now clarified, recommend against silently expanding scope. Options to put
to Michael:
- **(A) Full D3 as originally intended**: implement `OBJ_OFS_DELTA` decode (new type + varint
  decoder + offset tracking) as a prerequisite, THEN request `thin-pack` capability only on the
  Ortec-guarded negotiation path, gated by a completeness check before trusting/using it. This
  is the real performance win but is the largest, highest-risk slice remaining in the whole
  project - likely warrants its own dedicated design-detail pass (schema for offset-tracking
  if persisted, the exact varint algorithm, exact capability-negotiation call site, exact
  fallback-to-non-thin behavior) before any code is written, consistent with the design-gate
  rule already applied to every other phase.
- **(B) Skip thin-pack for now.** Keep the existing (working, already-shipped) REF_DELTA
  handling and its crash-resume repair path exactly as is. Spend this slice on a smaller,
  lower-risk, still-valuable piece instead - e.g. the Phase 6 admin report (off the hot path,
  low risk), or building the deferred unified status engine, or hardening the existing
  "missing base -> full non-targeted store reload" fallback to be SHA1-targeted instead of a
  full-repo SELECT (a real, bounded, low-risk performance fix for very large repos, using
  infrastructure that already exists).
- **(C) Something else Michael specifies.**

No code changed in this investigation. No schema changed. No capability string changed.
