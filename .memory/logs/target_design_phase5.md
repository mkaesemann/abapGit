# Target design — Phase 5: OFS_DELTA decode + thin-pack negotiation

- **Author:** ortec-abapgit-design (Claude Opus 4.8, large-reasoning tier)
- **Date:** 2026-07-11
- **Status:** DESIGN ONLY — no productive ABAP changed. Implementation-detail plan for the
  **full** version of Phase 5, approved in scope by Michael (implement OFS_DELTA decode as a
  prerequisite, then enable `thin-pack`/`ofs-delta` on the Ortec-guarded path only, gated by a
  delta-base completeness check). Coding starts only on Michael's explicit go-ahead per the
  design-gate rule.
- **Supersedes for Phase 5:** `target_design.md` §6.2 gap 1 (which wrongly assumed no
  delta-base tracking exists). Ground truth is `phase5_findings.md`; this document builds on it
  and does **not** re-derive those facts.
- **Companion outputs:** `.memory/diagrams/phase5_ofs_delta_flow.mmd`,
  `.memory/decisions/phase5_ofs_delta_review_required.md`.
- **Verified source anchors (read this session):**
  `src/git/zcl_abapgit_git_pack.clas.abap` (`decode`, `get_type` L660, `get_length` L614),
  `src/git/zcl_abapgit_git_delta.clas.abap` (`decode_deltas`, `delta`),
  `src/git/zcl_abapgit_git_delta.clas.locals_imp.abap` (`lcl_stream`, `eat_offset_and_length`),
  `src/git/zif_abapgit_git_definitions.intf.abap` (`c_type` L122-128),
  `src/git/zcl_abapgit_git_transport.clas.abap` (`upload_pack` capa L378, `upload_pack_by_branch`
  CATCH L419),
  `src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap` (`resumable_decode` L693, `get_type`
  L1234, DELTA_BASE/PACK_OFFSET population L1012, targeted base prefetch L1085+),
  `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap` (`upload_pack` capa L544, have/deepen
  suppression L554-582, filter capa L201),
  `src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap` (`get_have_commits`),
  `src/ortec/git/zcl_abapgit_ortec_pack_index.clas.abap` (`ty_index_entry`, `pack_offset`,
  `delta_base`), `src/ortec/git/zaog_pack_idx.tabl.xml` (schema),
  `src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap` (`is_index_ready`, `__READY__` marker).

---

## 0. Executive summary

Enabling `thin-pack` makes the currently-dead `OBJ_OFS_DELTA` (git pack type 6) a **live**
decode path, because real Git servers overwhelmingly prefer ofs-delta once any delta reuse is
allowed. Therefore OFS_DELTA decode must be implemented and unit-proven **before** the
capability is advertised. The whole feature lives **exclusively on the Ortec opt-in path** (D1
Option B), so the standard `zcl_abapgit_git_pack` / `zcl_abapgit_git_delta` /
`zif_abapgit_git_definitions` need **zero** changes — the strongest possible D7 outcome. The
work is:

1. A spec-exact OFS negative-offset varint decoder (`get_offset`).
2. Offset→base resolution via an in-memory `offset → entry` map (the Ortec decoder already
   stores `PACK_OFFSET` per object; the map is that column, in memory).
3. A small Ortec-owned **unified delta resolver** (`zcl_abapgit_ortec_delta`) that resolves
   ref **and** ofs bases (including delta chains and thin/`obj_store` bases) in dependency order
   and reuses the byte-level copy/insert apply algorithm.
4. A **completeness gate** (`get_verified_have_commits`) so `thin-pack`/`ofs-delta` are
   advertised only when every offered `have` base is verified-complete locally.
5. A **fail-safe cascade** (thin → non-thin Ortec → standard) that never promotes partial data
   to the resolved store.

Rollout is strictly ordered: **5a decode-only (dead code, unit-tested)** → **5b enable
capability + gate + cascade** → **5c persistence/eviction tie-in**.

---

## 1. Requirement 1 — OFS_DELTA negative-offset varint decoding (exact algorithm)

### 1.1 Where the bytes sit in a pack entry
An `OBJ_OFS_DELTA` entry is laid out as:

```
[ type+size varint ]   <- get_length() already handles this; type nibble = 6 (byte & 0x70 = 0x60 = 96)
[ ofs varint      ]    <- NEW: the negative base offset (this section)
[ zlib(delta instructions) ]
[ 4-byte adler32  ]    <- Ortec path skips 4 bytes after the DEFLATE stream
```

Contrast with `OBJ_REF_DELTA`, where the 20-byte base SHA1 sits where the ofs varint is. The
existing `resumable_decode` already reads that 20-byte SHA (L835-838); the ofs branch reads the
varint instead.

### 1.2 The exact git offset encoding (base-128, MSB continuation, **+1 bias per continuation**)
This is **not** the same encoding as `get_length` (which is `size` encoding). It is the
"offset encoding" from the official git pack format
(`Documentation/technical/pack-format.txt`). Reference algorithm (C, from git):

```
byte   = read_byte()
offset = byte & 0x7F
while (byte & 0x80) {
    byte   = read_byte()
    offset = ((offset + 1) << 7) | (byte & 0x7F)
}
```

The `+1` before each `<< 7` is the load-bearing detail (it makes the encoding a bijection with
no redundant representations). Omitting it silently decodes almost every multi-byte offset
wrong — the single highest-risk line in the whole feature.

`offset` is the **distance backwards**: the base object's header starts at
`base_offset = this_entry_header_offset − offset`, where `this_entry_header_offset` is the byte
position of the first byte of *this* entry's type+size varint (i.e. `lv_curr_offset` in
`resumable_decode`, already computed at L831 as `xstrlen( iv_data ) − xstrlen( lv_data )`
*before* reading the type byte).

### 1.3 Exact ABAP (`<<7` = `*128`, no native shift needed)

```abap
METHOD get_offset.
  " OFS-delta negative base-offset encoding (git pack-format.txt "offset encoding").
  " NOTE: distinct from get_length; the +1 bias per continuation byte is mandatory.
  CONSTANTS lc_msb  TYPE x LENGTH 1 VALUE '80'.  " 1000_0000 continuation flag
  CONSTANTS lc_low7 TYPE x LENGTH 1 VALUE '7F'.  " 0111_1111 payload mask
  CONSTANTS lc_zero TYPE x LENGTH 1 VALUE '00'.

  DATA lv_byte TYPE x LENGTH 1.
  DATA lv_low  TYPE x LENGTH 1.
  DATA lv_int  TYPE i.

  lv_byte = cv_data(1).
  cv_data = cv_data+1.
  lv_low  = lv_byte BIT-AND lc_low7.
  ev_offset = lv_low.

  WHILE lv_byte BIT-AND lc_msb <> lc_zero.
    IF xstrlen( cv_data ) = 0.
      zcx_abapgit_exception=>raise( |OFS varint truncated| ).
    ENDIF.
    lv_byte   = cv_data(1).
    cv_data   = cv_data+1.
    lv_low    = lv_byte BIT-AND lc_low7.
    lv_int    = lv_low.
    ev_offset = ( ev_offset + 1 ) * 128 + lv_int.   " ((off+1) << 7) | payload
  ENDWHILE.
ENDMETHOD.
```

`ev_offset` fits comfortably in `TYPE i` (INT4) for any pack < 2 GB; Ortec packs are far
smaller. Overflow is not a practical concern, but §6 specifies the guard.

Exact decode vectors (used verbatim in the unit test, §7.2):

| input bytes | decoded offset | reasoning |
|---|---|---|
| `00` | 0 | single byte, payload 0 |
| `7F` | 127 | single byte, payload 0x7F |
| `80 00` | 128 | `(0+1)*128 + 0` |
| `80 7F` | 255 | `(0+1)*128 + 127` |
| `81 00` | 256 | `(1+1)*128 + 0` |
| `FF 7F` | 16383 | `(127+1)*128 + 127` |
| `80 80 00` | 16512 | `((0+1)*128) → 128`, then `(128+1)*128 + 0` |

---

## 2. Requirement 2 — offset → base resolution (both decoder paths)

### 2.1 The core difficulty (delta chains) and why a plain offset→SHA rewrite is wrong
The existing `decode_deltas` identifies a delta's base by the base's **reconstructed content
SHA1** (`READ TABLE ct_objects WITH KEY sha COMPONENTS sha1 = is_object-sha1`, with an
`obj_store` fallback). For OFS the server gives only a byte offset. It is tempting to convert
`ofs → ref` in one forward pass by mapping `base_offset → base_sha`, but this is **only correct
when every ofs base is a full object**. In real thin packs, delta **chains** occur (git default
depth 50): an ofs-delta's base may itself be a not-yet-resolved delta (ofs *or* ref), whose
reconstructed SHA1 is unknown until it is resolved. A naive offset→SHA rewrite therefore
mis-binds chained deltas and produces silently wrong object data. Chains — and the
`ofs-on-ref-thin-base` case (see D-P5-7) — are the reason a dependency-ordered resolver is
required rather than a one-pass rewrite.

### 2.2 The mechanism: in-memory `offset → entry` map + dependency-ordered resolve
Both decoder paths build, during the single parse pass, an **in-memory** map keyed by pack
offset:

```
offset_map : offset (i) -> reference to the parsed entry (index into the objects table)
```

Every entry records its own `pack_offset`; ofs entries additionally record `base_offset`
(computed from `get_offset`). Git guarantees an ofs base always precedes its delta (offset is
strictly backwards), so `base_offset` is always already present in the map by the time the delta
is parsed. Resolution then runs in **dependency order** (memoized recursion, §3), not a single
forward pass, so a base that is itself a delta is fully resolved (to real data + real SHA)
before the dependent delta applies its instructions.

### 2.3 (a) Ortec incremental decoder path — `zcl_abapgit_ortec_pack_dec=>resumable_decode`
This path **already** tracks per-object pack offset. At L1012:

```abap
ls_idx-pack_offset = lv_curr_offset.   " already stored per object today
ls_idx-delta_base  = lv_ref_delta.     " today only set for REF_DELTA
```

Changes (Ortec-only):
1. `get_type` (L1234) gains `WHEN 96. rv_type = c_ofs_d.` (see §3.1 for the constant).
2. In the parse loop, add an ofs branch parallel to the existing ref branch (L835):
   ```abap
   IF lv_type = zcl_abapgit_ortec_delta=>c_type_ofs_d.
     lv_base_offset = lv_curr_offset - zcl_abapgit_ortec_delta=>get_offset(
                        CHANGING cv_data = lv_data ).
   ENDIF.
   ```
   `lv_curr_offset` is already captured (L831). Record `(index, pack_offset, base_offset,
   raw_delta_data)` in an in-memory `lt_ofs_meta` table and add `pack_offset → index` to
   `offset_map`. (Full/ref entries are added to the map too, so ofs bases that are full/ref
   objects resolve.)
3. At finalization, **replace the single `zcl_abapgit_git_delta=>decode_deltas` call (L1138)
   with `zcl_abapgit_ortec_delta=>resolve_all`** (§3), which handles ref **and** ofs uniformly.
   The existing targeted delta-base prefetch (L1085-1136) is retained and feeds `obj_store`
   thin bases into the resolver's ref-lookup fallback exactly as it does today for ref deltas.
4. Persist `DELTA_BASE = resolved base content-SHA1` for ofs entries too (uniform with ref),
   so `ZAOG_PACK_IDX.DELTA_BASE` becomes "the base SHA1 regardless of encoding". `PACK_OFFSET`
   is already stored. **No schema change.**

**Resume correctness.** On resume, the raw pack is re-read from `ZAOG_RAW_PACK` and re-parsed,
so ofs varints and `base_offset` values are recomputed from bytes (nothing new needs
persisting). Already-done entries rehydrated from DB (L771-808) carry their `PACK_OFFSET` from
`ZAOG_PACK_IDX`, so the finalization `offset_map` is complete across the rehydrated + freshly
parsed set. Resolution is therefore always performed over the full in-memory set at
finalization, identical whether or not a resume occurred.

### 2.4 (b) Standard decoder path — `zcl_abapgit_git_pack=>decode`
**No change, by design.** The standard `decode` (and `decode_deltas`) has no persisted or
in-memory offset map, and adding one plus a resolver would exceed the D7 budget (§3). Crucially
it is **not needed**: `thin-pack`/`ofs-delta` are advertised **only** on the Ortec fastpath
`upload_pack` (§4), so a spec-compliant server never sends an ofs entry or a thin pack to the
standard path. The standard path keeps its current behaviour — including raising
`Todo, unknown git pack type` (L688) if a **non-compliant** server sends ofs unsolicited, which
is a safe hard failure, not silent corruption. (Optional, minor: reword that message to name
ofs-delta; see D-P5-1.)

---

## 3. Requirement 3 — D7 minimal-touch assessment (determines the file list)

### 3.1 The type constant
Adding `ofs_d` to the shared `zif_abapgit_git_definitions=>c_type` is additive and harmless, but
it touches a standard interface. Because ofs is an Ortec-only live path, the constant can live
**Ortec-local** on `zcl_abapgit_ortec_delta` (`CONSTANTS c_type_ofs_d TYPE
zif_abapgit_git_definitions=>ty_type VALUE 'ofs_d '`), touching **zero** standard files. This is
the recommended default (D-P5-1). The 6-char `ty_type` fits `'ofs_d '` (5 chars + pad); it must
not collide with the existing `commit/tree/ref_d/tag/blob` values — `ofs_d` does not.

### 3.2 Does OFS support fit the D7 budget for standard `zcl_abapgit_git_pack` /
`zcl_abapgit_git_delta` / `zif_abapgit_git_definitions`?

**Verdict: it would EXCEED the budget on the standard classes — but the correct design avoids
touching them entirely, which is even better than "minimal-touch".**

- A correct ofs implementation needs: a new type branch, a new varint reader, an offset map
  threaded through the decode loop, and a **dependency-ordered resolver** replacing the
  index-ordered `decode_deltas`. The resolver alone is control-flow restructuring well beyond
  "≤10 changed lines per method / guarded delegation only" (D7 §8.0 budget). Putting it in
  standard `zcl_abapgit_git_delta` would restructure a core, heavily-used class and create
  upstream-rebase conflict risk — squarely the case D7 routes to an **Ortec mirror**.
- **But** D1 Option B means the standard path never *receives* ofs/thin packs, so standard
  code needs no ofs support at all. The design therefore adds a small **Ortec-owned resolver
  class** (`zcl_abapgit_ortec_delta`) used only on the Ortec decode path, and leaves standard
  `zcl_abapgit_git_pack`, `zcl_abapgit_git_delta`, and `zif_abapgit_git_definitions`
  **untouched**. This satisfies D7 in its strongest form (zero standard churn) *and* is correct
  because of D1.

**Consequence for the file list:** no standard-class production changes (one optional cosmetic
message reword aside, D-P5-1). All new logic is Ortec-namespaced. This is the concrete D7 call.

### 3.3 Byte-apply reuse (`delta()` is private)
The byte-level copy/insert algorithm in `zcl_abapgit_git_delta=>delta()` is private and coupled
to `ct_objects` + SHA-based base lookup — it is not a clean `apply(base, delta)` primitive.
Reusing it would require exposing a public `apply` on standard `zcl_abapgit_git_delta` (a small
but real standard-code refactor). Per D7 the recommended default is an **Ortec-owned copy** of
the ~20-line copy/insert loop inside `zcl_abapgit_ortec_delta` (zero standard touch; the
algorithm is tiny and spec-stable and is unit-pinned against exact vectors, §7.2). Exposing a
public standard `apply` (less duplication, small standard touch) is the alternative — this is a
genuine trade-off, raised as **D-P5-3**.

---

## 4. Requirement 4 — capability negotiation change (exact site, Ortec-only)

### 4.1 The single production call site to change
`src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap`, method `upload_pack`, currently L544:

```abap
lv_capa = 'side-band-64k no-progress multi_ack'.
```

This is inside the `LOOP AT it_hashes` block that emits `want` lines; the capability list is
attached to the **first** want line only (correct per protocol v1). Immediately below (L554-582)
the method already resolves `lt_ortec_haves` via `zcl_abapgit_ortec_fetch_neg=>get_have_commits`
and already suppresses `deepen` when haves exist so the server can send a delta pack.

### 4.2 The change (gated)
Advertise `thin-pack ofs-delta` **only when** the completeness gate (§5) yields a non-empty set
of **verified-complete** haves, because a thin pack's delta bases will be exactly those have
objects; if they are not genuinely complete+valid locally, the server's deltas become
unresolvable → corruption. Reorder so haves are resolved **before** the capability line is
built:

```abap
" resolve verified-complete haves FIRST (moved up from L554)
DATA(lt_verified_haves) = zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits(
                            iv_url         = iv_url
                            it_want_hashes = it_hashes ).
DATA(lv_allow_thin) = xsdbool( lt_verified_haves IS NOT INITIAL AND iv_allow_thin = abap_true ).

LOOP AT it_hashes FROM 1 ASSIGNING <lv_hash>.
  IF sy-tabix = 1.
    IF lv_allow_thin = abap_true.
      lv_capa = 'side-band-64k no-progress multi_ack thin-pack ofs-delta'.
    ELSE.
      lv_capa = 'side-band-64k no-progress multi_ack'.
    ENDIF.
    lv_line = |want { <lv_hash> } { lv_capa }{ cl_abap_char_utilities=>newline }|.
  ELSE.
    lv_line = |want { <lv_hash> }{ cl_abap_char_utilities=>newline }|.
  ENDIF.
  ...
ENDLOOP.
```

`iv_allow_thin` is a new `IMPORTING ... DEFAULT abap_false` parameter on the Ortec `upload_pack`
(and its `upload_pack_by_branch`/`upload_pack_by_commit` callers), so the fail-safe cascade (§6)
can force a **non-thin** retry by passing `abap_false`. When thin is disallowed the exact
current string is emitted (byte-for-byte unchanged behaviour).

### 4.3 What is explicitly NOT changed
- **Standard `zcl_abapgit_git_transport=>upload_pack` (L378)** keeps
  `'side-band-64k no-progress multi_ack'` unchanged — the non-opted-in path never negotiates
  thin/ofs (D1 Option B).
- **Ortec `find_missing_commits` filter path (L201)** keeps `'... multi_ack filter'` — it is a
  commits-only `filter tree:0` shallow fetch; thin/ofs would add risk with no benefit there.
  (ofs-alone for that path is out of scope; D-P5-5.)

---

## 5. Requirement 5 — delta-base completeness gate (what, where)

### 5.1 What "verified-complete" means for a `have` commit `C`
A commit may be advertised as a thin-pack base source **only if all** hold for `C`:
1. `C`'s commit object is present as `status='R'` in `ZAOG_OBJ_STORE` and decodes to a valid
   commit (root tree SHA parseable).
2. `C`'s object graph is **index-complete**: `zcl_abapgit_ortec_obj_index=>is_index_ready(
   repo_key, C )` returns true in STRICT mode — i.e. the `$IDX/__READY__` marker exists,
   proving the tree walk for `C` finished fully (Phase 4b semantics). A marker-less/partial
   index is treated as NOT complete.
3. **No dangling delta base:** every `ZAOG_PACK_IDX.DELTA_BASE` referenced by objects
   reachable from `C` resolves to a present `status='R'` object. This is the delta-base
   completeness check D3 asks for; it is the difference between "we have the objects" and "we
   have the objects *and their delta bases*" — the latter is what makes it safe to let the
   server delta against them.

Conditions 2+3 together are the gate. Condition 3 is the one with real correctness stakes and
is raised as **D-P5-2** (marker-only vs marker + no-dangling-base scan).

### 5.2 Where the check lives
- New `zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits( iv_url, it_want_hashes )
  RETURNING rt_haves` — wraps the existing `get_have_commits` and drops any commit failing 5.1.
- New helper `zcl_abapgit_ortec_fetch_neg=>is_commit_complete( iv_repo_key, iv_commit )
  RETURNING rv_yes` — implements 5.1(1-3). Condition 3's dangling-base scan reuses a new
  set-based helper on `zcl_abapgit_ortec_obj_store` (e.g. `has_dangling_delta_base( iv_repo_key,
  iv_commit )` — one `SELECT` of the reachable objects' non-blank `DELTA_BASE` values
  `LEFT`/anti-joined against present `status='R'` SHA1s; no per-object loop, honouring the
  hot-path constraint).
- The capability site (§4.2) advertises thin/ofs **iff** `get_verified_have_commits` is
  non-empty, so the gate and the have-lines are inseparable: we only ever advertise thin when we
  are simultaneously sending verified-complete haves.

---

## 6. Requirement 6 — safety / fail-safe behaviour (traced through the real chain)

### 6.1 The existing exception boundary and the gap it leaves
Standard `zcl_abapgit_git_transport=>upload_pack_by_branch` (L411+) wraps the Ortec fastpath in:

```abap
IF zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_true.
  TRY.
      zcl_abapgit_ortec_fastpath=>upload_pack_by_branch( ... ).
      RETURN.
    CATCH zcx_abapgit_ortec_git.      " <-- only ortec_git triggers standard fallback
  ENDTRY.
ENDIF.
" ... standard find_branch + upload_pack (non-thin, standard decode) ...
```

**Critical finding:** the Ortec decode raises `zcx_abapgit_exception` (e.g. `Base not found`,
`Todo, unknown git pack type`, `Decompression failed`), **not** `zcx_abapgit_ortec_git`. That
`CATCH` only catches `zcx_abapgit_ortec_git`, so a raw decode failure on the thin/ofs path would
**propagate past the fallback and abort the whole pull** — surfacing as "the fastpath crashed"
rather than a clean fall back to standard. This gap does not exist today only because ofs/thin
are never negotiated; enabling them exposes it. **New fallback logic is required** — it does not
already exist.

### 6.2 The mandated fail-safe cascade (never corrupt the store)
The thin/ofs attempt is bounded and self-contained. On **any** failure (ofs varint truncation,
unknown type, unresolvable base after the existing `obj_store` repair, chain-depth exceeded,
Adler/SHA/trailer mismatch, decompression failure):

1. **No partial promotion.** Temp rows are written as `status='P'` during decode and only
   promoted to `status='R'` *after* a fully successful parse **and** resolve (existing
   promote/`DELETE ... status='P'` step, L1160-1191). On failure the resolver raises **before**
   promotion, so no wrong object ever reaches the resolved store. The cascade additionally issues
   an explicit `DELETE FROM zaog_obj_store WHERE repo_key = … AND pack_id = … AND status = 'P'`
   (+ matching `ZAOG_PACK_IDX` temp cleanup) for the failed `pack_id`, so a failed thin attempt
   leaves the store byte-identical to before it started.
2. **Non-thin Ortec retry (one shot).** Re-issue the *same* want via Ortec `upload_pack` with
   `iv_allow_thin = abap_false` → capability string reverts to the current
   `'side-band-64k no-progress multi_ack'`, no thin haves offered → the server returns a
   self-contained pack → decode with the existing ref-only/full logic. This keeps the Ortec
   have/deepen bandwidth optimisation while removing the thin/ofs risk.
3. **Standard fallback.** If the non-thin Ortec retry also fails, wrap/convert the error to
   `zcx_abapgit_ortec_git` and raise it, so the **existing** standard `CATCH zcx_abapgit_ortec_git`
   in `zcl_abapgit_git_transport` fires and the fully standard `find_branch + upload_pack +
   zcl_abapgit_git_pack=>decode` path runs (guaranteed self-contained, no thin/ofs).

Net cascade: **thin/ofs (Ortec) → non-thin (Ortec) → standard (upstream)** — three strictly
weaker, strictly safer attempts, ending at behaviour identical to a repo that never opted in.
The 3-try shape vs a simpler 2-try (thin → standard) is a minor trade-off raised as **D-P5-4**.

### 6.3 Correctness invariant preserved
The unified resolver validates every reconstructed object by recomputing its content SHA1
(`zcl_abapgit_hash=>sha1`) exactly as `decode_deltas` does today, and the decode still verifies
the pack trailer SHA1 (L1175-1179). A mis-decoded ofs entry therefore fails a hash check and
routes into 6.2, never into the store. This upholds the non-negotiable invariant "correctness
outranks performance" and "missing/incomplete data never becomes wrong data".

---

## 7. Requirement 7 — file-by-file changes, test plan, phased rollout

### 7.1 Concrete file-by-file change list

**New (Ortec):**
| File | Kind | Contents |
|---|---|---|
| `src/ortec/git/zcl_abapgit_ortec_delta.clas.abap` (+ `.clas.xml`) | NEW | Unified delta resolver. Public: `c_type_ofs_d` const; `get_offset( CHANGING cv_data ) RETURNING ev_offset` (§1.3); `apply( iv_base, iv_delta ) RETURNING rv_result` (Ortec-owned byte copy/insert, §3.3); `resolve_all( CHANGING ct_objects IMPORTING it_offset_map it_ofs_meta iv_repo_key )` — memoized dependency-ordered resolve of ref+ofs (incl. `obj_store` thin bases + depth cap, §6). Name length `ZCL_ABAPGIT_ORTEC_DELTA` = 23 ≤ 30 ✓. |

**Modified (Ortec):**
| File | Method(s) | Change |
|---|---|---|
| `zcl_abapgit_ortec_pack_dec.clas.abap` | `get_type` | add `WHEN 96. rv_type = zcl_abapgit_ortec_delta=>c_type_ofs_d.` |
| " | `resumable_decode` (+ `decode_commits_only` for symmetry) | ofs parse branch (read `get_offset`, compute `base_offset`, fill `offset_map`/`lt_ofs_meta`); replace final `decode_deltas` call with `zcl_abapgit_ortec_delta=>resolve_all`; persist `DELTA_BASE` = resolved base SHA for ofs; explicit temp-row cleanup on failure |
| `zcl_abapgit_ortec_fetch_neg.clas.abap` | +`get_verified_have_commits`, +`is_commit_complete` | completeness gate (§5) |
| `zcl_abapgit_ortec_obj_store.clas.abap` | +`has_dangling_delta_base` (set-based) | gate condition 5.1(3) |
| `zcl_abapgit_ortec_fastpath.clas.abap` | `upload_pack` (+ `upload_pack_by_branch`/`_by_commit`) | new `iv_allow_thin` param; gated capability string (§4.2); fail-safe cascade + cleanup (§6.2) |
| `zcl_abapgit_ortec_git_tests.clas.testclasses.abap` | +`ltcl_ofs_delta` | unit tests (§7.2) |

**Standard:** none (see §3.2). Optional cosmetic only: reword `Todo, unknown git pack type`
(D-P5-1).

### 7.2 Test plan (network-free, exact vectors)

Component tests pin the two spec-critical primitives without any pack or zlib:

**T1 — `get_offset` varint** (the seven vectors from §1.2). For each, wrap the bytes in an
`xstring`, call `get_offset`, assert `ev_offset` equals the expected value **and** that
`cv_data` advanced by the exact byte count. This is the single most important test (the +1 bias).

**T2 — `apply` byte copy/insert.** base = `48 65 6C 6C 6F` (`"Hello"`), delta instruction bytes
= `05 06 90 05 01 21`, decoded as:
- `05` = base size varint (5)
- `06` = result size varint (6)
- `90` = copy op: MSB set (`0x80`) + length-byte-1 flag (`0x10`), offset flags none → copy from
  base offset 0
- `05` = copy length 5
- `01` = insert op, 1 literal byte
- `21` = `'!'`

Expected result = `48 65 6C 6C 6F 21` (`"Hello!"`). Assert exact xstring equality. (This also
pins the existing standard behaviour, giving confidence the Ortec copy matches.)

**T3 — full OFS pack integration (constructed in-test).** Build a 2-object pack:
- header: `PACK` + `00000002` + objcount `00000002`
- obj1 (full blob, size 5): first byte `35` (`0x30` blob | size 5) + `zlib("Hello")`
- obj2 (ofs-delta, size 6): first byte `66` (`0x60` ofs | size 6) + `get_offset`-encoded
  `neg_offset` + `zlib(05 06 90 05 01 21)`
- trailer: 20-byte `zcl_abapgit_hash=>sha1_raw` of all preceding bytes.

`neg_offset = O2 − 12`, where `O2 = 12 + 1 + xstrlen(zlib("Hello"))` (obj1 header byte + its
DEFLATE stream). The test computes zlib via `cl_abap_gzip=>compress_binary`, the ofs varint via
`get_offset`'s inverse (or a hand-encoded small value once the concrete zlib length is known),
and the trailer via `sha1_raw` — all deterministic at runtime. Assert `resumable_decode` /
`resolve_all` yields two objects: the base blob `"Hello"` and the reconstructed blob `"Hello!"`
with a correct recomputed SHA1.

**T4 — chain (ofs-on-ofs).** 3-object pack: full blob `"Hello"`, ofs-delta→obj1 producing
`"Hello!"`, ofs-delta→obj2 producing `"Hello!?"`. Proves dependency-ordered resolution of a
chain (the case a one-pass rewrite would break, §2.1).

**T5 — thin base (ref-delta to `obj_store`).** Pre-seed `ZAOG_OBJ_STORE` (via the existing
`cl_osql_test_environment` double already used in the Ortec test class) with a base blob at
`status='R'`; build a pack with a **ref**-delta whose base is that stored SHA (not in the pack)
and an **ofs**-delta whose base is that ref-delta (D-P5-7 case). Assert both resolve. Proves the
resolver's `obj_store` thin-base fallback + ofs-on-ref path.

**T6 — fail-safe.** Feed a pack with an ofs entry whose `base_offset` points outside the pack
and whose base is absent from `obj_store`. Assert `resolve_all` raises, **no** `status='R'` row
is written for the pack, and temp `status='P'` rows are cleaned (store unchanged).

**T7 — completeness gate.** With a commit whose `$IDX/__READY__` marker is absent (or with a
seeded dangling `DELTA_BASE`), assert `get_verified_have_commits` excludes it, so thin is not
advertised.

### 7.3 Phased rollout order (strict)

- **Phase 5a — decode-only (dead code).** Land `zcl_abapgit_ortec_delta`, the ofs parse branch,
  `get_type` case, and the `resolve_all` swap. **No capability change** → ofs/thin are still
  never negotiated, so the new code is unreachable in production (exactly as safe as today's
  unimplemented ofs). Gate to green: T1-T6 pass. This is independently shippable and reviewable.
- **Phase 5b — enable negotiation.** Add `iv_allow_thin`, the gated capability string, the
  completeness gate (`get_verified_have_commits`), and the fail-safe cascade. Only after 5a is
  proven. Gate to green: T7 + the cascade path (T6 via a live-ish injected failure) pass; a real
  fetch against a thin-capable server (e.g. GitHub) succeeds and a forced-failure falls back
  cleanly. **Do not enable 5b until 5a's decode is unit-green.**
- **Phase 5c — persistence/eviction tie-in (folds into Phase 6).** Uniform `DELTA_BASE` for ofs
  is already written in 5a; 5c wires the delta-base completeness check into the admin
  report/eviction (never evict a referenced base) per D5/Phase 6. Optional and separable.

---

## 8. Requirement 8 — open decisions needing Michael's input

See `.memory/decisions/phase5_ofs_delta_review_required.md` (D-P5-1 … D-P5-7), same format as
`design_review_required.md`. The two with genuine data-corruption stakes — flagged rather than
guessed — are:
- **D-P5-2** completeness gate strength (marker-only vs marker **+** no-dangling-delta-base
  scan before advertising thin).
- **D-P5-7** the `ofs-delta → ref-delta → thin base` case: confirm the unified resolver's
  recursion + `obj_store` thin-base fallback covers it and that the gate guarantees the thin base
  is present. Recommended: explicit test T5 + require the gate; needs Michael's ack because a
  mistake here is silent wrong-data.

The rest (D-P5-1 constant location, D-P5-3 byte-apply reuse, D-P5-4 cascade shape, D-P5-5
ofs-without-thin scope, D-P5-6 chain-depth cap) are lower-stakes with recommendations given.

---

## 9. Risks (Phase 5 specific)

| Risk | Impact | Mitigation |
|---|---|---|
| Wrong ofs varint (missing +1 bias) | Silent wrong base offset → wrong object data | T1 pins all seven vectors incl. multi-byte; SHA recompute + trailer check catch mis-decode → cascade |
| Delta chain mis-resolution (one-pass rewrite) | Silent corruption on chains | Dependency-ordered memoized resolver (§2-3); T4 chain test |
| ofs base = unresolved ref/thin base | Unresolvable base → decode abort | Unified resolver recursion + `obj_store` fallback; gate guarantees thin base present (D-P5-7); T5 |
| Decode failure escapes fallback (`zcx_abapgit_exception` not caught) | Whole pull aborts, looks like fastpath crash | New cascade converts to `zcx_abapgit_ortec_git` → existing standard CATCH (§6.1) |
| Partial thin decode promoted to store | Corrupt local store | Promote only after full success; explicit temp-row cleanup on failure (§6.2); T6 |
| Advertising thin without complete bases | `Walk,`-class corruption / unresolvable server deltas | Completeness gate (§5); advertise thin iff verified haves non-empty |
| Standard path receives ofs from non-compliant server | Hard error | Unchanged safe `raise` (not silent); acceptable (server bug) |

---

## 10. Files produced by this pass
- `.memory/logs/target_design_phase5.md` (this file)
- `.memory/diagrams/phase5_ofs_delta_flow.mmd`
- `.memory/decisions/phase5_ofs_delta_review_required.md`
- No productive ABAP source modified. No transports created. `.memory/state.md` intentionally
  left for Michael to update.
