# Delta resolver deep design reconsideration — `zcl_abapgit_ortec_delta`

- **Date:** 2026-07-15
- **Agent:** ortec-abapgit-design (design/analysis pass only — no productive ABAP changed)
- **Model tier:** large-reasoning (escalated above the default small tier; reason: this is a
  deep root-cause + architecture-replacement decision after three failed incremental fixes,
  which the model-routing policy allows to escalate).
- **Scope (narrow, per instruction):** only `zcl_abapgit_ortec_delta`'s REF_DELTA chain
  resolution (`resolve_all` / `resolve_one` / `apply`), plus the parts of
  `zcl_abapgit_ortec_pack_dec=>resumable_decode` that feed it. The broader Ortec architecture
  was NOT re-reviewed.
- **Files/methods inspected (first-hand, this pass):**
  - `src/ortec/git/zcl_abapgit_ortec_delta.clas.abap` — full source: `resolve_all`,
    `resolve_one` (both REF and OFS branches, thin-fetch branch, promotion), `apply`,
    `get_offset`, `skip_size_header`.
  - `src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap` — `resumable_decode` object-parse
    loop (L712–930), the rehydrate/resume branch, the "Targeted delta-base prefetch" block
    (L1167–1235), and the `resolve_all` call site (L1237+); how `lt_offset_map` / `lt_ofs_meta`
    are built.
  - `src/zif_abapgit_definitions.intf.abap` L106–118 — the exact `ty_object` / `ty_objects_tt`
    type (`STANDARD TABLE ... WITH DEFAULT KEY WITH NON-UNIQUE SORTED KEY sha COMPONENTS sha1
    WITH NON-UNIQUE SORTED KEY type COMPONENTS type sha1`).
- **Evidence source for the root cause:** official SAP ABAP keyword documentation
  `ABENSECONDARY_KEY_GUIDL` ("Robust ABAP" internal-tables guideline), quoted verbatim below.

---

## CONCRETE BUG FOUND (#4) — `sy-tabix` from a secondary-key loop is used as a primary index

**File / method:** `src/ortec/git/zcl_abapgit_ortec_delta.clas.abap`, `METHOD resolve_one`,
REF_DELTA branch (the `LOOP AT ct_objects ... USING KEY sha` and the `ELSE` block that follows
it).

**This is a genuine, identifiable fourth bug, distinct from the three already fixed, and it
matches the live symptom exactly.**

### The defect

`ty_objects_tt` is a `STANDARD TABLE` whose base lookup key `sha` is a **sorted secondary
key**. Fix #1 (commit `3a57757`) resolves the base with:

```abap
LOOP AT ct_objects ASSIGNING <ls_base>
    USING KEY sha
    WHERE sha1 = <ls_object>-sha1.
  IF <ls_base>-type <> ...ref_d AND <ls_base>-type <> c_type_ofs_d.
    lv_base_tabix = sy-tabix.      " <-- captures the SECONDARY table index
    EXIT.
  ENDIF.
ENDLOOP.
```

Inside a `LOOP AT ... USING KEY <sorted secondary key>`, `sy-tabix` is **the position in that
secondary key's own internal index, not the primary table index.** `lv_base_tabix` is then
consumed in three places that all interpret it as a **primary** index:

```abap
" ELSE branch (base already resolved):
resolve_one( iv_tabix = lv_base_tabix ... ).                 " (a) primary-index recursion
READ TABLE ct_objects ASSIGNING <ls_base> INDEX lv_base_tabix. " (b) primary-index read
```
and transitively, inside that recursive call and the promotion:
```abap
READ TABLE ct_objects ASSIGNING <ls_object> INDEX iv_tabix.  " primary read of wrong row
MODIFY ct_objects FROM <ls_object> INDEX iv_tabix ...        " primary MODIFY of wrong row
```

Whenever the pack's SHA1-sorted order differs from its pack (primary) order — the normal case
for any non-trivial pack — the secondary index value ≠ the primary index value, so every one of
(a)/(b)/the recursion/the `MODIFY` operates on **the wrong row**.

### Why this exactly matches the live failure

- The reported stack is `resolve_one -> resolve_one -> apply` failing at `apply` with
  *"Delta copy instruction exceeds base length"*, on a **2-level REF_DELTA chain**, on a large
  "complete refresh" pack, **three times, after both prior fixes were live.** Neither prior fix
  touched the primary-vs-secondary index-space confusion.
- Path: the outer `resolve_one` finds a resolved base via the secondary loop and captures its
  **secondary** index into `lv_base_tabix`. It then calls `resolve_one(iv_tabix = lv_base_tabix)`
  which reads **primary**`[lv_base_tabix]` — a *different* row `W`. If `W` happens to be an
  unresolved delta, the inner `resolve_one` tries to resolve `W`, reaches `apply`, and applies
  `W`'s delta bytes against a wrong/short base → *"exceeds base length"* — i.e. the exact
  `resolve_one -> resolve_one -> apply` shape and message reported.
- It only bites on **large, complex** packs (where secondary order diverges strongly from
  primary order and where a wrong primary row is itself a delta). Every current unit test uses
  ≤4 objects with the base adjacent/first, so secondary and primary indices coincide (or the
  wrong row is not a delta) and the bug stays invisible — which is why all three prior fixes
  "passed in isolation" yet the live failure persisted.

### Authoritative confirmation (verbatim)

SAP ABAP docs, `ABENSECONDARY_KEY_GUIDL`:

> "Note that the `sy-tabix` system field is populated by the assigned secondary index, if
> sorted secondary keys are used. **If this value is used for the subsequent index access to
> the internal table, the same table index must be explicitly used here. If used implicitly,
> the value would be interpreted as a primary index.**"

This is the code's exact mistake: `sy-tabix` is captured from the `USING KEY sha` loop and then
used implicitly as a primary index.

### Corroboration — why OFS_DELTA never had this bug

The OFS branch locates its base with `READ TABLE ct_objects ... WITH KEY index = ...`. `index`
is **not** a secondary key of `ty_objects_tt` (only `sha` and `type` are), so that is a free-key
linear search over the **primary** index → `sy-tabix` there IS the primary index. That is
precisely why OFS_DELTA resolution has been correct throughout and REF_DELTA has not — a strong
independent confirmation that the secondary-key index space is the differentiator.

### Effect of the diagnostics commit (`97d914e`) on the next retest

`97d914e` added a post-lookup check `IF <ls_base>-sha1 <> <ls_object>-sha1 -> raise identity
mismatch`, evaluated **after** the recursion + `READ ... INDEX lv_base_tabix`. With bug #4:
- if the wrong primary row is already resolved, the next retest will surface as
  **"Delta base identity mismatch"** (not "exceeds base length");
- if the wrong primary row is itself a delta, it still surfaces as **"exceeds base length"**
  from the inner `resolve_one -> apply`, before the outer identity check is reached.
Either outcome is consistent with, and further confirms, bug #4.

### Related latent defect (same root family, lower live probability) — thin-fetch first-match

In the thin-base branch (`lv_base_tabix IS INITIAL`), after `APPEND ls_base_object TO
ct_objects` the code does:

```abap
READ TABLE ct_objects ASSIGNING <ls_base>
  WITH KEY sha COMPONENTS sha1 = <ls_object>-sha1.
```

`sha` is **non-unique**, and the freshly appended real base shares its `sha1` value with any
still-unresolved sibling `ref_d` entries that *declare* that same base (their `sha1` holds the
declared-base placeholder). This first-match read can therefore bind `<ls_base>` to an
unresolved sibling delta instead of the appended real base. The identity check does **not**
catch it (the sibling's `sha1` == the declared base == `<ls_object>-sha1`), so it would raise
the later *"Delta, base still unresolved"* guard. This is unlikely to be the current live
trigger (a full/deepen pack carries its bases in-pack, so the thin branch should rarely fire),
but it is the same class of defect and must die with the redesign, not survive it.

### Immediate narrow fix option (if a redesign is not taken right now)

If a minimal hotfix is wanted ahead of the redesign, the surgical correction is: never reuse a
secondary-key `sy-tabix` as a primary index. In the REF branch, resolve the base's **primary**
`tabix` explicitly (e.g. keep the base's SHA1 and re-derive the primary index via a primary
read, or capture the primary index a different way), and drive both the recursion and the
`READ ... INDEX` / `MODIFY ... INDEX` from that primary index. However — see the architecture
section: this bug is the third symptom of one fragile mechanism, and I recommend replacing the
mechanism rather than adding a fourth spot-fix to it.

---

## Architectural assessment — keep-and-patch vs. replace

### The pattern behind all four defects

Every failure in this pipeline (bugs #1, #2, #4, and the thin first-match latent) has the same
single root cause: **base identity is resolved by querying a NON-UNIQUE SORTED SECONDARY KEY
(`sha`) built on the objects table's own dual-purpose, mutable `sha1` field.**

- `sha1` is overloaded: for an unresolved `ref_d`/`ofs_d` row it holds the *declared base*
  placeholder; only after resolution does it hold the row's *own real identity*. So the very
  field the key is built on carries two different meanings for two different row states.
- Because the key is non-unique, lookups can match the wrong same-valued row (#1, thin).
- Because the key field is mutated in place during resolution, the key's internal structure
  goes stale (#2) and its `sy-tabix` lives in a different index space from the primary table
  (#4).

Three commits (`3a57757`, `2898c9e`, plus the `97d914e` diagnostics) have each correctly fixed
one facet and left the mechanism itself intact. The mechanism is **inherently fragile for
real, large packs with many interleaved chains** and should be replaced, not patched a fourth
time. This is consistent with the project invariant **correctness > performance >
maintainability**: the current design repeatedly trades correctness for a marginally simpler
lookup.

### Strategy comparison

| | (A) Fixpoint multi-pass + separate known-identity set | (B) Keep recursion, linear-scan lookup | (C) Separate HASHED side-index sha1→tabix | Current (patch again) |
|---|---|---|---|---|
| Kills #1 (non-unique first-match) | Yes, by construction | Yes | Yes, by construction | No (only masked) |
| Kills #2 (stale key after promote) | Yes — key never used for lookup | Partial — still MODIFYs key field | Yes — lookup index is independent | No |
| Kills #4 (secondary vs primary `sy-tabix`) | Yes — no keyed-loop `sy-tabix` reused as index | Yes — plain primary loop only | Yes — index stored is primary tabix | No |
| Kills thin first-match latent | Yes — thin base inserted into the set by real sha1 | Needs care | Yes | No |
| Recursion / depth-guess machinery | **Removed** — progress-based termination | Kept | Kept | Kept |
| Lookup cost | O(1) hashed | O(n) per lookup → O(n²) | O(1) hashed | O(log n) but wrong |
| Correctness confidence | Highest | High | High | Low (0/3 live) |

- **(B)** is correct-by-construction for the lookup but keeps the recursion, the depth-limit
  cycle proxy, in-place mutation of a key-participating field, and O(n²) cost. It removes the
  *least* fragile surface for the *most* retained machinery. Acceptable as a fallback minimal
  change, not the target.
- **(C)** is a strong intermediate: O(1) and correct-by-construction, but still keeps recursion,
  depth-guessing, and in-place promotion of the key field.
- **(A)** removes the recursion and the depth-as-cycle heuristic entirely, replacing them with a
  deterministic *progress-based* fixpoint (a full pass that resolves nothing ⇒ terminate).

### Recommendation — **hybrid (A) + (C): non-recursive fixpoint driver over an explicit hashed known-identity side-index**

Recommend replacing `resolve_one`'s recursion and its secondary-key lookups with:

1. A private **hashed side-index of certain identities**, `sha1 → primary tabix`, built ONCE up
   front from every non-delta row (identities that are certain from the moment of parsing) and
   **`INSERT`ed into** (never mutated for an existing key) as each delta resolves. All base
   lookups go through this index — the objects table's own `sha`/`type` secondary keys are never
   queried for resolution again.
2. A **non-recursive fixpoint driver**: repeatedly pass over the still-unresolved deltas,
   resolving exactly those whose base is currently in the known-identity index (REF: by declared
   `sha1`; OFS: by offset→object-index→**primary** tabix, unchanged and already correct), until a
   full pass makes no progress. Remaining unresolved ⇒ either a thin/external base (object-store
   fallback) or a genuine error (cycle / missing base).

This is the single design that kills **all four** root causes plus the thin latent one *by
construction*, and additionally deletes the two most error-prone remaining mechanisms
(recursion + depth-guessing). Correctness first; the O(1) hashed index keeps it fast; a
worklist keeps it near-O(n).

**Single strongest piece of evidence for choosing replacement over a fourth patch:** all four
distinct defects (#1, #2, #4, thin) trace to the *same* mechanism — resolving base identity via
a non-unique sorted secondary key on a mutable dual-purpose field — and the official
`ABENSECONDARY_KEY_GUIDL` guidance explicitly warns against two of the exact operations this
mechanism performs (reusing secondary `sy-tabix` as a primary index; relying on a secondary key
over a frequently-mutated field). The mechanism is contraindicated by SAP's own robustness
guideline; patching it a fourth time keeps the contraindicated mechanism.

---

## Proposed design detail (for `ortec-abapgit-implementation`, after review/approval)

### Public contract — unchanged

`resolve_all( it_offset_map, it_ofs_meta, iv_repo_key CHANGING ct_objects )` keeps its exact
signature and its exit contract (every delta row replaced in place by its resolved object; on
any unresolvable base / cycle / missing base / bad delta → raise, caller distrusts `ct_objects`
entirely). The sole caller, `zcl_abapgit_ortec_pack_dec=>resumable_decode`, is **not touched**.
`apply`, `get_offset`, `skip_size_header` are **unchanged** (byte-level algorithm is not
implicated in any of the four bugs).

### New / changed internals (all Ortec-local; zero standard changes)

```abap
PRIVATE SECTION.
  TYPES: BEGIN OF ty_known,
           sha1  TYPE zif_abapgit_git_definitions=>ty_sha1,
           tabix TYPE i,                 " PRIMARY index into ct_objects (stable: table only APPENDs)
         END OF ty_known.
  TYPES ty_known_tt TYPE HASHED TABLE OF ty_known WITH UNIQUE KEY sha1.

  TYPES: BEGIN OF ty_idx2tabix,
           obj_index TYPE i,
           tabix     TYPE i,             " PRIMARY index into ct_objects
         END OF ty_idx2tabix.
  TYPES ty_idx2tabix_tt TYPE HASHED TABLE OF ty_idx2tabix WITH UNIQUE KEY obj_index.
```

- `resolve_all`:
  1. Build `lt_idx2tabix` (obj_index → primary tabix) and `lt_known` (real sha1 → primary
     tabix) in one primary-index `LOOP AT ct_objects` (`sy-tabix` here IS the primary index —
     safe). Seed `lt_known` with every row whose `type` ∉ {ref_d, ofs_d}.
  2. Fixpoint:
     ```
     DO lines( ct_objects ) TIMES.        " hard safety cap; each pass resolves >=1 or we stop
       lv_progress = abap_false.
       LOOP AT ct_objects ASSIGNING <obj>.  " primary index; sy-tabix = primary
         IF <obj>-type <> ref_d AND <obj>-type <> ofs_d. CONTINUE. ENDIF.
         determine base primary tabix:
           REF: READ lt_known WITH TABLE KEY sha1 = <obj>-sha1  -> base tabix (or thin-fetch)
           OFS: base obj_index from it_ofs_meta/it_offset_map;
                READ lt_idx2tabix WITH TABLE KEY obj_index = ...; base known iff that row is non-delta
         IF base not yet known: CONTINUE.   " resolve on a later pass
         READ ct_objects INDEX base_tabix -> <base>   (primary)
         lv_result   = apply( iv_base = <base>-data iv_delta = <obj>-data )
         lv_sha1     = zcl_abapgit_hash=>sha1( iv_type = <base>-type iv_data = lv_result )
         <obj>-type = <base>-type. <obj>-data = lv_result. <obj>-sha1 = lv_sha1.
         MODIFY ct_objects FROM <obj> INDEX sy-tabix.   " primary index — correct
         INSERT VALUE #( sha1 = lv_sha1 tabix = sy-tabix ) INTO TABLE lt_known.
         lv_progress = abap_true.
       ENDLOOP.
       IF lv_progress = abap_false. EXIT. ENDIF.
     ENDDO.
     ```
  3. Finalize: if any `ref_d`/`ofs_d` rows remain → for `ref_d`, attempt one
     `zcl_abapgit_ortec_obj_store=>get_object` per still-missing declared base (track attempted
     shas to avoid re-hitting the store every pass), `APPEND` the fetched base, `INSERT` it into
     `lt_known` by its real sha1, set progress and run one more fixpoint round. If, after all
     available thin bases are pulled, a full pass still makes no progress with deltas remaining →
     `raise` (cycle or genuinely missing base — never silently leave a delta unresolved, per the
     "missing/uncertain data must never be silently misinterpreted" invariant).
- `resolve_one` (recursive) and `c_max_chain_depth`-as-recursion-guard are **removed**. A
  progress-based cap (`lines( ct_objects )` passes, plus the thin round) replaces the
  depth-guess; `c_max_chain_depth` may be retained only as documentation or dropped.

### Interaction with the thin-base persistent-store fallback

Thin bases are pulled exactly as today (`zcl_abapgit_ortec_obj_store=>get_object`, keyed by the
declared SHA1), but only in the finalize step, and the fetched base is registered in
`lt_known` by its **real** sha1 — so the dependent resolves on the next pass via the O(1) index,
never via a non-unique first-match read. This removes the thin first-match latent defect.

### Interaction with OFS_DELTA (must stay correct — it is not the source of any prior bug)

OFS base identification remains **offset-driven and unchanged**:
`it_ofs_meta[obj_index].base_offset → it_offset_map[base_offset].obj_index →
lt_idx2tabix[obj_index] → primary tabix`. The only change is that the "is my base resolved yet?"
question is answered by "is that primary tabix's row non-delta / present in `lt_known`?" instead
of by a recursive `resolve_one` call. Mixed chains (a `ref_d` whose base is an `ofs_d`, or vice
versa) fall out naturally: whichever base resolves first is registered in `lt_known`/becomes
non-delta, and the dependent resolves on a later pass regardless of its own or its base's delta
type.

---

## Regression tests required (prove BOTH simple and the failure-class scenarios)

Existing tests to keep green: `ltcl_ofs_delta` (all), `ltcl_ref_delta=>base_positioned_after_
dependent`, `ltcl_ref_delta=>resolve_after_prior_resolution_in_same_pass`.

New tests (the current suite exercises none of these; all use trivial single-byte offset=0,
adjacent, already-resolved bases — which is why bug #4 stayed hidden):

1. **REF chain where SHA1-order ≠ pack-order (the exact live trigger).** ≥3 objects arranged so
   the `sha`-secondary order differs from the primary order, with a 2-level `ref_d → ref_d →
   base` chain. Must resolve correctly. (Under the *old* code this reproduces
   "exceeds base length" / "identity mismatch"; under the new design it passes.)
2. **Shared base, multiple sibling `ref_d`s (non-unique sha1) resolved in one pass.** Proves no
   first-match ambiguity and correct per-sibling resolution.
3. **Multi-byte copy offset & length in `apply`.** Vectors that set copy-offset flag bits for
   bytes 2/3/4 (offset > 255, > 65535) and length flag bits for bytes 2/3 (length > 255), plus
   the `length == 0 ⇒ 65536` quirk. Pin exact input→output byte vectors. (Current `apply`
   coverage is single-byte offset=0 only — the multi-byte decode paths are completely
   unexercised.)
4. **Mixed OFS+REF chain both directions:** a `ref_d` whose base is an unresolved `ofs_d`, and an
   `ofs_d` whose base is an unresolved `ref_d`. Proves the unified fixpoint handles cross-type
   chains.
5. **Thin base from object store with in-pack sibling sharing the declared base sha1.** Proves
   the dependent binds to the fetched real base, not the sibling (kills the thin latent defect).
6. **Cycle and genuinely-missing base.** Must `raise` deterministically (progress-based
   termination), never hang, never misresolve, never silently leave a delta.
7. **Large-ish out-of-order fixpoint smoke test** (e.g. 20–30 objects, deltas shuffled ahead of
   their bases, 3–4 level chains) to exercise multi-pass progress and the pass cap.

---

## Invariants applied

- **Correctness > performance > maintainability:** recommend replacing a fast-but-repeatedly-
  wrong mechanism with a correct-by-construction one; the O(1) hashed index keeps performance
  acceptable, but correctness drove the choice.
- **Missing/uncertain data is never silently misinterpreted:** the fixpoint's finalize step
  raises on any unresolved remainder rather than leaving a delta half-resolved or guessing.
- **All logic stays in `zcl_abapgit_ortec_*`:** the redesign is entirely inside
  `zcl_abapgit_ortec_delta`; `resolve_all`'s signature and its caller are unchanged; standard
  `zcl_abapgit_git_pack` / `zcl_abapgit_git_delta` / `zif_abapgit_definitions` are untouched.

## Status

Design/analysis only. **No productive ABAP modified.** Awaiting Michael's review/approval before
`ortec-abapgit-implementation` executes either the narrow bug-#4 hotfix or (recommended) the
hybrid A+C redesign.
