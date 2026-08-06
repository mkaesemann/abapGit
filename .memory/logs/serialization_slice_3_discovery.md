# SER-SLICE-3 discovery packet

- Current wire formats implemented today:
  - ser_pref exports a single-object payload via `EXPORT msag = lt_msag dokil = lt_dokil language = mv_language dokil_prepared = mv_dokil_prepared TO DATA BUFFER rv_buffer` and imports the same names back in `inject_from_buffer`.
  - ser_pref_ext exports a single-object payload via `EXPORT dtel = lt_dtel enhs = lt_enhs fugr_areat = ... prog_langs = ... tran = lt_tran language = mv_language ...` and imports the identical field names back.
  - ser_pref_oo exports a single-object payload via `EXPORT classtx = lt_classtx compotx = lt_compotx subcotx = lt_subcotx language = mv_language` and imports the same fields back.
  - All three are single-object, single-buffer shapes; `inject_from_buffer` clears the receiver caches and inserts the imported rows into them, so they are already designed for one logical payload per call.

- `ZCL_ABAPGIT_ORTEC_SER_PROV_DD` exists today: no. The workspace contains no class definition or implementation for that name; the only mention is a comment in the ser_pref_ext testclass, so the provider DD class is still absent.

- Why concatenating single-object EXPORT buffers is unsafe for batch use:
  - Each current buffer is an `EXPORT/IMPORT` payload for exactly one object’s worth of rows, not an envelope with object-count/version/object-name fields.
  - The importer expects one logical payload and then clears and repopulates the family cache (`mt_msag/mt_dokil`, `mt_dtel/...`, `mt_classtx/...`) — there is no batch-aware merge contract, no per-object delimiter, and no way to split a concatenated payload back into individual object slices.
  - A simple byte-concatenation of N single-object buffers would therefore be ambiguous and likely invalid for the existing `IMPORT ... FROM DATA BUFFER` contract; a real batch envelope needs explicit versioning and object-level structure.

- Ordered object-type ranking from current source/static evidence plus the Stage-A CLAS measurements:
  1. DTEL -> existing DTEL prefetch consumer in `zcl_abapgit_object_dtel` and an existing `ser_pref_ext` cache family.
  2. DOMA -> adjacent DDIC family, existing parity work already documented, and a clear next provider target after DTEL.
  3. CLAS -> existing OO provider and the Stage-A measurements show strong batch payoff (serialization down 55.7%, RFC starts down 95.9% for the CLAS subset).
  4. INTF -> same OO family as CLAS and low additional structural risk.
  5. MSAG -> existing `ser_pref` cache family and direct serializer use.
  6. TRAN -> existing `ser_pref_ext` cache family and direct serializer use.
  7. FUGR -> existing `ser_pref_ext` cache family and direct serializer use.
  8. PROG -> existing `ser_pref_ext` cache family and direct serializer use.
  9. TABL -> DDIC-like, but no current prefetch hook visible in the object-class source.
  10. TTYP -> same as TABL, with no current prefetch hook visible.
  11. DDLS -> no visible prefetch path in the current object source.
  12. DCLS -> no visible prefetch path in the current object source.
  13. WAPA -> Stage-A policy already treats it as singleton-batch-only, so it is later-slice material.
  14. ENQU -> no current prefetch hook visible.
  15. SHLP -> no current prefetch hook visible.
  16. VIEW -> no current prefetch hook visible.

- Recommended first provider slice: DOMA/DTEL. It is the most evidence-backed first move because DTEL already has a real prefetch consumer, DOMA is the adjacent DDIC family with documented parity semantics, and the needed new provider surface is small and local instead of inventing a whole new batch contract from scratch.

- Smallest viable Stage B design shape: make the existing prefetch classes additive by adding `extract_for_batch` methods that loop over the batch’s TADIR rows, reuse the same per-object lookup logic already used today by `extract_for_object`, and build one batch-wide payload per family; pair that with a real versioned envelope (for example `version = 1`, `family`, `language`, `object_count`, and the accumulated payload tables) so `inject_from_buffer` can import, validate, and merge one batch at a time without any byte-concatenation hack.
