# SER-SLICE-4 common discovery

## Scope

Read-only discovery for the serialization slice 4 common-discovery work. Focus was the current source behavior of TABL, TTYP, PROG, and FUGR, plus the ORTEC serializer prefetch helpers that already exist for related families.

## Evidence base

Inspected current source in:
- [src/objects/tabl/zcl_abapgit_object_tabl.clas.abap](src/objects/tabl/zcl_abapgit_object_tabl.clas.abap)
- [src/objects/zcl_abapgit_object_ttyp.clas.abap](src/objects/zcl_abapgit_object_ttyp.clas.abap)
- [src/objects/zcl_abapgit_object_prog.clas.abap](src/objects/zcl_abapgit_object_prog.clas.abap)
- [src/objects/zcl_abapgit_object_fugr.clas.abap](src/objects/zcl_abapgit_object_fugr.clas.abap)
- [src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap](src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap)
- [src/ortec/serial/zcl_abapgit_ortec_ser_pref.clas.abap](src/ortec/serial/zcl_abapgit_ortec_ser_pref.clas.abap)
- [src/ortec/serial/zcl_abapgit_ortec_ser_pref_oo.clas.abap](src/ortec/serial/zcl_abapgit_ortec_ser_pref_oo.clas.abap)

## Family-by-family findings

### TABL
- Current serialize path is centered on DDIC reads through `DDIF_TABL_GET` in the main `zif_abapgit_object~serialize` method.
- The method collects DDIC metadata and then calls `serialize_texts`, `serialize_idoc_segment`, and longtext serialization before writing XML.
- `serialize_texts` is language-driven and uses `DDIF_TABL_GET` repeatedly for additional languages, gated by `mo_i18n_params` and the main-language-only flag.
- No current ORTEC prefetch seam was found in the inspected serialize path for TABL. There is no call to the prefetch extension helpers from this class.
- Conclusion: TABL is still a plain DDIC-like serializer from the prefetch point of view; it would need a new local seam if the same sort of prefetch optimization is to be introduced here.

### TTYP
- Current serialize path is centered on `DDIF_TTYP_GET` in `zif_abapgit_object~serialize`.
- It collects DD40V/DD42V/DD43V data and writes XML plus longtexts.
- No current ORTEC prefetch seam was found in the inspected serialize path for TTYP.
- Conclusion: TTYP is structurally similar to TABL from a prefetch perspective and also lacks a local ORTEC hook at present.

### PROG
- The class already has a prefetch seam in `serialize_texts`.
- `serialize_texts` checks `zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( )` and, when active, calls `zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(...)`.
- On a miss, it falls back to the existing `SELECT DISTINCT ... FROM d010tinf` path and then reads the text pool for each language.
- The seam is narrow and already local to the translation-language discovery path rather than the main source serialization path.
- Conclusion: PROG already has a concrete prefetch seam and is therefore a good candidate for incremental extension without introducing a new provider pattern from scratch.

### FUGR
- The class already has multiple prefetch seams.
- `serialize_xml` uses `zcl_abapgit_ortec_ser_pref_ext=>get_fugr_areat(...)` for the function-group short text and then falls back to `SELECT SINGLE ... FROM tlibt` on miss.
- `functions` uses `zcl_abapgit_ortec_ser_pref_ext=>get_fugr_enlfdir(...)` and then falls back to `SELECT ... FROM enlfdir`.
- `serialize_functions` uses `zcl_abapgit_ortec_ser_pref_ext=>get_fugr_func_metadata(...)` for function-module metadata and falls back to `SELECT SINGLE exten3 ... FROM enlfdir`.
- Conclusion: FUGR already has direct ORTEC prefetch access in the core serialize branches and is therefore already on the prefetch-aware path.

## ORTEC prefetch helper relevance

### Existing batch-envelope pattern
- The strongest reusable pattern is the DOMA/DTEL batch envelope in [src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap](src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap).
- It exposes `extract_for_batch` and `inject_batch_from_buffer` and uses the header/entry structures `ZAOG_SER_DD_BHDR`, `ZAOG_SER_DD_BENTRY`, and `ZAOG_SER_DD_BENTRY_TT`.
- The implementation is explicitly batch-oriented and is already used by the orchestrator layer for a dispatch-wide prefetch buffer.

### Reuse assessment for the four families
- For TABL and TTYP, the DOMA/DTEL batch-envelope pattern is directly relevant as a design reference because these families are DDIC-like and currently do not have any local ORTEC prefetch seam.
- For PROG and FUGR, the current state is already prefetch-aware through object-local helper accessors rather than a new batch-envelope abstraction. The existing provider pattern is therefore less urgent for these two families than for TABL/TTYP.
- In other words: the batch-envelope shape is a good fit for future DDIC-family prefetch work, while PROG/FUGR already have a more immediate object-local seam to extend.

## Practical seam conclusion

- TABL and TTYP: no current ORTEC seam in the serialize path; they would need a new seam if common prefetch support is to be added.
- PROG: already has a working prefetch seam in the text-pool path.
- FUGR: already has multiple prefetch seams in the metadata/short-text path.
- The DOMA/DTEL batch-provider implementation is the strongest nearby precedent for any future DDIC-family batch work.
