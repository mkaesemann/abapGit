# SER-SLICE-4 Package C FUGR Adversarial Review Cycle 1

```text
TASK=SER_SLICE_4_PACKAGE_C_ADVERSARIAL_REVIEW_CYCLE_1
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
REVIEW_SCOPE=READ_ONLY
VERDICT=REVISE_AND_REVIEW_ONCE
```

## Evidence Matrix

```text
E-FG-DES-001=.memory/logs/serialization_slice_4_fugr_design.md section 0: claims existing FUGR seams and same root-cause gap as Package B.
E-FG-DES-002=.memory/logs/serialization_slice_4_fugr_design.md sections 2/6: proposes RFCSCOPE/RFCVERS in ZAOG_SER_FUGR_FN_BROW and ty_fugr_func_meta using TFDIR field references plus rfc_fields_valid.
E-FG-DES-003=.memory/logs/serialization_slice_4_fugr_design.md section 4: inject pseudocode inserts only funcname/exception_classes into mt_fugr_func_meta, then references mt_fugr_tfdir.
E-FG-DES-004=.memory/logs/serialization_slice_4_fugr_design.md sections 0/1/10: claims FUGR main-program i18n is covered transitively by Package B and not re-tested here.
E-FG-SRC-001=src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap before_dispatch: computes DD/OO_BATCH/MSAG buffers only and dispatches no iv_prefetch_buffer_ext value.
E-FG-SRC-002=src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap: worker injects iv_prefetch_buffer_ext only if it is non-initial.
E-FG-SRC-003=src/objects/zcl_abapgit_object_fugr.clas.abap functions/serialize_xml/serialize_functions: consumes get_fugr_enlfdir, get_fugr_areat, get_fugr_func_metadata, then falls back to ENLFDIR/TFDIR selects.
E-FG-SRC-004=src/objects/zcl_abapgit_object_fugr.clas.abap ty_function: RFCSCOPE/RFCVERS are declared as TYPE c LENGTH 1/10 with comments that data elements are not on older releases.
E-FG-SRC-005=src/objects/zcl_abapgit_object_fugr.clas.abap serialize_functions: current TFDIR read is dynamic and caught with cx_sy_dynamic_osql_semantics.
E-FG-SRC-006=src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap ty_fugr_func_meta/prepare_fugr/get_fugr_func_metadata: current func metadata cache has only funcname and exception_classes, populated from ENLFDIR.
E-FG-SRC-007=src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap collect_keys: FUGR main program is added to et_prog for prepare_prog_langs.
E-FG-SRC-008=src/objects/zcl_abapgit_object_fugr.clas.abap serialize_texts: FUGR text-pool language discovery performs its own D010TINF SELECT and never calls get_prog_tpool_languages.
E-FG-SRC-009=src/objects/zcl_abapgit_object_prog.clas.abap serialize_texts: PROG is the class that consumes get_prog_tpool_languages.
E-FG-PROG-001=.memory/logs/serialization_slice_4_prog_design.md: Package B scope is PROG's existing get_prog_tpool_languages consumer, not FUGR's separate serialize_texts implementation.
E-FG-PREC-001=.memory/logs/serialization_slice_3_clas_intf.md and shared infra: generic envelope reuse and whole-buffer rejection/clear-first pattern are valid precedents.
```

## Verified Claims

```text
ROOT_CAUSE_CLAIM=VERIFIED. before_dispatch currently computes DD, OO_BATCH, and MSAG buffers only; the legacy iv_prefetch_buffer_ext parameter remains initial, and the RFC worker injects ZCL_ABAPGIT_ORTEC_SER_PREF_EXT only when that parameter is non-initial. Therefore the three existing FUGR seams are not populated in the RFC worker today.
HIT_MISS_RULE=VERIFIED_FOR_METADATA. prepare_fugr pre-creates mt_fugr_areat and mt_fugr_enlfdir rows per requested area, func_meta is derived only from ENLFDIR rows, and extract loops func_meta only through the ENLFDIR row. A func_meta-only hit is not reachable from the current source shape.
OPTION_C_EXCLUSION=ACCEPTED. Full source/include batching is adequately excluded as MEASURE_FIRST because the current source path is kernel/RPY/generated-source based, no bulk DB equivalent is identified, and the design keeps metadata payloads bounded.
COMMON_PROVIDER_CONSTRAINTS=PARTIAL. Envelope reuse, clear-first worker injection, whole-buffer rejection, and combined byte-sum requirements are stated, but the TFDIR cache contradiction below violates decision-free implementation and release compatibility.
```

## Findings

```text
ID=FG-001
SEVERITY=BLOCKER
CLAIM=Section 6 preserves the existing release gate, so releases without TFDIR-RFCSCOPE/RFCVERS cannot dump or silently produce wrong data.
COUNTEREXAMPLE=On an older release where TFDIR lacks RFCSCOPE/RFCVERS, the proposed DDIC wire row and private cache fields typed as tfdir-rfcscope/tfdir-rfcvers are compile/activation-time references to fields the source itself says may not exist. cx_sy_dynamic_osql_semantics only protects the dynamic SELECT at runtime; it cannot protect DDIC activation or class syntax for missing field-type references. The proposed inline SELECT target DATA(lt_tfdir) also leaves field typing dependent on the missing SELECT list instead of on a release-stable target structure.
EVIDENCE=E-FG-DES-002,E-FG-SRC-004,E-FG-SRC-005
IMPACT=compatibility/release-gating
REQUIRED_CHANGE=Replace all new RFCSCOPE/RFCVERS type references with release-stable owned/primitive compatible types, matching the existing serializer shape (for example TYPE c LENGTH 1 and TYPE c LENGTH 10, or dedicated DDIC types not based on TFDIR fields). Use an explicit compatible bulk SELECT target structure inside the TRY block, not inline type inference from potentially absent TFDIR fields. State that the TRY/CATCH protects only the dynamic SQL execution, while type declarations remain release-stable by construction.
RETEST=Activation/syntax on a release where TFDIR lacks RFCSCOPE/RFCVERS succeeds; a worker with rfc_fields_valid=false falls back through the existing dynamic SELECT/CATCH path and serializes byte-identically to feature OFF.
```

```text
ID=FG-002
SEVERITY=MAJOR
CLAIM=The new TFDIR fields are decision-free additions to the existing FUGR metadata cache and worker injection path.
COUNTEREXAMPLE=Sections 2 and 6 place rfcscope/rfcvers/rfc_fields_valid in the func payload and ty_fugr_func_meta, but section 4's injection pseudocode inserts only funcname and exception_classes into mt_fugr_func_meta and then says a separate mt_fugr_tfdir is inserted/cleared. No mt_fugr_tfdir type, class-data declaration, accessor, extract validation, or worker telemetry contract exists in the real source or in the design. An implementer could follow section 4 literally and drop the RFC fields during injection, making lv_rfc_prefetched false or initial in the worker even when the main process bulk-read valid TFDIR data.
EVIDENCE=E-FG-DES-002,E-FG-DES-003,E-FG-SRC-006
IMPACT=correctness/implementation-ambiguity
REQUIRED_CHANGE=Choose one cache shape and make every section match it. The smallest amendment is to extend ty_fugr_func_meta only, remove all mt_fugr_tfdir references, and have inject_batch_from_buffer_fugr insert funcname, exception_classes, rfcscope, rfcvers, and rfc_fields_valid into mt_fugr_func_meta. If a separate cache is intended instead, fully specify its type, key, extraction, import, clear, accessor, duplicate behavior, and tests.
RETEST=Extract/inject round-trip with rfc_fields_valid=true proves RFCSCOPE/RFCVERS survive into get_fugr_func_metadata; a second inject with no TFDIR payload clears prior RFC field values; corrupt/duplicate/object_count failures reject the whole FUGR buffer without partial cache mutation.
```

```text
ID=FG-003
SEVERITY=MAJOR
CLAIM=FUGR main-program text-pool i18n is already covered transitively by Package B once Package B is implemented, so Package C correctly avoids duplicating or testing it.
COUNTEREXAMPLE=collect_keys does add each FUGR main program to et_prog, so Package B can prepare mt_prog_langs for it. But the FUGR serializer's serialize_texts method does not consume mt_prog_langs: it performs its own SELECT DISTINCT language FROM d010tinf and READ TEXTPOOL flow. The only verified consumer of get_prog_tpool_languages is zcl_abapgit_object_prog, not zcl_abapgit_object_fugr. Therefore Package B's buffer can contain the FUGR main-program language row while FUGR serialization still misses it entirely under RFC batch.
EVIDENCE=E-FG-DES-004,E-FG-SRC-007,E-FG-SRC-008,E-FG-SRC-009,E-FG-PROG-001
IMPACT=cross-package-correctness/performance-claim
REQUIRED_CHANGE=Either remove the synergy claim and explicitly classify FUGR text-pool i18n as still unoptimized/out-of-scope, or amend Package B/C so zcl_abapgit_object_fugr=>serialize_texts uses the same get_prog_tpool_languages seam with iv_program=iv_prog_name and the same language filtering semantics as PROG. If amended, include FUGR I18N_TPOOL parity and provider-hit tests rather than delegating them to PROG-only tests.
RETEST=A FUGR with extra-language text-pool entries serializes byte-identically with feature OFF, feature ON batch OFF, and feature ON batch ON; the batch worker records/usefully exercises a PROG-language cache hit for the FUGR main program or the design explicitly documents no such hit is expected.
```

## Non-Findings

```text
NF-001=Central root-cause claim is correct: the RFC worker never receives iv_prefetch_buffer_ext from before_dispatch today.
NF-002=The metadata HIT rule is internally consistent for the current source because func_meta cannot be extracted independently of the ENLFDIR row.
NF-003=Option C source/include exclusion is conservative but justified by absent measurement, kernel read semantics, and payload risk.
NF-004=The section 10 test list is broad for envelope failures and isolation, but must be amended by FG-001/FG-002/FG-003 retests before approval.
```

## Ledger

```text
OPEN_BLOCKER=1 FG-001
OPEN_MAJOR=2 FG-002,FG-003
OPEN_MINOR=0
CLOSED=none
VERDICT=REVISE_AND_REVIEW_ONCE
NEXT=Amend the design for release-stable TFDIR typing, a single unambiguous RFC metadata cache shape, and the FUGR/PROG i18n cross-reference before implementation.
```

## Cycle 2

```text
TASK=SER_SLICE_4_PACKAGE_C_ADVERSARIAL_REVIEW_CYCLE_2
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
REVIEW_SCOPE=READ_ONLY_EXCEPT_THIS_ARTIFACT
VERDICT=REVISE_AND_REVIEW_ONCE
```

## Cycle 2 Evidence Matrix

```text
E-FG2-DES-001=.memory/logs/serialization_slice_4_fugr_design.md packet header and sections 2/6: ZAOG_SER_FUGR_FN_BROW, ty_tfdir_rfc_row, and ty_fugr_func_meta use TYPE c LENGTH 1/10 for rfcscope/rfcvers and describe tfdir-rfcscope/tfdir-rfcvers only as the rejected cycle-1 defect.
E-FG2-DES-002=.memory/logs/serialization_slice_4_fugr_design.md sections 3/4/6: extract/inject/clear carry rfcscope/rfcvers/rfc_fields_valid through mt_fugr_func_meta only; no positive mt_fugr_tfdir cache contract remains.
E-FG2-DES-003=.memory/logs/serialization_slice_4_fugr_design.md section 6a: FUGR text-pool i18n false Package-B-transitive claim is removed and replaced by a proposed get_prog_tpool_languages seam in zcl_abapgit_object_fugr=>serialize_texts.
E-FG2-SRC-001=src/objects/zcl_abapgit_object_fugr.clas.abap ty_function: rfcscope TYPE c LENGTH 1 and rfcvers TYPE c LENGTH 10, with comments that data elements are not on older releases.
E-FG2-SRC-002=src/objects/zcl_abapgit_object_fugr.clas.abap serialize_functions: current RFCSCOPE/RFCVERS read is dynamic SELECT SINGLE from ('TFDIR') guarded by cx_sy_dynamic_osql_semantics.
E-FG2-SRC-003=src/objects/zcl_abapgit_object_fugr.clas.abap serialize_texts: current method SELECTs D010TINF languages, then always runs mo_i18n_params->trim_saplang_keyed_table, SORT, READ TEXTPOOL, and I18N_TPOOL add.
E-FG2-SRC-004=src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap get_prog_tpool_languages: clears et_tpool_i18n, returns early on language mismatch, and returns rv_found = abap_true only when mt_prog_langs has the program key.
E-FG2-SRC-005=src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap prepare: collect_keys inserts FUGR main programs into et_prog and prepare_prog_langs populates mt_prog_langs in the preparing process.
E-FG2-PROG-001=.memory/logs/serialization_slice_4_prog_design.md sections 0/3/4: Package B's batch envelope is the mechanism that makes mt_prog_langs available in RFC workers; without that worker injection, the existing generic ext buffer path is not populated by before_dispatch.
```

## Cycle 2 Closure Review

```text
ID=FG-001
STATUS=CLOSED
CLAIM=Cycle 2 replaces activation-time TFDIR field-type dependencies with release-stable rfcscope/rfcvers types matching zcl_abapgit_object_fugr's own ty_function.
COUNTEREXAMPLE=No surviving positive design path declares rfcscope/rfcvers TYPE tfdir-rfcscope/tfdir-rfcvers or relies on inline SELECT target inference for the new bulk cache row; the new bulk target is explicitly TYPE c LENGTH 1/10.
EVIDENCE=E-FG2-DES-001,E-FG2-SRC-001,E-FG2-SRC-002
IMPACT=compatibility/release-gating
REQUIRED_CHANGE=none for FG-001
RETEST=Implementation syntax/activation on a release without TFDIR-RFCSCOPE/RFCVERS plus parity on a release with the fields present.
```

```text
ID=FG-002
STATUS=CLOSED
CLAIM=Cycle 2 uses exactly one FUGR function metadata cache carrying exception_classes/rfcscope/rfcvers/rfc_fields_valid.
COUNTEREXAMPLE=The revised implementation pseudocode imports, exports, inserts, and clears only mt_fugr_func_meta/ty_fugr_func_meta for function metadata. Remaining mt_fugr_tfdir text is explanatory negative wording, not a second cache contract.
EVIDENCE=E-FG2-DES-002
IMPACT=correctness/implementation-ambiguity
REQUIRED_CHANGE=none for FG-002
RETEST=Extract/inject round-trip proves rfc_fields_valid=true and rfcscope/rfcvers survive into get_fugr_func_metadata, and a second inject clears old values.
```

```text
ID=FG-003
STATUS=CLOSED_WITH_NEW_DEFECT_FG-004
CLAIM=Cycle 2 no longer claims FUGR text-pool i18n is transitively covered by Package B and instead adds a FUGR-local serialize_texts seam.
COUNTEREXAMPLE=The false transitive claim is removed, and the proposed replacement correctly keeps mv_language as iv_language and keeps trim_saplang_keyed_table plus SORT after both branches. However the new pseudocode mishandles the accessor miss case; see FG-004.
EVIDENCE=E-FG2-DES-003,E-FG2-SRC-003,E-FG2-SRC-004,E-FG2-SRC-005
IMPACT=cross-package-correctness/performance-claim
REQUIRED_CHANGE=none for original FG-003 claim; address FG-004 before implementation.
RETEST=FUGR I18N_TPOOL parity under feature OFF, feature ON batch OFF, feature ON batch ON, plus an explicit cache-miss fallback case.
```

## Cycle 2 New Findings

```text
ID=FG-004
SEVERITY=MAJOR
CLAIM=Section 6a's new FUGR text-pool seam is a coherent, low-risk replacement for only the D010TINF SELECT and does not depend on Package B worker injection order.
COUNTEREXAMPLE=In an RFC worker where Package C is implemented but Package B's mt_prog_langs batch envelope is absent, corrupt, rejected, or simply does not contain this program/language, get_prog_tpool_languages clears et_tpool_i18n and returns rv_found = abap_false. Section 6a ignores the RETURNING value and unconditionally sets lv_fugr_i18n_prefetched = abap_true, so the original SELECT DISTINCT language FROM d010tinf fallback is skipped and the method silently serializes no I18N_TPOOL translations for a FUGR that has them. The same loss occurs on iv_language/mv_language mismatch because the accessor returns early after clearing the table.
EVIDENCE=E-FG2-DES-003,E-FG2-SRC-003,E-FG2-SRC-004,E-FG2-PROG-001
IMPACT=correctness/cross-package-ordering/partial-import
REQUIRED_CHANGE=Make lv_fugr_i18n_prefetched reflect get_prog_tpool_languages's RETURNING rv_found exactly, or call it in an IF expression mirroring the existing FUGR areat/enlfdir metadata seam pattern. Only skip the D010TINF SELECT when rv_found = abap_true; otherwise execute the unchanged SELECT fallback. Keep trim_saplang_keyed_table and SORT unconditionally after either branch.
RETEST=With feature ON and an intentionally empty/missing mt_prog_langs entry in a worker-style setup, a FUGR with extra-language text-pool rows still emits byte-identical I18N_TPOOL output via the fallback SELECT. Then repeat with a populated mt_prog_langs hit to prove the SELECT is skipped only on true hit.
```

## Cycle 2 Ledger

```text
OPEN_BLOCKER=0
OPEN_MAJOR=1 FG-004
OPEN_MINOR=0
CLOSED=FG-001,FG-002,FG-003
VERDICT=REVISE_AND_REVIEW_ONCE
NEXT=Amend section 6a so FUGR serialize_texts falls back to the existing D010TINF SELECT whenever get_prog_tpool_languages returns rv_found = abap_false, then review the narrow diff once.
```

## Cycle 3

```text
TASK=SER_SLICE_4_PACKAGE_C_ADVERSARIAL_REVIEW_CYCLE_3
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
REVIEW_SCOPE=READ_ONLY_EXCEPT_THIS_ARTIFACT
VERDICT=APPROVE
```

## Cycle 3 Evidence Matrix

```text
E-FG3-DES-001=.memory/logs/serialization_slice_4_fugr_design.md packet header CYCLE_3_FIXES and section 6a: lv_fugr_i18n_prefetched is assigned from zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages( ... ) instead of hardcoded abap_true.
E-FG3-DES-002=.memory/logs/serialization_slice_4_fugr_design.md section 6a: fallback SELECT DISTINCT language FROM d010tinf remains under IF lv_fugr_i18n_prefetched = abap_false, while trim_saplang_keyed_table and SORT remain unconditional after either branch.
E-FG3-SRC-001=src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap get_prog_tpool_languages: CLEAR et_tpool_i18n first; RETURNs before setting rv_found on language mismatch or cache miss; sets et_tpool_i18n and rv_found = abap_true only when mt_prog_langs contains the program key.
E-FG3-SRC-002=src/objects/zcl_abapgit_object_fugr.clas.abap serialize_texts: current D010TINF fallback path, trim_saplang_keyed_table, SORT, READ TEXTPOOL, and I18N_TPOOL add are the exact behavior section 6a preserves around the new seam.
E-FG3-SRC-003=src/objects/zcl_abapgit_object_prog.clas.abap serialize_texts and existing object serializers such as DOMA/DTEL/FUGR/TRAN: productive source already uses lv_prefetched = class=>method( EXPORTING ... IMPORTING ... ) functional-style calls with RETURNING plus IMPORTING output parameters.
E-FG3-SYNTAX-001=ABAP syntax pitfall review: the known shorthand hazard is omitted EXPORTING when mixed with CHANGING/RECEIVING; section 6a uses explicit EXPORTING and IMPORTING and no CHANGING clause.
```

## Cycle 3 Closure Review

```text
ID=FG-004
STATUS=CLOSED
CLAIM=Cycle 3 makes the FUGR text-pool seam skip the D010TINF fallback only when get_prog_tpool_languages reports a real HIT.
COUNTEREXAMPLE=No remaining reviewed section-6a path hardcodes lv_fugr_i18n_prefetched = abap_true. On language mismatch, absent mt_prog_langs row, absent Package B worker buffer, or rejected/corrupt Package B buffer, the real accessor clears et_tpool_i18n and leaves rv_found initial/abap_false; the revised pseudocode assigns that RETURNING value to lv_fugr_i18n_prefetched, so the existing D010TINF fallback executes. On a true cache hit, rv_found = abap_true and the SELECT is correctly skipped. The proposed call syntax is valid for this codebase's ABAP style because it uses explicit EXPORTING/IMPORTING blocks while assigning the RETURNING value, matching existing productive call sites.
EVIDENCE=E-FG3-DES-001,E-FG3-DES-002,E-FG3-SRC-001,E-FG3-SRC-002,E-FG3-SRC-003,E-FG3-SYNTAX-001
IMPACT=correctness/cross-package-ordering/partial-import
REQUIRED_CHANGE=none for FG-004
RETEST=Implementation must still include the stated worker-style cache-miss fallback parity test and populated-cache-hit test from cycle 2, but the design-level defect is closed.
```

## Cycle 3 New Findings

```text
none
```

## Cycle 3 Ledger

```text
OPEN_BLOCKER=0
OPEN_MAJOR=0
OPEN_MINOR=0
CLOSED=FG-001,FG-002,FG-003,FG-004
VERDICT=APPROVE
NEXT=Proceed to implementation with the required FUGR I18N_TPOOL miss/hit parity tests and real syntax/activation validation.
```