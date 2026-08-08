# SER-SLICE-4 Package A TABL/TTYP Adversarial Review Cycle 1

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_A_ADVERSARIAL_REVIEW_CYCLE_1
CYCLE=1
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
STATUS=FAIL
VERDICT=REVISE_AND_REVIEW_ONCE
```

## Scope Control

Reviewed only the requested design/context files and the requested source scope. No productive ABAP/DDIC/UI/RFC/test files, diagrams, archive files, editor memory, or state file were modified. This file is the only written artifact.

Allowed design/context evidence:
- `.memory/logs/serialization_slice_4_tabl_ttyp_design.md`
- `.memory/logs/serialization_slice_4_shared_infrastructure.md`
- `.memory/logs/serialization_slice_4_common_discovery.md`
- `.memory/logs/serialization_slice_3_provider_contract.md`
- `.memory/logs/serialization_slice_3_clas_intf.md`

Allowed source evidence:
- `src/objects/tabl/zcl_abapgit_object_tabl.clas.abap`
- `src/objects/zcl_abapgit_object_ttyp.clas.abap`
- `src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap`

## Evidence Matrix

```text
E1=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §4 prepare_tabl: skips DD02T rows with initial DDTEXT and claims this mirrors serialize_texts's defensive skip.
E2=src/objects/tabl/zcl_abapgit_object_tabl.clas.abap serialize_texts: SELECT DISTINCT language from DD02V, then deletes only when DDIF_TABL_GET returns LS_DD02V-DDLANGUAGE initial; it does not test DDTEXT initial.
E3=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §5/§7: GET_TABL_EXTRAS must return RV_FOUND=true with initial ES_TDDAT for a checked table that has no TDDAT row.
E4=src/objects/tabl/zcl_abapgit_object_tabl.clas.abap read_extras: SELECT SINGLE * FROM TDDAT ignores SY-SUBRC and therefore treats absent row as initial TDDAT output.
E5=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §4/§6: MT_TABL_EXTRAS contains only rows selected from TDDAT; extract marks P only if text exists or a TDDAT row exists; inject only repopulates text/extras caches.
E6=src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap prepare_tobj/get_tobj_data: existing TOBJ precedent pre-inserts one cache row per requested tabname before overlaying optional TDDAT/TVDIR/TVIMF rows, so negative optional-row facts are representable.
E7=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §6/§9: mandates method name INJECT_BATCH_FROM_BUFFER_TABL_TTYP.
E8=ABAP global method names are limited to 30 characters; INJECT_BATCH_FROM_BUFFER_TABL_TTYP is 34 characters.
E9=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §6: inject validation lists IMPORT/version/object_count/duplicate ENTRIES/corrupt IMPORT, then inserts payload rows; it does not specify duplicate or unexpected payload-key rejection.
E10=.memory/logs/serialization_slice_4_shared_infrastructure.md §3/§4: shared constraints require bounded actual-byte admission and corrupt provider buffers to degrade to provider MISS, not silently accept inconsistent contents.
E11=src/objects/zcl_abapgit_object_ttyp.clas.abap serialize: one DDIF_TTYP_GET call, no i18n loop; design's TTYP DEFER factual basis matches current source.
E12=src/objects/tabl/zcl_abapgit_object_tabl.clas.abap serialize: one main DDIF_TABL_GET returns DD02V/DD09L/DD03P/DD05M/DD08V/DD12V/DD17V/DD35V/DD36M, then standard cleanup; design's central DD03P exclusion rationale matches current source shape.
```

## Findings

```text
ID=AR-1-001 / TT-001
SEVERITY=BLOCKER
CLAIM=prepare_tabl may skip DD02T rows whose DDTEXT is initial because that mirrors serialize_texts's existing "don't save this lang" behavior.
COUNTEREXAMPLE=A table has an extra-language DD02T/DD02V row with DDLANGUAGE populated and DDTEXT initial. Current serialize_texts discovers the language, calls DDIF_TABL_GET, keeps the language unless LS_DD02V-DDLANGUAGE is initial, and appends a DD02 text row by MOVE-CORRESPONDING. The proposed provider drops the row before the consumer sees it.
EVIDENCE=E1,E2
IMPACT=correctness / source-claim false / byte parity regression
REQUIRED_CHANGE=Remove the DDTEXT-initial skip from prepare_tabl unless a live DDIF_TABL_GET parity proof shows DDLANGUAGE becomes initial for that exact case; rewrite §4/§11 so the design accurately states the source condition is DDLANGUAGE initial, not DDTEXT initial.
RETEST=Add a parity test with an existing extra-language text row whose DDTEXT is initial; feature OFF, feature ON batch OFF, and feature ON batch ON must produce byte-identical TABL XML including the same I18N language/text-row behavior.
```

```text
ID=AR-1-002 / TT-002
SEVERITY=BLOCKER
CLAIM=GET_TABL_EXTRAS can unambiguously return RV_FOUND=true with initial ES_TDDAT for a checked table that has no TDDAT row.
COUNTEREXAMPLE=Table A has i18n text but no TDDAT row. The design creates a P entry because text exists, but inject only stores MT_TABL_TEXT and MT_TABL_EXTRAS; no per-table checked marker survives. A keyed read of MT_TABL_EXTRAS for A returns not found, so a literal implementation falls back to SELECT SINGLE. Conversely, the prose says RV_FOUND=true whenever MT_TABL_EXTRAS was ever populated for the batch, which would incorrectly treat unrelated rowless tables as cached if some other table B had TDDAT. In an all-rowless batch, extract may return INITIAL and cannot cache negative TDDAT facts at all.
EVIDENCE=E3,E4,E5,E6
IMPACT=correctness contract ambiguity / false MISS or false HIT / performance invariant not met / telemetry divergence
REQUIRED_CHANGE=Represent checked TABL identities explicitly, e.g. pre-insert one per requested TABL key into a TABL extras/checked cache before overlaying TDDAT rows, and carry that checked set through the batch envelope or entries so inject can answer per-object absence. Alternatively, downgrade the design to say missing TDDAT rows intentionally fall back and remove every claim that absence is cached as RV_FOUND=true.
RETEST=Add cases for (1) text-only table with no TDDAT, (2) no-text/no-TDDAT table in a mixed batch, (3) batch with one table having TDDAT and one without, and (4) all-rowless TABL batch. In each case GET_TABL_EXTRAS must return the specified per-object result without cross-object leakage, and serialized output/telemetry must match the revised contract.
```

```text
ID=AR-1-003 / TT-003
SEVERITY=BLOCKER
CLAIM=The mandatory suffixed method name INJECT_BATCH_FROM_BUFFER_TABL_TTYP is a decision-free implementation constraint.
COUNTEREXAMPLE=INJECT_BATCH_FROM_BUFFER_TABL_TTYP is 34 characters. ABAP method names are limited to 30 characters, so an implementation following the design cannot compile.
EVIDENCE=E7,E8
IMPACT=activation failure / naming collision risk unresolved
REQUIRED_CHANGE=Choose compile-safe names <=30 characters before implementation, and update §6/§9 consistently. Example shape: EXTRACT_FOR_BATCH_TABL_TTYP is 27 and safe, but the inject method needs a shorter form such as INJECT_BATCH_FROM_BUF_TABL or another explicit <=30-character name.
RETEST=Run a method-name length scan over all new declarations/implementations/call sites before syntax validation; every method name must be <=30 characters, then live ABAP syntax check must pass.
```

```text
ID=AR-1-004 / TT-004
SEVERITY=MAJOR
CLAIM=inject_batch_from_buffer_tabl_ttyp follows the established validation sequence and satisfies strict duplicate/unexpected-key handling.
COUNTEREXAMPLE=A corrupt or stale buffer can contain payload rows for TABL keys that are absent from ENTRIES, duplicate TABL text payload rows for the same TABNAME/DDLANGUAGE, duplicate extras rows for the same TABNAME, or payload rows inconsistent with entry object type/state. The design only mandates duplicate ENTRIES rejection; silent INSERT into hashed caches or ignored SY-SUBRC can collapse duplicates or accept unexpected rows. A worker then may report/provider-hit data for a key whose entry did not authorize/account for that payload.
EVIDENCE=E9,E10
IMPACT=corrupt-buffer safety / strict duplicate handling / actual-byte and telemetry correlation
REQUIRED_CHANGE=Before mutating caches, validate every payload key belongs to exactly one TABL entry, no payload key is duplicated, no TTYP/unknown object payload exists, and entry state matches payload presence according to the final HIT/MISS/checked-absence contract. Reject the whole buffer on violation so the RFC worker falls back to provider MISS.
RETEST=Add corrupt-buffer tests for duplicate text payload, duplicate extras payload, payload without entry, entry without allowed payload under the final state rules, and TTYP/unknown-object payload; each must reject the whole buffer and leave caches clean.
```

## Central Decision Assessment

The DD03P/DD43V exclusion is defensible within the supplied evidence. Current TABL source really obtains DD03P/DD05M/DD08V/DD12V/DD17V/DD35V/DD36M from one main `DDIF_TABL_GET`, and `clear_dd03p_fields`/include-derived cleanup makes the flattening risk credible. The design is not excessively conservative on that central boundary; the failure is in the narrower TABL provider specification.

## Prior/Changed Section Verification

```text
CONFIRMED=TABL main DDIF_TABL_GET shape; TABL serialize_texts per-language DDIF_TABL_GET; TABL read_extras ignores TDDAT SY-SUBRC; TTYP has one DDIF_TTYP_GET and no i18n loop; ORCH currently counts only DD buffer and shared infra correctly requires future summation.
REJECTED=DDTEXT-initial skip as source-equivalent; TDDAT checked-absence semantics as implementable with only MT_TABL_EXTRAS rows; INJECT_BATCH_FROM_BUFFER_TABL_TTYP as ABAP-safe name; injection validation as complete under common provider constraints.
NOT_VERIFIED=External SER-SLICE-3 ranking note about DD03P prevalence/benefit was not present in the allowed context. The central exclusion was assessed against the supplied source and design evidence only.
```

## Verdict

```text
VERDICT=REVISE_AND_REVIEW_ONCE
OPEN_BLOCKER=3/TT-001,TT-002,TT-003
OPEN_MAJOR=1/TT-004
OPEN_MINOR=0
CLOSED=none
NEXT=Revise Package A design to fix source parity, explicit checked-absence representation, ABAP-safe names, and payload-key validation; then rerun adversarial review before implementation.
```

## Cycle 2

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_A_ADVERSARIAL_REVIEW_CYCLE_2
CYCLE=2
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
STATUS=FAIL
VERDICT=REVISE_AND_REVIEW_ONCE
```

## Cycle 2 Scope Control

Reviewed only the requested revised design/context files and source scope. No productive ABAP/DDIC/UI/RFC/test files, diagrams, archive files, state files, or editor-memory files were modified. This appended Cycle 2 section is the only written artifact.

Allowed design/context evidence:
- `.memory/logs/serialization_slice_4_tabl_ttyp_design.md`
- `.memory/reviews/serialization_slice_4_tabl_ttyp_adversarial.md`
- `.memory/logs/serialization_slice_4_shared_infrastructure.md`
- `.memory/logs/serialization_slice_3_clas_intf.md`

Allowed source evidence:
- `src/objects/tabl/zcl_abapgit_object_tabl.clas.abap`
- `src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap`
- `src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap`

## Cycle 2 Evidence Matrix

```text
C2-E1=src/objects/tabl/zcl_abapgit_object_tabl.clas.abap serialize_texts: after DDIF_TABL_GET, the only defensive language skip is `sy-subrc <> 0 OR ls_dd02v-ddlanguage IS INITIAL`; DDTEXT is not tested.
C2-E2=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §4 prepare_tabl: cycle-2 pseudocode skips DD02T rows only when `ls_dd02t-ddlanguage IS INITIAL` and explicitly preserves initial DDTEXT rows.
C2-E3=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §4/§5: prepare_tabl unconditionally pre-inserts one mt_tabl_extras row per requested name before TDDAT overlay; get_tabl_extras returns found iff that per-tabname row exists, with initial TDDAT representing checked absence.
C2-E4=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §6/§7/§11: extract_for_batch_tabl treats a successful mt_tabl_extras read as P, appends exactly one extras payload row for every P entry, and tests checked-but-empty/all-empty batches as P buffers.
C2-E5=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §3: the wire-envelope state rule still says state P iff text list is non-empty or a TDDAT row exists, and says a table with zero extra translations and zero TDDAT row is a valid M.
C2-E6=Exhaustive character count for the new method names: extract_for_batch_tabl=22, inject_batch_from_buffer_tabl=29, clear_tabl_cache=16, get_tabl_i18n=13, get_tabl_extras=15, prepare_tabl=12; all are <=30.
C2-E7=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §6 inject_batch_from_buffer_tabl: validates provider_id, entry obj_type/state, duplicate text payload rows, duplicate extras payload rows, extras count against P entries, and text/extras payload keys against P entries before mutating caches.
C2-E8=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §5 get_tabl_i18n contract: rv_found=false when the table has zero cached text rows.
C2-E9=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §7/§11: a P entry absent from tabl_text is described as checked no translations with get_tabl_i18n rv_found=true, and all-checked-empty TABL batches are required to avoid being treated as unprepared.
C2-E10=src/objects/tabl/zcl_abapgit_object_tabl.clas.abap read_extras: SELECT SINGLE TDDAT ignores SY-SUBRC and then fills ABAP language version; absent TDDAT is a valid initial-output condition.
```

## Cycle 2 Closure Tracking

```text
ID=AR-2-001 / TT-001
STATUS=CLOSED
SEVERITY=BLOCKER
CLAIM=Cycle 2 prepare_tabl now mirrors serialize_texts by skipping only initial DDLANGUAGE, not initial DDTEXT.
COUNTEREXAMPLE=No surviving counterexample in the reviewed text: a DD02T row with populated DDLANGUAGE and initial DDTEXT is kept by §4, matching the current serialize_texts condition.
EVIDENCE=C2-E1,C2-E2
IMPACT=correctness / source parity preserved
REQUIRED_CHANGE=none for TT-001
RETEST=Run the required parity case with an existing extra-language DD02T row whose DDTEXT is initial; feature OFF, feature ON batch OFF, and feature ON batch ON must produce byte-identical TABL XML.
```

```text
ID=AR-2-002 / TT-002
STATUS=OPEN
SEVERITY=BLOCKER
CLAIM=Cycle 2 fully fixes checked-vs-not-prepared TDDAT absence and makes state='P' follow unambiguously from unconditional mt_tabl_extras pre-insert.
COUNTEREXAMPLE=§4/§5/§6 now define the correct mechanism: every prepared TABL has an mt_tabl_extras row, a missing TDDAT row is represented by initial TDDAT, and extract emits P plus exactly one extras payload row. But §3, the formal wire-envelope state definition, still states P only when text is non-empty or a real TDDAT row exists and explicitly classifies zero-text/zero-TDDAT as M. A literal implementer following §3 will produce M/no extras for the exact checked-empty case §6/§7/§11 require to be P/one initial-extras row, reintroducing the original ambiguity.
EVIDENCE=C2-E3,C2-E4,C2-E5,C2-E10
IMPACT=correctness contract ambiguity / false MISS / decision-free implementation failure
REQUIRED_CHANGE=Rewrite §3 so P means "TABL was prepared and has its mandatory checked extras row" for this provider, even when both text rows and the overlaid TDDAT content are empty. Remove the stale zero-text/zero-TDDAT-is-M rule, or explicitly limit M to defensive/not-prepared-or-corrupt-fallback cases consistent with §6.
RETEST=Use the TT-002 tests already listed in §11, especially all-checked-empty TABL batch: extract must return a non-initial buffer, entries must contain P, tabl_extras must contain one initial row per P entry, injected get_tabl_extras must return true/initial TDDAT, and no prose section may define that case as M.
```

```text
ID=AR-2-003 / TT-003
STATUS=CLOSED
SEVERITY=BLOCKER
CLAIM=Every new method name introduced by this TABL design is within ABAP's 30-character method-name limit.
COUNTEREXAMPLE=No counterexample found by exhaustive count of the six requested method names.
EVIDENCE=C2-E6
IMPACT=activation risk removed
REQUIRED_CHANGE=none for TT-003
RETEST=Before implementation, rerun the method-name scan over all actual declarations/implementations/call sites touched by Packages A/B/C together; every method name must remain <=30 characters.
```

```text
ID=AR-2-004 / TT-004
STATUS=CLOSED
SEVERITY=MAJOR
CLAIM=inject_batch_from_buffer_tabl now rejects stale/corrupt TABL buffers before cache mutation, including provider_id, entry type/state, duplicate payload rows, and payload-to-P-entry correlation.
COUNTEREXAMPLE=No surviving counterexample in the §6 validation pseudocode: provider_id is checked; non-TABL or non-P/M entries are rejected; duplicate text/extras payload keys are rejected; extras rows must be 1:1 with P entries; extras and text keys must belong to P entries. Given §6's producer-side invariant that every P entry is generated from a successful mt_tabl_extras read and emits exactly one extras row, the consumer-side 1:1 extras-to-P validation is internally consistent.
EVIDENCE=C2-E3,C2-E4,C2-E7
IMPACT=corrupt-buffer safety restored for the TABL payload contract
REQUIRED_CHANGE=none for TT-004 itself; the TT-002 §3 state contradiction must still be corrected so the producer invariant remains globally unambiguous.
RETEST=Run the §11 corrupt-buffer tests for unexpected provider_id, unexpected entry obj_type/state, duplicate text payload, duplicate extras payload, extras-count mismatch vs P entries, extras/text payload key not in P entries, unknown version, object_count mismatch, duplicate ENTRIES row, and corrupt IMPORT; each must reject the whole buffer and leave caches clean.
```

## Cycle 2 New Findings

```text
ID=AR-2-005 / TT-005
SEVERITY=MAJOR
CLAIM=Cycle 2's checked-empty semantics apply consistently to both TABL sub-features: extras absence and i18n absence.
COUNTEREXAMPLE=A prepared TABL with zero extra-language DD02T rows and an initial checked extras row is described in §7 as a valid checked/no-translations HIT for get_tabl_i18n with rv_found=true, and §11 requires all-checked-empty batches to prove this. But §5's actual get_tabl_i18n accessor contract returns rv_found=false whenever the table has zero cached text rows. That sends serialize_texts down the unchanged fallback SELECT/CALL path for the exact checked-empty i18n case the design claims is cached, and leaves the required §7/§11 behavior unimplementable without adding a separate text-checked marker or deriving i18n checked status from mt_tabl_extras.
EVIDENCE=C2-E3,C2-E8,C2-E9
IMPACT=performance invariant failure / telemetry ambiguity / decision-free implementation contradiction
REQUIRED_CHANGE=Define one canonical checked marker for TABL i18n absence. Either make get_tabl_i18n treat the per-tabname mt_tabl_extras row as proof that prepare_tabl checked i18n for that TABL and return rv_found=true with empty tables, or add an explicit mt_tabl_text_checked/entry marker carried through extract/inject. Then align §5, §7, §11, and telemetry wording to that single rule.
RETEST=Add a zero-extra-language TABL case with batch prefetch active: get_tabl_i18n must return the revised specified value without falling back to the per-object SELECT/DDIF path, serialized output must remain byte-identical, and a second worker invocation with an unrelated TABL must not leak the previous checked-empty state.
```

## Cycle 2 Verdict

```text
VERDICT=REVISE_AND_REVIEW_ONCE
OPEN_BLOCKER=1/TT-002
OPEN_MAJOR=1/TT-005
OPEN_MINOR=0
CLOSED=TT-001,TT-003,TT-004
NEXT=Make one final design revision to remove the stale §3 M-state rule and define a canonical checked-empty i18n marker/contract, then run the permitted Cycle 3 review.
```

## Cycle 3

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SER_SLICE_4_PACKAGE_A_ADVERSARIAL_REVIEW_CYCLE_3
CYCLE=3
BASELINE_HEAD=e5e10d62137033801739b8a53c69bdff9cfa650e
STATUS=PASS
VERDICT=APPROVE
```

## Cycle 3 Scope Control

Reviewed only the requested revised design/context files and source scope. No productive ABAP/DDIC/UI/RFC/test files, diagrams, archive files, state files, or editor-memory files were modified. This appended Cycle 3 section is the only written artifact.

Allowed design/context evidence:
- `.memory/logs/serialization_slice_4_tabl_ttyp_design.md`
- `.memory/reviews/serialization_slice_4_tabl_ttyp_adversarial.md`
- `.memory/logs/serialization_slice_4_shared_infrastructure.md`

Allowed source evidence:
- `src/objects/tabl/zcl_abapgit_object_tabl.clas.abap`
- `src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap`

## Cycle 3 Evidence Matrix

```text
C3-E1=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §3: state='P' iff prepare_tabl processed the TABL at all, derived from existence of the mt_tabl_extras checked row; state='M' is defensive-only for an object PREPARE never saw; zero text plus zero TDDAT is explicitly valid P.
C3-E2=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §4: prepare_tabl unconditionally inserts one mt_tabl_extras row per requested TABL before overlaying optional TDDAT content.
C3-E3=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §5 get_tabl_i18n: rv_found=true iff an mt_tabl_extras row exists for iv_tabname; empty et_i18n_langs/et_dd02_texts is valid under that marker.
C3-E4=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §5 get_tabl_extras: rv_found=true iff an mt_tabl_extras row exists for iv_tabname; initial es_tddat represents checked absence of a TDDAT row.
C3-E5=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §6 extract_for_batch_tabl: READ mt_tabl_extras success emits P and exactly one extras payload row; M is described as defensive and unreachable in correct PREPARE coverage.
C3-E6=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §6 inject_batch_from_buffer_tabl: validates extras payload 1:1 with P entries and repopulates mt_tabl_extras from that payload, preserving the checked-marker across workers.
C3-E7=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §7: P without tabl_text is valid checked-empty i18n and valid extras hit; neither sub-feature treats empty content as MISS.
C3-E8=.memory/logs/serialization_slice_4_tabl_ttyp_design.md §11: required tests explicitly cover zero extra-language rows, no TDDAT rows, neither text nor TDDAT, and all-checked-empty TABL batches as P/checked cases.
C3-E9=src/objects/tabl/zcl_abapgit_object_tabl.clas.abap read_extras ignores SELECT SINGLE TDDAT sy-subrc and treats absent TDDAT as initial output; serialize_texts skips only initial DDLANGUAGE, not empty DDTEXT.
C3-E10=src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap existing TOBJ/FUGR/PROG patterns pre-insert per-key cache rows for optional child data and return found from the per-key row, matching the revised TABL marker approach.
```

## Cycle 3 Closure Tracking

```text
ID=AR-3-002 / TT-002
STATUS=CLOSED
SEVERITY=BLOCKER
CLAIM=Cycle 3 removes the stale §3 rule and makes P/M follow the same checked-marker rule used by §5/§6/§7/§11.
COUNTEREXAMPLE=No surviving counterexample in the reviewed text. §3 now defines P as "prepare_tabl processed this table" via mt_tabl_extras existence, and explicitly says zero translations plus zero TDDAT is P, not M. §6's extract rule reads the same mt_tabl_extras key to emit P and an extras payload row, §7 treats P-with-empty-text as a hit, and §11 requires all-checked-empty batches to remain real P buffers.
EVIDENCE=C3-E1,C3-E2,C3-E4,C3-E5,C3-E6,C3-E7,C3-E8,C3-E9,C3-E10
IMPACT=correctness contract ambiguity resolved / false MISS risk removed
REQUIRED_CHANGE=none for TT-002
RETEST=Implement the §11 TT-002 cases: text-only/no-TDDAT, no-text/no-TDDAT in mixed batch, mixed TDDAT-present/absent, and all-rowless TABL batch. Each must produce P plus exactly one extras row per P entry and get_tabl_extras=true with initial TDDAT where absent.
```

```text
ID=AR-3-005 / TT-005
STATUS=CLOSED
SEVERITY=MAJOR
CLAIM=Cycle 3 aligns get_tabl_i18n's checked-empty contract to the same mt_tabl_extras marker as get_tabl_extras.
COUNTEREXAMPLE=No surviving counterexample in the reviewed text. §5 now states get_tabl_i18n returns rv_found=true iff mt_tabl_extras has the iv_tabname row, and that an empty text/language result is valid once that marker is present. This is the same marker used by get_tabl_extras and by §3's canonical P rule.
EVIDENCE=C3-E1,C3-E2,C3-E3,C3-E4,C3-E7,C3-E8,C3-E10
IMPACT=i18n checked-empty performance invariant restored / telemetry ambiguity removed
REQUIRED_CHANGE=none for TT-005
RETEST=Implement the §11 zero-extra-language TABL case with batch prefetch active: get_tabl_i18n must return true with empty outputs without falling back to SELECT/DDIF, serialized XML must remain byte-identical, and a later worker invocation for another TABL must not inherit stale text state.
```

## Cycle 3 New Findings

```text
NEW_FINDINGS=0
OPEN_BLOCKER=0
OPEN_MAJOR=0
OPEN_MINOR=0
```

## Cycle 3 Verdict

```text
VERDICT=APPROVE
CLOSED=TT-002,TT-005
OWNER_DECISION_REQUIRED=NO
NEXT=Proceed to implementation with the §11 acceptance tests and the method-name length scan retained as mandatory validation gates.
```