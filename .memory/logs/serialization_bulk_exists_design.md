# SER-1 — Bulk Exists Design

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER1_BULK_EXISTS_DESIGN
STATUS=SCOPE_REDUCED_ON_EVIDENCE (headline finding below)
DEPENDS_ON=.memory/logs/serialization_ser0_audit.md §2, §5,
  .memory/logs/serialization_ser0_audit_ortec.md §1
```

## Headline finding — most of SER-1 is already implemented

`ZCL_ABAPGIT_ORTEC_BULK_EXISTS=>filter_existing` **already** provides bulk
(`FOR ALL ENTRIES`) existence classification for `DOMA` (always active) and
`DTEL`, `CLAS`, `INTF` (switch-gated, `cs_bulk_exists-{dtel,clas,intf}_active`,
**default ON**), in addition to `DSYS`, `FUGR`, `MSAG`, `PROG`, `SHLP`,
`SMIM`, `TOBJ`, `TRAN`, `TTYP`, and `TABL` (also switch-gated, default ON).
This is called from `zcl_abapgit_tadir=>check_exists` (L212), which runs
**before** the filtered/existing object list ever reaches
`zcl_abapgit_serialize`. By the time the SER-2 batch orchestrator sees
`lt_tadir`, every row has already been confirmed to exist (or the type
fell back to a per-row standard check inside `check_exists` itself, still
before the serializer runs).

**Consequence:** the SER-2 planner does not need its own existence check,
does not need a `NOT_EXISTS` state, and does not need a new `UNKNOWN`
exposure — `UNKNOWN` already resolves to a standard per-object check
*inside* `filter_existing` itself, transparently, before the caller ever
sees the row. No new design or implementation is required for the
prototype's CLAS/INTF/DTEL/DOMA scope.

## What SER-1 actually needs (small, low-risk)

1. **Verification only, no code change**: confirm (owner-executed on IT8,
   or via a targeted ABAP Unit test written as part of SLICE 0, see the
   implementation-readiness manifest) that `cs_bulk_exists-{dtel,clas,
   intf}_active` are indeed ON in the target productive system's actual
   configuration, since "switch-gated, default ON" is a class-constant
   default, not a live runtime guarantee if some other code path ever
   flips `mv_bulk_exists_active` off. This is a one-line
   `cl_abap_unit_assert=>assert_true( zcl_abapgit_ortec_git_switch=>
   is_bulk_exists_active( ) )`-style smoke check, not a design.
2. **A registration mechanism for FUTURE types (SER-4), not the current
   four.** The owner brief explicitly asks for "extension mechanism for
   other measured object types." Today's mechanism is a single monolithic
   `CASE` statement inside one method. This is **acceptable as-is for the
   current 14 types** (small, stable list, low churn) but does not scale
   cleanly past a handful more additions without the method becoming
   unwieldy. Recommendation: introduce the mechanism **only when SER-4
   identifies the next type to add** (do not build unused abstraction
   now), but PRE-DEFINE its shape here so a future slice does not have to
   re-decide it:

```abap
INTERFACE zif_abapgit_ortec_ser_exist_h.
  METHODS supports    IMPORTING iv_obj_type      TYPE trobjtype
                       RETURNING VALUE(rv_yes)    TYPE abap_bool.
  METHODS classify     IMPORTING it_tadir         TYPE zif_abapgit_definitions=>ty_tadir_tt
                       RETURNING VALUE(rt_result) TYPE zaog_ser_exist_result_tt
                       RAISING   zcx_abapgit_exception.
ENDINTERFACE.
```

   `ZAOG_SER_EXIST_RESULT` (new structure, used ONLY if/when this
   mechanism is actually introduced): `OBJ_TYPE`, `OBJ_NAME`, `STATUS`
   (domain values `E`=EXISTS, `N`=NOT_EXISTS, `U`=UNKNOWN). A registry
   class (`ZCL_ABAPGIT_ORTEC_SER_EX_REG`, 28 chars, verified <=30) would hold
   `TYPES: tt_handler TYPE STANDARD TABLE OF REF TO
   zif_abapgit_ortec_ser_exist_h`, populated in `class_constructor`, and the
   EXISTING `zcl_abapgit_ortec_bulk_exists=>filter_existing` would keep its
   exact current signature as a compatibility facade calling the registry
   internally — this is a **future, not-yet-authorized refactor**, listed
   here only so SER-4 does not need a new design cycle when it triggers.
3. **DDIC needed**: none for the current prototype. `ZAOG_SER_EXIST_RESULT`
   and `ZAOG_SER_EXIST_RESULT_TT` are listed in the creation manifest as
   `CREATE_IN_SLICE=FUTURE (SER-4-triggered)`, not part of SLICE 1/2.

## Correctness tests and SQL-shape tests (still valuable now, independent
of the registration-mechanism question)

```text
T-1  Existing behavior regression: for a representative package containing
     CLAS/INTF/DTEL/DOMA objects, `zcl_abapgit_tadir~read( iv_check_exists
     = abap_true )` returns the identical row set with `mv_bulk_exists_active`
     forced ON vs. forced OFF (proves the bulk path and the standard
     per-object fallback agree).
T-2  A deleted/never-existed CLAS/INTF/DTEL/DOMA name in the input list is
     correctly excluded under both ON and OFF.
T-3  A CHDO-generated/SADL-generated/proxy-generated exclusion case (already
     coded per the ORTEC sub-audit's CLAS/INTF branches) has an explicit
     test — this exclusion logic is exactly the kind of subtle per-type
     rule a future registration-mechanism refactor could accidentally drop;
     pin it now with a test BEFORE any refactor is attempted.
```

## Decision

```text
SER-1_STATUS=NO_NEW_IMPLEMENTATION_AUTHORIZED_FOR_PROTOTYPE
RATIONALE=Existing zcl_abapgit_ortec_bulk_exists already satisfies the
  EXISTS/NOT_EXISTS/UNKNOWN contract's OBSERVABLE effect (UNKNOWN resolves
  to standard-check transparently, before the object list reaches
  serialization) for all four prototype types.
FUTURE_TRIGGER=SER-4 identifies a new type needing bulk existence AND the
  monolithic CASE statement becomes a real maintenance cost — then, and
  only then, implement the registry/interface shape pre-defined above.
NEAR-TERM_ACTION=T-1..T-3 regression/pinning tests (cheap, safe, valuable
  regardless of the registration-mechanism question) — recommended as part
  of SLICE 0 (see implementation-readiness manifest).
```
