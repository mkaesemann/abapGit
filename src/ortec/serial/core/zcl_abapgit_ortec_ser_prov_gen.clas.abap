"! <p class="shorttext synchronized">ORTEC serialization: trivial no-op generic provider</p>
"! Last-resort, always-matching batch-scoped prefetch provider for object
"! types with no dedicated prefetch cache.
"!
"! RESPONSIBILITY: give the orchestrator ONE uniform provider-dispatch
"! shape ("find the first provider whose SUPPORTS check matches, generic
"! always matches last") instead of a special-cased "no provider found"
"! branch. Every method here is an intentional NO-OP - this provider never
"! actually prefetches or caches anything; every object routed through it
"! always takes the standard, unchanged, per-object serialization path.
"! Enabling or removing this provider must never change ANY object's
"! serialized output - it exists purely to make the dispatch loop simpler,
"! never to accelerate anything itself.
"!
"! LIFECYCLE AND OWNERSHIP: stateless. Holds no CLASS-DATA, so there is
"! nothing to prepare, clear, or leak across batches, workers, or runs.
"!
"! NOT YET WIRED TO THE PROVIDER INTERFACE: SER-SLICE-3 introduces
"! ZIF_ABAPGIT_ORTEC_SER_PROV and the facade pattern
"! (serialization_provider_design.md &sect;5); this class exposes the same
"! method shape as plain CLASS-METHODS today (matching this workspace's
"! existing ZCL_ABAPGIT_ORTEC_SER_PREF*-family convention) and will
"! implement that interface only when SER-SLICE-3 creates it - do not add
"! "INTERFACES zif_abapgit_ortec_ser_prov" in SER-SLICE-2.
CLASS zcl_abapgit_ortec_ser_prov_gen DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.

    "! Always returns TRUE - this provider matches every object type as
    "! the last-resort fallback in the provider-dispatch list.
    "! @parameter iv_obj_type | abapGit object type to check
    "! @parameter rv_yes | Always ABAP_TRUE
    CLASS-METHODS supports
      IMPORTING
        !iv_obj_type   TYPE trobjtype
      RETURNING
        VALUE(rv_yes)  TYPE abap_bool.

    "! No-op: this provider never prefetches anything.
    "! @parameter it_tadir | Ignored
    "! @parameter iv_language | Ignored
    CLASS-METHODS prepare
      IMPORTING
        !it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
        !iv_language TYPE spras.

    "! No-op: clears nothing, because nothing is ever cached.
    CLASS-METHODS clear.

    "! No-op: always returns an initial (empty) buffer - there is never
    "! anything to send to a batch worker for this provider.
    "! @parameter it_tadir | Ignored
    "! @parameter rv_buffer | Always initial
    CLASS-METHODS extract_for_batch
      IMPORTING
        !it_tadir         TYPE zif_abapgit_definitions=>ty_tadir_tt
      RETURNING
        VALUE(rv_buffer)  TYPE xstring.

    "! No-op: there is never anything to inject into a worker session.
    "! @parameter iv_buffer | Ignored
    CLASS-METHODS inject_from_buffer
      IMPORTING
        !iv_buffer TYPE xstring.

    "! @parameter rv_version | Fixed contract version 1 for this provider
    CLASS-METHODS get_version
      RETURNING
        VALUE(rv_version) TYPE i.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcl_abapgit_ortec_ser_prov_gen IMPLEMENTATION.

  METHOD supports.
    rv_yes = abap_true.
  ENDMETHOD.

  METHOD prepare.
    " Intentional no-op - see class-level documentation.
  ENDMETHOD.

  METHOD clear.
    " Intentional no-op - see class-level documentation.
  ENDMETHOD.

  METHOD extract_for_batch.
    " Intentional no-op - see class-level documentation.
  ENDMETHOD.

  METHOD inject_from_buffer.
    " Intentional no-op - see class-level documentation.
  ENDMETHOD.

  METHOD get_version.
    rv_version = 1.
  ENDMETHOD.

ENDCLASS.