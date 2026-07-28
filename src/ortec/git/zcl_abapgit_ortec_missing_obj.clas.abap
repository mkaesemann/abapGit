"! <p class="shorttext synchronized">ORTEC Bulk Missing-Object Collector</p>
"! Ensures a set of blob objects is present in the persistent object store,
"! performing at most one bounded, adaptively-batched negotiated remote
"! fetch (via zcl_abapgit_ortec_cold_init=>materialize_missing_batches) if
"! some are missing, then retrying the local check once. Never performs a
"! network call for a repository that has not opted into the ORTEC
"! write/protocol behavior, so a read-only caller (e.g. filtered Stage/Diff
"! resolution) never pays a surprise full-fetch cost.
"! In terms of zcl_abapgit_ortec_obj_store=>cs_object_state, this class moves
"! objects from NOT_BUFFERED to LOADED via one bounded fetch + persist. It never
"! resolves CONFIRMED_ABSENT itself (that requires positively-resolved parent
"! tree/path context this class does not have) - if an object is still missing
"! after the retry, that is reported as a failure (effectively
"! CORRUPT_OR_INCOMPLETE/unresolved), never as a deleted-file signal.
"! Variant B D2 TIME_OUT incident fix
"! (.memory/incidents/variant_b_d2_it8_system_no_roll_timeout.md,
"! .memory/logs/variant_b_d2_timeout_fix_design.md): the remote fetch is
"! scoped to exactly IT_SHA1S (the caller's own missing set), never to the
"! whole reachable graph of a commit. A live incident measured
"! ensure_available's PRIOR implementation (a commit-scoped
"! upload_pack_by_commit(deepen=1) fetch) returning 162,919 objects for a
"! caller that only needed a small, filtered blob subset - causing a
"! TIME_OUT during pack decode. IT_SHA1S is therefore narrowed, and should
"! be understood, as blob SHA1s specifically (both real callers,
"! zcl_abapgit_ortec_obj_index=>build_files_from_rows and
"! zcl_abapgit_ortec_walk_prep=>topup_missing_blobs, already only ever pass
"! blob SHA1s - confirmed by source read).
CLASS zcl_abapgit_ortec_missing_obj DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    "! Ensure the given blob objects are present in the persistent object
    "! store.
    "! Flow: local bulk check -> (if any missing) one bounded, adaptively-
    "! batched negotiated remote fetch of exactly it_sha1s -> local bulk
    "! check retried once.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_url |
    "! Remote URL. Pass initial to skip fetching entirely (local check only).
    "! @parameter iv_commit |
    "! Retained for interface stability and diagnostics only - no longer
    "! used to scope the remote fetch (it_sha1s is placed on the wire
    "! directly; see the class doc's TIME_OUT-fix note).
    "! @parameter it_sha1s |
    "! Blob SHA1s the caller needs to be present in the store - these
    "! exact values are what is requested from the server, nothing more
    "! @raising zcx_abapgit_ortec_git |
    "! Raised if objects are still missing after the fetch+retry, if no remote
    "! fetch is possible (blank URL or write/protocol opt-in inactive), or if the
    "! negotiated fetch itself fails (including a missing arbitrary-object-want
    "! server capability, distinguishable via mv_unsupported_capability - the
    "! underlying exception is propagated unchanged, never re-wrapped).
    "! Callers should treat this exactly like "no fast-path benefit
    "! available" and fall back to their existing safe path.
    CLASS-METHODS ensure_available
      IMPORTING
        iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_url      TYPE string
        iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
        it_sha1s    TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING
        zcx_abapgit_ortec_git.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.


CLASS zcl_abapgit_ortec_missing_obj IMPLEMENTATION.

  METHOD ensure_available.

    DATA lt_missing TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    " Step 1: local bulk resolve - one set-based DB lookup, no per-object reads.
    lt_missing = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
      iv_repo_key = iv_repo_key
      it_sha1s    = it_sha1s ).

    IF lt_missing IS INITIAL.
      RETURN. " Already fully buffered - nothing to do.
    ENDIF.

    " Never trigger a network fetch for a repo that has not opted into the ORTEC
    " write/protocol behavior. Without that opt-in, the transport layer has no
    " incremental have/want negotiation available and a fetch here could turn a
    " read-only Stage/Diff resolution into a full, non-negotiated pull.
    IF iv_url IS INITIAL OR zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_false.
      zcx_abapgit_ortec_git=>raise(
        |{ lines( lt_missing ) } object(s) not buffered and remote fetch is not available| ).
    ENDIF.

    " Step 2: one bounded, adaptively row/byte-batched MATERIALIZE_BLOBS
    " fetch+persist of exactly lt_missing (never iv_commit's whole reachable
    " graph - see class doc). Persistence happens as a side effect inside
    " materialize_missing_batches (via decode_streaming, the same primitive
    " every other fetch mode already uses) - no separate store_objects call
    " is needed or made here, matching materialize_tip_snapshot's own
    " established shape.
    " The exception is deliberately propagated UNCHANGED (not re-wrapped
    " with a new message) so mv_unsupported_capability - and any other
    " structured detail zcx_abapgit_ortec_git carries - survives intact to
    " this method's own caller.
    zcl_abapgit_ortec_cold_init=>materialize_missing_batches(
      iv_url      = iv_url
      iv_repo_key = iv_repo_key
      it_sha1s    = lt_missing ).

    " Step 3: retry once - re-check what is still missing after the fetch.
    lt_missing = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
      iv_repo_key = iv_repo_key
      it_sha1s    = it_sha1s ).

    IF lt_missing IS NOT INITIAL.
      zcx_abapgit_ortec_git=>raise(
        |{ lines( lt_missing ) } object(s) still missing after negotiated fetch| ).
    ENDIF.

  ENDMETHOD.

ENDCLASS.
