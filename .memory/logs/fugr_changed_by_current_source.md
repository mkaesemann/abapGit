# SER-FINAL-CONTINUOUS — FUGR CHANGED_BY current-source mapping (2026-08-10)

## Exact method read: `ZCL_ABAPGIT_OBJECT_FUGR~ZIF_ABAPGIT_OBJECT~CHANGED_BY`

```
lv_program = main_name( ).                                   " SAPL<area> (namespace-aware)
IF mt_includes_all IS INITIAL.
  CALL FUNCTION 'RS_GET_ALL_INCLUDES' ... TABLES includetab = mt_includes_all.   " REQUIRED_ONCE_PER_FUGR
ENDIF.
LOOP AT mt_includes_all WHERE table_line = to_upper( iv_extra ).                 " O(includes), cheap, in-memory
  lv_program = <include>. lv_found = abap_true. EXIT.
ENDLOOP.
" (fixed this pass) was: unconditional lt_functions = functions( ).             " DUPLICATED_WITHIN_REQUEST / NEGLIGIBLE-CALLER, but triggers real DB work
IF iv_extra IS NOT INITIAL.
  lt_functions = functions( ).                                                  " RS_FUNCTION_POOL_CONTENTS + ENLFDIR (provider-aware)
  LOOP AT lt_functions WHERE funcname = to_upper( iv_extra ). ... ENDLOOP.
ENDIF.
SELECT ... FROM reposrc WHERE progname = lv_program AND r3state = 'A' ...        " REQUIRED_ONCE_PER_FUGR (single row)
IF mt_includes_all IS NOT INITIAL AND lv_found = abap_false.
  SELECT ... FROM reposrc FOR ALL ENTRIES IN mt_includes_all ...                 " already bulk (1 SQL stmt for all includes) - BULKABLE_BY_EXACT_KEYS, already done
ENDIF.
SELECT ... FROM repotext WHERE progname = lv_program ...                        " REQUIRED_ONCE_PER_FUGR (single row)
SELECT ... FROM eudb WHERE relid='CU' AND name=lv_program ...                    " REQUIRED_ONCE_PER_FUGR (single row)
SORT lt_stamps BY date DESCENDING time DESCENDING.                               " take first row
```

## Classification per mandated categories

| Operation | Classification |
|---|---|
| `RS_GET_ALL_INCLUDES` | REQUIRED_ONCE_PER_FUGR — no bulk/multi-program FM variant exists; reimplementing its TRDIR/D010INC resolution directly would be the same class of release-dependent-semantics risk the DDLS gate rejects. Already cached per-instance (`mt_includes_all`), so genuinely called exactly once per top-level `CHANGED_BY` invocation. |
| `RS_FUNCTION_POOL_CONTENTS` (via `functions()`) | Was **DUPLICATED_WITHIN_REQUEST in the sense of always-executed-but-provably-unused**: `functions()`'s only consumer inside `CHANGED_BY` is the `funcname = to_upper( iv_extra )` match loop, which can **never** match when `iv_extra` is initial (ABAP function names are never empty strings) or when `iv_extra` already matched an include name. The unconditional call was pure waste for the dominant "whole-object" `iv_extra`-empty invocation shape (confirmed real caller: `zcl_abapgit_repo_content_list=>build`, which calls `zcl_abapgit_objects=>changed_by( ls_item )` with no filename → empty `iv_extra`, once per changed object). **FIXED this pass** — see design log. |
| `REPOSRC` (main program) | REQUIRED_ONCE_PER_FUGR — single-row lookup, not a loop. |
| `REPOSRC` (all includes) | Already BULKABLE_BY_EXACT_KEYS and already implemented as one `FOR ALL ENTRIES` statement — no further win. |
| `REPOTEXT` | REQUIRED_ONCE_PER_FUGR — single-row lookup. |
| `EUDB` | REQUIRED_ONCE_PER_FUGR — single-row lookup. |
| `D010INC`/`RSEUINC` | SAP_INTERNAL_NOT_SAFELY_REPLACEABLE — internal to `RS_GET_ALL_INCLUDES`'s own implementation, not directly called by abapGit code; reconstructing them directly would require reimplementing a release-dependent standard FM. |
| `ENLFDIR` (via `functions()`) | Provider-aware already (`get_fugr_enlfdir`); only reached at all when `iv_extra IS NOT INITIAL` after this pass's fix, i.e. materially fewer times than before. |

## No cross-request state, no bulk-across-object mechanism proposed

`ZCL_ABAPGIT_OBJECT_FUGR` is created fresh per top-level call (`zcl_abapgit_objects=>create_object` has no instance cache — confirmed via source read of `gt_obj_serializer_map`, which only caches the resolved *class name*, never an instance). This means `mt_includes_cache` (used by `includes()`) and `mt_includes_all` (used by `changed_by()`) never share a live instance across the two entry points in practice — unifying them would add complexity for zero real-world benefit. Not attempted.

## Confirmed real caller (dominant cost driver)

`src/repo/zcl_abapgit_repo_content_list.clas.abap` (standard abapGit, `AT ... changes > 0` block): calls
`zcl_abapgit_objects=>changed_by( ls_item )` — **no filename** — once per object with local changes, for
the repository content/overview list. This is the call shape that matches last pass's trace evidence
(147 `CHANGED_BY` hits, always the "whole object" case). A second caller,
`zcl_abapgit_gui_page_diff_base` (diff view), passes `iv_filename = is_status-filename` — a per-file,
`iv_extra`-populated call, verified unaffected by this pass's fix (guarded by `IF iv_extra IS NOT INITIAL`).
