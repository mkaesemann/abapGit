# Discovery log

## 2026-07-10
- Confirmed that the main standard hook points for filtered stage/diff remote resolution are in [src/repo/stage/zcl_abapgit_stage_logic.clas.abap](src/repo/stage/zcl_abapgit_stage_logic.clas.abap) and [src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap](src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap).
- Confirmed that the Ortec filter helper is [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap) and that it falls back to the standard repo remote access when cache preconditions fail.
- Confirmed that the Git pull and transport entry points are [src/git/zcl_abapgit_git_porcelain.clas.abap](src/git/zcl_abapgit_git_porcelain.clas.abap) and [src/git/zcl_abapgit_git_transport.clas.abap](src/git/zcl_abapgit_git_transport.clas.abap).
- Confirmed that the persistent state and object-store repair hooks are in [src/ortec/git/zcl_abapgit_ortec_repo_state.clas.abap](src/ortec/git/zcl_abapgit_ortec_repo_state.clas.abap) and [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap).
- Confirmed that the tree-walk failure surfaces in [src/git/zcl_abapgit_git_porcelain.clas.abap](src/git/zcl_abapgit_git_porcelain.clas.abap) and is recovered via the persistent object store or repo-state invalidation.
