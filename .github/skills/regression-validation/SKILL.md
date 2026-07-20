---
name: regression-validation
description: Regression and acceptance validation
---

# Regression validation skill

Minimum scenarios:
- Full repository staging.
- Stage by transport with filtered file list.
- Diff page for modified file.
- Diff page for unchanged file.
- Locally added/deleted file.
- Remotely deleted file.
- File present in remote but not initially buffered locally.
- Branch switch with partly buffered objects.
- Branch switch with mostly missing objects.
- Repository with >40000 files/objects.
- Thin packs and deltas.
- Multiple branches sharing blobs/trees.
- Interrupted communication followed by retry/recovery using `zaog_*` state.
- Fastpath disabled -> standard abapGit behavior unchanged.

Hard failures:
- Unknown/not-buffered shown as remote deleted.
- Full repo fetch as default filtered-staging fix.
- One remote request per object.
- `SELECT SINGLE` loops in tree/status hot paths.
- Standard abapGit path used to bypass Ortec buffers for correctness.
