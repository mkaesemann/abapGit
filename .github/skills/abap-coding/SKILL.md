---
name: abap-coding
description: ABAP implementation and unit-test conventions
---

# ABAP coding skill

See also: `abap-syntax-pitfalls` (real compiler errors invisible to local tooling),
`abap-adt-workflow` (reliable read/write/activate workflow, new-object checklist),
`abap-performance-patterns` (bulk DB access, hashed-table/LRU patterns),
`git-workflow-safety` (memory-file and stash/checkout safety).

Rules:
- Prefer `zcl_abapgit_ortec_*` classes for Ortec behavior.
- Only touch standard `zcl_abapgit_*` classes for minimal hook/delegation points.
- Do not create standalone global test classes such as `zcl_abapgit_ortec_git_tests`.
- Put ABAP Unit tests in the testclasses include of the relevant production class.
- Avoid `SELECT SINGLE` in hot loops. Use bulk selects and internal-table lookup.
- Avoid repeated full pack scans. Use indexed object/pack lookup APIs.
- Respect ABAP/DDIC 30-character object-name limits.
- If syntax/activation cannot be checked locally, use MCP/SAP IT8 or ask Michael to pull and activate.

Before changing productive ABAP:
- record approved plan in `.memory/state.md`,
- list exact classes/methods to touch,
- state whether any standard abapGit hook is required.

### Test placement

New Variant B tests must be placed in the testclasses include belonging to the
affected productive class.

Do not add further tests to
`zcl_abapgit_ortec_git_tests.clas.testclasses.abap`.

Existing tests in that legacy aggregate include may remain until a separate,
non-functional cleanup is approved.
