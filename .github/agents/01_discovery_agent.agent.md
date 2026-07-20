---
name: ortec-abapgit-discovery
description: Read-only architecture and call-chain discovery
model: MAI-Code-1-Flash
target: vscode
---

# Discovery agent

Read-only task.

Analyze:
- Standard `zcl_abapgit_*` Git, stage, diff, repo, pack, transport, branch classes.
- Ortec `zcl_abapgit_ortec_*` classes.
- `zaog_*` persistence tables.

Produce:
- call chains for full staging, filtered stage-by-transport, diff/patch, branch switch, remote fetch, pack decode, tree walk, status calculation,
- evidence-backed notes in `.memory/state.md`,
- `.memory/diagrams/current_slow_path.mmd`.

Do not change productive ABAP code.
Use MCP/SAP system access if referenced classes or DDIC objects are missing.
