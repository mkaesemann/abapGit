---
name: mermaid-flow
description: Mermaid diagram generation for call flows and target design
---

# Mermaid flow skill

Create and maintain only diagrams linked from the active topic in
`.memory/state.md`.

For `variant-b-partial-clone`, the target diagram is:

- `.memory/diagrams/variant_b_flow.mmd`

Rules:

- use `flowchart TD` unless another diagram type is clearly superior;
- include exact class and method names only when verified;
- distinguish standard abapGit from ORTEC nodes;
- show fetch mode, filter, bulk collection, persistence, publication, recovery,
  and performance batch boundaries;
- do not recreate historical diagrams unless the active design explicitly needs
  them;
- diagrams must match the active design and current source; never invent flows.
