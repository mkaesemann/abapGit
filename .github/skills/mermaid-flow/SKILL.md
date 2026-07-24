---
name: mermaid-flow
description: Mermaid diagram generation for call flows and target design
---

# Mermaid flow skill

Create or modify a diagram only when the current parent task explicitly sets
`DIAGRAM_WRITE_ALLOWED=yes` and names the exact path in `OUTPUT_ARTIFACTS`.
A diagram linked from active state is eligible context, not an automatic write
target.

Do not rewrite, archive, rename, or reclassify historical diagrams during a
source-discovery, design, review, implementation, regression, or performance
task unless diagram maintenance is itself an explicit deliverable.

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
