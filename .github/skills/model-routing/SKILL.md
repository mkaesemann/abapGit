---
name: model-routing
description: Cost-aware model selection and escalation rules
---

# Model routing skill

Default to the smallest suitable model.

Use small/fast coding model for:
- grep/search tasks,
- markdown updates,
- Mermaid edits,
- mechanical renames,
- ABAP Unit scaffolding,
- simple implementation tasks with an approved plan.

Use medium reasoning/coding model for:
- method-level ABAP design,
- performance hotspot classification,
- regression review,
- protocol implementation details.

Use large reasoning model only for:
- architecture decisions,
- Git protocol correctness design,
- regression archaeology across branches/commits/SAP versions,
- correctness/performance trade-off analysis.

Escalation rules:
- Record the reason in `.memory/state.md`.
- Keep the context small: pass exact file paths/methods, not entire repositories.
- De-escalate after the complex decision is made.
