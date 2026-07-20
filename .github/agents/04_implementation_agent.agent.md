---
name: ortec-abapgit-implementation
description: Approved ABAP implementation phases
mode: agent
model: inherit
user-invocable: false
disable-model-invocation: true
model_policy:
  default_tier: medium-coding
  max_tier: large-reasoning
  escalation_requires_reason: true
  record_model_in_memory: true

skills:
  - memory-protocol
  - model-routing
  - git-protocol
  - git-partial-clone
---

# Implementation agent

Implement only the approved phase from `.memory/decisions/`.

Rules:
- Ortec logic in `zcl_abapgit_ortec_*`.
- Standard classes only for approved hooks.
- Do not bypass Ortec buffers for correctness.
- Do not fix filtered staging by full-repo fetch.
- Do not issue one request per missing object.
- Persist missing-object fetch results before retrying status calculation.
- Add ABAP Unit tests to relevant testclasses includes.

After each phase:
- update `.memory/state.md`,
- update `.memory/handoffs/implementation_<phase>.md`,
- list changed classes/methods,
- create a focused commit if allowed by Michael/workspace policy.

### Multi-slice owner approval

If an owner prompt explicitly approves an ordered implementation sequence and
the design review verdict is APPROVE or APPROVE_WITH_MINOR_REVISIONS:

- execute the approved slices in order without requesting confirmation between
  slices;
- stop only on a listed hard-stop condition;
- run regression after each slice;
- do not merge multiple slices into one large patch;
- do not change the approved architecture during implementation.

### Model routing for protocol-sensitive implementation

Use a medium or large coding/reasoning model for:

- fetch request policy and serialization;
- partial-clone/promisor semantics;
- materialization certificate transitions;
- transaction and attempt isolation;
- delta-resolution redesign;
- DDIC schema changes that affect correctness.

Use MAI-Code-1-Flash only after the method-level design is fixed, for:

- mechanical signatures;
- XML/DDIC serialization;
- test scaffolding;
- call-site updates;
- renames and deletion of obsolete methods;
- documentation and memory updates.