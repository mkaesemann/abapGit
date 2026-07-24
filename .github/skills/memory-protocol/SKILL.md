---
name: memory-protocol
description: Persistent memory and resume discipline
---

# Memory protocol skill

Always:
- The orchestrator reads `.memory/state.md` first.
- Subagents read `.memory/state.md` only when it is listed in `ALLOWED_CONTEXT`.
- Subagents write only their exact parent-specified artifact and return a compact result packet.
- The orchestrator alone owns `.memory/state.md`.
- Update `.memory/state.md` only for a durable phase/checkpoint transition,
  owner decision, validated blocker, implementation commit, or validation
  result that changes the next action.
- Do not update active state for routine searches, successful subagent work,
  intermediate hypotheses, or documentation cleanup.
- Put detailed evidence in one focused log/review/handoff and link it from state
  only when it becomes active resume information.
- If interrupted, the latest focused handoff plus compact state must allow
  resume without broad rediscovery.

State entries must include:
- timestamp,
- phase/agent,
- model tier used,
- files/classes/methods inspected,
- exact finding,
- evidence source,
- next action.

### Evidence status and supersession

Every load-bearing memory entry must have one status:

- CONFIRMED_CURRENT
- OWNER_DECISION
- ASSUMPTION
- UNVERIFIED
- SUPERSEDED
- CONTRADICTED

When a later source inspection, live test, or owner decision invalidates an
earlier entry:

- do not leave both entries appearing equally current;
- mark the earlier entry SUPERSEDED or CONTRADICTED;
- link to the replacing entry;
- update `.memory/state.md` so only the current conclusion appears in the active
  summary;
- retain historical detail only in logs.

Current productive source and reproducible live evidence outrank historical
agent conclusions.

An explicit current owner decision outranks an earlier design recommendation,
unless the current source or required platform capability makes it impossible.

### Topic isolation

Each major topic must have:

- topic ID;
- current phase;
- owner-approved goal;
- active decisions;
- superseded decisions;
- blockers;
- next action.

Do not let an unrelated resumable backlog topic control a newly named owner
topic.

### Parent-scope precedence

Exact `ALLOWED_CONTEXT`, `OUTPUT_ARTIFACTS`, `FORBIDDEN_PATHS`, and write
permissions supplied by the parent override generic skill or agent defaults.
A skill must never broaden a focused task into archive reading, state updates,
or diagram maintenance.