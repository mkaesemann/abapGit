---
name: memory-protocol
description: Persistent memory and resume discipline
---

# Memory protocol skill

Always:
1. Read `.memory/state.md` first.
2. Update `.memory/state.md` after every meaningful finding, decision, code change, or test result.
3. Put long phase details under `.memory/logs/` and link them from state.
4. Put handoff summaries under `.memory/handoffs/`.
5. Put reviewed or pending decisions under `.memory/decisions/`.
6. If interrupted, the next agent must be able to resume without redoing discovery.

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
