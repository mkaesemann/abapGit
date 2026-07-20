# Persistent memory protocol

The memory tree is an indexed project record, not a conversation transcript.

## Default read set

Every agent reads only:

1. `.memory/state.md`;
2. the current owner specification linked there;
3. latest decision/review/handoff files linked for the active slice;
4. exact productive source named by the task.

Do not load all logs or `.memory/archive` by default.

## Directory responsibilities

- `state.md` - concise active topic, current phase, gates, links, and next action.
- `decisions/` - owner-approved or pending decisions.
- `reviews/` - correctness and performance gate verdicts.
- `handoffs/` - one focused implementation/regression handoff per slice.
- `logs/` - detailed evidence, investigations, traces, and historical analysis.
- `diagrams/` - only diagrams linked by the active design.
- `archive/` - superseded large state snapshots; read only when specifically needed.

## Update rules

- Keep `state.md` below roughly 6 KB whenever possible.
- Never append long analysis to `state.md`; write a focused log and link it.
- Replace active summaries instead of accumulating chronological transcripts.
- Mark claims as `CONFIRMED_CURRENT`, `OWNER_DECISION`, `ASSUMPTION`,
  `UNVERIFIED`, `SUPERSEDED`, or `CONTRADICTED`.
- When superseding a claim, update the active summary and link to the replacement.
- Record exact classes, methods, tables, evidence, model, and validation status in
  the focused artifact.
- Chat responses remain concise; persistent artifacts contain the details.
