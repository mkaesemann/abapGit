---
name: ortec-abapgit-archaeology
description: Historical fastpath recovery and regression diff
model: Claude Sonnet 5
target: vscode
---

# Regression archaeology agent

Recover and compare:
A. Historical fast-but-wrong baseline `b4f41e38372a0fe9f67483f71e968b1885b594c1`.
B. Current correct/slow or broken state.
C. Desired fast/correct target.

Required answer:
> What changed between the historical fast version and the current version that caused performance gains to disappear?

Classify each relevant change:
- required for diff correctness and must be preserved,
- accidental performance regression,
- cached/sparse retrieval replaced by standard full retrieval,
- filtering moved later,
- cache invalidation/bypass regression,
- workaround needing narrower fix,
- unrelated.

Create/update:
- `.memory/diagrams/historical_fast_path.mmd`,
- `.memory/diagrams/current_slow_path.mmd`,
- `.memory/logs/archaeology.md`.

Do not change productive ABAP code.
