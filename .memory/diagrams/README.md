# Diagrams — active classification

Current productive source and reproducible SAP evidence outrank every diagram.
Diagrams are navigation aids, never implementation authority.

## Active diagrams for Package D

### `variant_b_flow.mmd`

Status: `CONFIRMED_CURRENT_BASELINE`

Purpose: compact end-to-end Variant B flow after Package C, including the
validated `HIST_LEVEL = F` plus `SNAP_STATE = C` publication invariant and the
explicit D1/D2 boundaries.

### `variant_b_package_d_target.mmd`

Status: `OWNER_TARGET_D0`

Purpose: desired Package D design intent for bounded external delta-base
resolution and attempt/session/transaction isolation. Exact methods, files,
DDIC changes, and transaction semantics still require D0 current-source
reconciliation and review.

## Historical diagrams

The following names are historical evidence only and must not be used to choose
or redefine the active Package D topic:

- `h4_target_architecture_legacy.mmd`
- `current_slow_path.mmd`
- `historical_fast_path.mmd`
- `fastpath_active_call_trace.mmd`

Status: `HISTORICAL_OR_SUPERSEDED`

These diagrams may describe the earlier H4/read-path architecture or an older
implementation state. Read them only when a current Package D finding links to
a specific historical claim.

## Rules

- Do not rewrite or archive diagrams during D0 unless the task explicitly asks
  for diagram maintenance.
- Do not infer that a class is obsolete merely because it appears in a
  historical diagram; Package E owns validated legacy classification/removal.
- Do not use diagrams to override `.memory/state.md`, the current owner prompt,
  current productive source, or SAP validation evidence.
- Package D D0 produces discovery, design, and review artifacts; diagram work is
  optional and must not replace those outputs.
