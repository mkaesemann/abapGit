# Variant B package renumbering

Date: 2026-07-24
Status: OWNER_DECISION

## Decision

A new Package E is inserted after Package D.

- Package D remains unchanged and is completed under its current design,
  review and performance gates.
- New Package E is named:
  **Snapshot Consumer Coherence and Adaptive Materialization**.
- The former Package E, validated legacy cleanup, is renamed to Package F.
- Historical Package D discovery, design and review artifacts are not
  rewritten.
- Historical statements saying that Package E owns legacy cleanup are
  superseded by this decision and mean Package F from this point onward.

## Required order

1. Package D – delta resolution and attempt/transaction isolation
2. Package E – snapshot consumer coherence and adaptive materialization
3. Package F – validated legacy cleanup

## Package E scope

Package E owns:

- one upload-pack capability discovery per complete snapshot materialization;
- adaptive MATERIALIZE_BLOBS want-list sizes;
- metadata-only final selected-tip blob verification;
- consumer repo-key and tip coherence after branch switching;
- prevention of normal MISSING_OBJ=>ENSURE_AVAILABLE execution for a certified,
  unchanged snapshot;
- classification and one bounded repair of CERTIFIED_BUT_MISSING;
- regression coverage for cold branch switch followed immediately by Stage,
  Stage-by-Transport, Diff and status calculation.

## Package F scope

Package F owns:

- physical removal of legacy fetch and decoder paths;
- removal of unreachable progressive-deepen and force-full scaffolding;
- removal of VERIFY_BATCH_OBJECTS after replacement tests are established;
- removal or final restriction of MISSING_OBJ=>ENSURE_AVAILABLE after all
  productive callers are classified and Package E is live-validated.