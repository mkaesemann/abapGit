---
name: abap-adt-workflow
description: Reliable ADT/ABAP object read-write-activate workflow and new-object
  checklist
---

# ABAP ADT workflow skill

Editing existing objects:
- `replace_string_in_file`/`multi_replace_string_in_file` work directly on abapfs `adt://`
  workspace URIs for ANY class include (main, definitions, implementations, macros,
  testclasses) — no separate "open object" step is required first. Get the exact URI via the
  abapfs workspace-URI lookup for the object (name + type, e.g. `CLAS/OC`), then edit like a
  normal file. Read first to get exact-match old-string text.
- After such an edit, `get_errors` on the file gives live ADT syntax-check diagnostics (0 errors
  is necessary but NOT sufficient — see the abap-syntax-pitfalls skill for what it still misses).
- If `abap_activate`/activation fails with "User X is currently editing `<class>`" right after an
  edit, this is a real enqueue lock left by the write — retry activation targeting the EXACT
  include URL that was just edited (e.g. the `.clas.testclasses.abap` URI), not the main
  `.clas.abap` URL. The include-specific URL is the actual lock owner and releases cleanly.
- `multi_replace_string_in_file` can fail generically ("input to the tool was invalid") on
  large/complex replacement arrays (~5-8+ sizable ABAP blocks) even with well-formed JSON — fall
  back to sequential single `replace_string_in_file` calls instead of retrying the same payload.
- When two replacements in the same multi-replace call share a boundary line (e.g. one `oldString`
  ends where the next begins, such as an `ENDCLASS.` / next-class-start anchor), the shared line
  can be silently dropped from the result. Re-read the edited region immediately after any edit
  that touches a method/class boundary to catch silent corruption before moving on.

New global object checklist:
- A new CLAS needs BOTH the `.clas.abap` source file AND a matching `.clas.xml` metadata file, or
  abapGit import silently fails to create the object on the target system with no visible error
  (syntax checks against the not-yet-existing name then trivially "pass" since there's nothing to
  check). Copy the `.clas.xml` structure from an existing sibling class in the same package rather
  than guessing the schema.
- Check the 30-character limit on both the object name and every method name before creating it
  (see abap-syntax-pitfalls skill for the scan command and consequences).
- Run "ABAP Cleaner: Format Document" on a class file before saving/committing changes to it.
- Before considering a "create new class" task done, verify EXISTENCE on the real system (not just
  a local syntax-check pass) — see the existence-check note in abap-syntax-pitfalls.

Verifying correctness on a real system:
- Prefer a live syntax dry-run over waiting for the user to test and report back, when live SAP
  diagnostic access is available — it is faster and catches real compiler-only errors described in
  the abap-syntax-pitfalls skill.
- Reading ST22 dumps directly (when available) is more reliable than asking the user to paste dump
  text.
- See abap-syntax-pitfalls for the specific false-positive/false-clean traps in the syntax-check
  and dry-run tooling itself (nonexistent-object false pass, isolated single-class dry-run
  cross-reference false fail).
