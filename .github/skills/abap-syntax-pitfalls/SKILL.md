---
name: abap-syntax-pitfalls
description: Real ABAP compiler errors that local get_errors/abaplint tooling misses
---

# ABAP syntax pitfalls skill

Local `get_errors`/abaplint in this workspace can report zero errors on code that a real SAP
system rejects at generation/activation time, or that compiles but silently produces the wrong
value. Never treat a clean local check as proof of correctness for the patterns below. Whenever
touching one of these constructs and a live system is reachable, verify with a real syntax
dry-run (e.g. `SAPDiagnose(action="syntax", type="CLAS", name=X, source=<full source>)`) before
considering the change done.

Real compile-time errors (local tooling shows 0 errors, real system rejects):
- `FOR ALL ENTRIES`: the `INTO TABLE @DATA(...)` clause must be the LAST clause, after
  `FOR ALL ENTRIES ... WHERE ...` — not right after `FROM <table>`.
- Offset/length notation on STRING/XSTRING (`iv_data(len)`, `iv_data+off(len)`) can only be the
  source of a plain assignment (`lv_x = iv_data(len).`). It cannot be passed inline as a method's
  actual parameter (`foo( iv_data(len) )`).
- `INSERT/MODIFY/UPDATE <dbtab> FROM VALUE #( ... )` is invalid Open SQL — an inline constructor
  expression as the source needs the host-expression operator: `FROM @( VALUE #( ... ) )`.
- Method parameter direction keywords are always from the CALLEE's perspective. To supply a value
  to a callee's `EXPORTING` parameter, receive it via the caller's `IMPORTING` clause — passing it
  under the caller's `EXPORTING` clause compiles locally but fails on the real system.
- Functional-style call syntax (`foo( iv_x = 1 )`) with an implicit/omitted `EXPORTING` keyword
  only works when EXPORTING-style parameters are the ONLY clause. As soon as `CHANGING` (or
  `RECEIVING`) is also present, `EXPORTING` must be written explicitly:
  `foo( EXPORTING iv_x = 1 CHANGING cv_y = lv_y ).`
- The `DEFAULT` addition on a formal parameter cannot be combined with an inline
  `TYPE c LENGTH n` (or any inline LENGTH/DECIMALS addition) — reference a named type/data
  element instead: `IMPORTING iv_x TYPE <named_type> DEFAULT 'R'`.
- Global ABAP object names (CLAS/INTF/etc.) AND method names both have a hard 30-character limit.
  Violations are not always rejected upfront — they can silently truncate on import/activation
  and break every reference to the intended name. Before creating/renaming, check length
  explicitly (PowerShell: `"NAME".Length`, or scan a whole file at once:
  `Select-String -Path <file> -Pattern '^\s*METHODS?\s+(\w+)' | %{ $_.Matches.Groups[1].Value } | Sort -Unique | ?{ $_.Length -gt 30 }`).
  Because ABAP compiles the whole class pool together, one over-length name anywhere blocks
  syntax check/unit tests for the ENTIRE class — scan the whole file in one pass, don't
  fix-and-recheck one violation at a time.

Real value-correctness bugs (compiles clean, produces wrong data):
- Hex text from `zcl_abapgit_hash=>sha1_raw()`/`sha1_blob()` is LOWERCASE. Assigning it directly
  to a `TYPE x` field does NOT correctly hex-decode it — always wrap with `to_upper(...)` first.
- `|{ lv_number WIDTH = 8 PAD = '0' }|` pads on the RIGHT with trailing zeros (effectively
  left-aligned), NOT leading zeros — `1`, `10`, `100`, `1000`, `10000` can all produce the
  identical string. Always add `ALIGN = RIGHT` explicitly for zero-padded numeric IDs:
  `|{ lv_number WIDTH = 8 ALIGN = RIGHT PAD = '0' }|`. Do not use `UNPACK` as a substitute (that's
  for BCD/packed-decimal source fields, not generic numeric-to-char padding).
  An equally valid alternative: assign to a NUMC field of the required length.
  Any lookup keyed by a value whose "natural empty state" is itself legitimate/meaningful
  (e.g. a 0-byte blob) must NOT use `IS NOT INITIAL` as an existence check — use a dedicated
  `has()`/exists lookup backed by the storage mechanism, not the retrieved value's content.
- Assigning a large `repeat( val = 'A' occ = N )` STRING result directly (`=`) to an XSTRING
  variable can lose byte-exactness for large generated test fixtures. Use
  `zcl_abapgit_convert=>string_to_xstring_utf8( iv_string = repeat(...) )` instead of a bare `=`.
- A single-column `SELECT col FROM dtab INTO TABLE @DATA(lt_x)` infers `lt_x` as a ONE-FIELD
  STRUCTURE table (component named after the column), not a plain elementary table — even though
  the scalar form `SELECT col ... INTO @DATA(lv_x)` infers elementary correctly. Declare the
  target type explicitly (reuse an existing elementary table type) whenever the result later
  feeds an elementary-row table/field-symbol.

Verification discipline:
- `SAPDiagnose(action="syntax", name=X)` (no `source=`, checking the ACTIVE version) returns a
  meaningless clean `{hasErrors: false}` for an object that DOES NOT EXIST on the target system at
  all. Confirm the object actually exists (e.g. a read/lookup call that 404s otherwise) before
  trusting any "pass" as meaningful — this applies doubly to a subagent's self-reported "verified
  live" claims.
- A `source=` dry-run that checks ONE class in isolation can spuriously fail
  (`"unknown or PROTECTED or PRIVATE"`, or `"REPORT/PROGRAM statement is missing"`) on a genuinely
  valid cross-class call if the OTHER class's new/changed method isn't also active/importable yet,
  or on some larger objects for no clear reason. Before treating either as a real defect: re-check
  the sibling class's actual declaration, and re-run with `version="active"` (no `source=`) against
  the currently active object to rule out a dry-run isolation artifact.
- A single git commit that imports cleanly into abapGit can still land PARTIALLY applied per-file
  on the live system (one hunk of a file activates, another reverts; one file in a multi-file
  commit is stale while a sibling is current). Never assume "the commit was imported" means every
  line is live — after a live error, re-read the actual active source of every file touched by the
  suspect commit before proposing a fix.
- After ANY externally-supplied "syntax fix" (from a person, formatter, or subagent), diff the
  WHOLE changed file against the last known-good version, not just re-check that it compiles — a
  narrow syntax fix can silently revert an adjacent, unrelated line of logic.
