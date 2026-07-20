---
name: git-workflow-safety
description: Safe git operations for this repo (memory-file protection, stash/checkout
  diffing)
---

# Git workflow safety skill

`.memory/` protection:
- `.memory/` in this workspace is intentionally UNTRACKED by git — there is no git history to
  recover from if a file in it gets overwritten or accidentally committed.
- `git add`/`git commit` with an explicit file list WILL happily track a `.memory/*` path (there is
  no `.gitignore` rule preventing it — only habit keeps it untracked). Always re-check the exact
  file list in any `git add`/`git commit` command for accidental `.memory/*` entries before
  running it, especially when staging several files in a hurry after a big edit. If one slips in
  before pushing, fix with `git rm --cached <path>` + `git commit --amend --no-edit`.
- When delegating diagram/log file writes to a subagent, verify afterward that it wrote to the NEW
  filename requested rather than silently overwriting an existing, differently-purposed file. Read
  the subagent's produced files back immediately after it reports done.

Before/after diffing:
- `git checkout <commit> -- <file>` / `git checkout HEAD -- <file>` (checkout-and-restore diffing)
  is ONLY safe for files with NO uncommitted changes. Running `git checkout HEAD -- <file>` to
  "restore" a file that has uncommitted working-tree changes silently DISCARDS that work (HEAD
  doesn't have it either, and there's no recovery). Always check `git status`/`git diff` first to
  confirm whether the target file's current content is committed.
- For files with uncommitted changes, use `git stash push -- <file>` / `git stash pop` instead —
  but check `git stash list` FIRST, both before pushing (to know what you're about to add) and
  before popping (git does not drop a stash on a conflicted pop, so a bad pop is usually still
  recoverable via `git checkout HEAD -- <conflicted files>` without touching the intact stash
  entry). Never assume the top of the stash stack is the one you just pushed.
- `git stash push -- <files>` is a no-op ("No local changes to save") if those files have NO
  uncommitted changes (e.g. the fix was already committed) — a subsequent `git stash pop` will
  then pop whatever unrelated older stash happens to be on top instead, which can produce real
  conflicts in unrelated files. If the file is already committed, diff via
  `git checkout <parent-commit> -- <file>` then `git checkout HEAD -- <file>` instead — no stash
  stack risk.

Trusting "it was imported":
- A single git commit that imports cleanly into abapGit can still land PARTIALLY applied per-file
  on the live system (one hunk of a two-hunk diff activates, the other reverts; one file in a
  multi-file commit is stale while a sibling is current). Never assume "the user imported commit
  X" means every line of X is live — after any live error, re-read the actual active source of
  every file touched by the suspect commit, not just the one throwing the error.
