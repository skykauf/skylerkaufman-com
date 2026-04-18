---
name: git-delivery
description: >-
  Defines skylerkaufman.com Git workflow—branch from main, push, fast-forward
  merge, substantive-change commits, and conflict handling. Use when making
  code changes, commits, pushes, merges, PRs, or delivery in this repository.
---

# Git / delivery (skylerkaufman.com)

## Rules

- For any code change, create an appropriately named branch from `main` before editing (for example: `feat/chat-timeout`, `fix/fivb-profile-null`, `chore/deps-update`).
- Keep branch names concise and descriptive, using `feat/`, `fix/`, `chore/`, `docs/`, or `refactor/` prefixes where they fit.
- This branch → push branch → fast-forward merge to `main` → push `main` sequence is the default delivery pattern unless the user explicitly requests a different flow.
- Before opening a PR or merging, sync with `origin/main` and resolve conflicts on the branch.
- After **substantive** changes (features, bug fixes, config that affects deploy), commit and push the branch, then automatically merge it into `main` when checks pass and there are no conflicts.
- Do not commit directly to `main` unless the user explicitly requests an exception.
- Use clear commit messages; one logical change per commit when practical.

## Example flow

```bash
# start from an up-to-date main branch
git checkout main
git pull origin main

# create a descriptive working branch
git checkout -b feat/short-description

# after making changes
git add -A
git commit -m "Add short description of change"
git push -u origin HEAD

# merge back into main after checks pass
git checkout main
git pull origin main
git merge --ff-only feat/short-description
git push origin main
```
