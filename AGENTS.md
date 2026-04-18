# Agent instructions (skylerkaufman.com)

Canonical skill files live under [`skills/`](skills/) and are symlinked into [`.cursor/skills/`](.cursor/skills/) and [`.claude/skills/`](.claude/skills/) so Cursor and Claude Code share one copy. See [`skills/README.md`](skills/README.md).

## Git / delivery

[`skills/git-delivery/SKILL.md`](skills/git-delivery/SKILL.md)

## Verify before you call it done (npm verify, HF router, local curl, Vercel)

[`skills/repo-verify/SKILL.md`](skills/repo-verify/SKILL.md)

For **manual FIVB pipeline cron** smoke tests in production, prefer the user’s **Cursor `/vercel`** integration or the **Vercel dashboard Cron Jobs → Run** — not pasting **`CRON_SECRET`** or one-off **`curl`** unless the secret is already available in the environment (see that skill).

## Chat tool smoke-test requirement

[`skills/chat-tools-smoke/SKILL.md`](skills/chat-tools-smoke/SKILL.md)

## User-facing site copy (identifiers)

On the public site (static pages under `/`, `/volley-chat/`, `/fivb-explorer/`, `/math-meteor/`, etc.), **do not show camelCase** for identifiers users read (math symbols, tool names in the UI, column names in explorer tables when sourced from APIs, and similar). Use **snake_case** instead (for example `team_elo`, `query_database`).

HTML/CSS `id` / `class` / `for` attributes may stay as needed for scripts and hooks; this rule applies to **visible** labels, body copy, and rendered math notation.

When displaying strings that might arrive in camelCase from an API, convert them to snake_case for display (see `displaySnake` in `volley-chat/volley.js` and `fivb-explorer/page.js`).
