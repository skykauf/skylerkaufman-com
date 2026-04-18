---
name: chat-tools-smoke
description: >-
  Requires updating runToolSmokeTests when changing structured chat tools in
  lib/chat-service.js and validating /api/chat-tools-smoke. Use when adding or
  editing chat tools, smoke tests, or /api/chat-tools-smoke behavior.
---

# Chat tool smoke tests (skylerkaufman.com)

When adding or changing structured chat tools in `lib/chat-service.js`, you must also update `runToolSmokeTests()` so the new or changed tool is exercised by `/api/chat-tools-smoke`.

## Minimum requirement

- Add (or adjust) a representative smoke-test case for the tool in `runToolSmokeTests()`.
- Ensure `/api/chat-tools-smoke` returns `ok: true` locally (or in a deployed environment with valid DB env vars).
- Do not merge tool additions that are missing smoke-test coverage.
