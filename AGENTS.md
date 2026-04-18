# Agent instructions (skylerkaufman.com)

## Git / delivery

Full workflow (branching, pushes, merges, substantive-change expectations) lives in the project Cursor skill: [.cursor/skills/git-delivery/SKILL.md](.cursor/skills/git-delivery/SKILL.md).

## Verify before you call it done

Run from the repo root:

```bash
npm install
npm run verify
```

That ensures `lib/chat-service.js` loads (syntax + deps).

**Optional — full chat path (needs secrets):**

```bash
export HF_TOKEN="hf_..."   # read-only Inference / provider access
# optional: export HF_MODEL=...  export DATABASE_URL=...
npm start
```

In another terminal:

```bash
curl -sS -X POST http://localhost:3000/api/chat \
  -H "Content-Type: application/json" \
  -d '{"messages":[{"role":"user","content":"Say hi in one short sentence."}]}'
```

Expect JSON with `"content"` on success. A `404` on `/api/chat` usually means the deploy is missing `api/chat.js` or the project isn’t wired for Vercel functions.

**After deploy (Vercel):** confirm env vars (`HF_TOKEN`, optional `HF_MODEL` / `DATABASE_URL`), redeploy, then smoke-test the same `curl` against production URL if appropriate.

## HF router

Chat uses Hugging Face **router** OpenAI-compatible **`/v1/chat/completions`**, not legacy `/models/{id}`. Default base is `https://router.huggingface.co/v1` (see `lib/chat-service.js`).

## Chat tool smoke-test requirement

When adding or changing structured chat tools in `lib/chat-service.js`, you must also update `runToolSmokeTests()` so the new/changed tool is exercised by `/api/chat-tools-smoke`.

Minimum requirement for tool-related changes:

- Add (or adjust) a representative smoke-test case for the tool in `runToolSmokeTests()`.
- Ensure `/api/chat-tools-smoke` returns `ok: true` locally (or in a deployed environment with valid DB env vars).
- Do not merge tool additions that are missing smoke-test coverage.
