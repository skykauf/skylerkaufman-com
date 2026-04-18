---
name: repo-verify
description: >-
  skylerkaufman.com pre-merge verification—npm verify, optional local chat API
  smoke test, Hugging Face router usage, and Vercel deploy env checks. Use when
  finishing a change, before merge, or when debugging chat/deploy issues.
---

# Verify and chat stack (skylerkaufman.com)

## Before you call a change done

Run from the repo root:

```bash
npm install
npm run verify
```

That ensures `lib/chat-service.js` loads (syntax + deps).

## Hugging Face router

Chat uses Hugging Face **router** OpenAI-compatible **`/v1/chat/completions`**, not legacy `/models/{id}`. Default base is `https://router.huggingface.co/v1` (see `lib/chat-service.js`).

## Optional — full chat path (needs secrets)

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

## After deploy (Vercel)

Confirm env vars (`HF_TOKEN`, optional `HF_MODEL` / `DATABASE_URL`), redeploy, then smoke-test the same `curl` against the production URL if appropriate.
