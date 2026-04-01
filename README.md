# skylerkaufman.com

Personal site with interactive visuals plus **Volley Chat**.  
Chat now uses a hosted open-source LLM (Hugging Face Inference API) and includes a Postgres tool-calling skeleton (`query_db`) compatible with MCP-style patterns.

## Architecture

- Frontend: static pages (`/` and `/volley-chat/`)
- Chat API:
  - Local dev: `server.js` at `/api/chat`
  - Vercel: `api/chat.js` serverless function
- LLM: Hugging Face hosted OSS model (`HF_MODEL`)
- Tool layer: optional Postgres query tool via `DATABASE_URL`

## Quick start (local)

```bash
npm install
npm start
```

Open [http://localhost:3000](http://localhost:3000), then click the ringed **Volley Chat** planet.

## Environment variables

Use `.env.example` as reference:

- `HF_TOKEN` (required) - Hugging Face access token
- `HF_MODEL` (optional) - defaults to `meta-llama/Meta-Llama-3-8B-Instruct`
- `HF_API_URL` (optional) - defaults to `https://router.huggingface.co`
- `DATABASE_URL` (optional) - enables `query_db` tool
- `PORT` (local only)

## Vercel deployment

1. Import repo in Vercel.
2. Add env vars (`HF_TOKEN`, optional `HF_MODEL`, optional `DATABASE_URL`).
3. Deploy.

The chat page calls `/api/chat`, which resolves to the Vercel function in production.

## Notes on tool-calling

The backend uses a lightweight MCP-style loop:
1. LLM first pass can emit `{"tool":"query_db","sql":"..."}`.
2. Backend validates read-only SQL and executes against Postgres.
3. Tool result is fed back to the model for final response.

For safety, only `SELECT`/`WITH` read-only SQL is allowed in this skeleton.
