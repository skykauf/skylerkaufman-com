# skylerkaufman.com

Personal site with interactive visuals plus **Volley Chat**.  
Chat now uses a hosted open-source LLM (Hugging Face Inference API) and includes a Postgres tool-calling skeleton (`query_db`) compatible with MCP-style patterns.

## Architecture

- Frontend: static pages (`/` and `/volley-chat/`)
- Chat API:
  - Local dev: `server.js` at `/api/chat` and `/api/bootstrap-supabase`
  - Vercel: `api/chat.js`, `api/bootstrap-supabase.js`
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
- `HF_API_URL` (optional) - defaults to `https://router.huggingface.co/v1` (router chat API; plain `https://router.huggingface.co` is auto-suffixed with `/v1`)
- `DATABASE_URL` (optional) - enables `query_db`, automatic Supabase schema bootstrap on the home page, and the scheduled FIVB job (duplicate this value into GitHub Actions secrets for the workflow)
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

## Supabase schema bootstrap (no manual SQL)

With **`DATABASE_URL`** set on Vercel, the home page triggers **`GET /api/bootstrap-supabase`**, which runs idempotent DDL (`CREATE SCHEMA IF NOT EXISTS` for `raw`, `staging`, `core`, `mart` and matching **`GRANT`**s). That mirrors **`fivb-pipeline/supabase_setup.sql`** so you do not need to paste SQL in the Supabase dashboard. Responses are cacheable for a few minutes to keep load low.

## FIVB beach data pipeline (Supabase)

The **`fivb-pipeline/`** directory is the full ingest and modeling stack ported from the **fivb-leaderboard** project (VIS ETL, dbt staging/core/mart views, player Elo). It writes into **Postgres** (`raw`, `staging`, `core`, `mart`).

### Why GitHub Actions?

The job pulls the full VIS dataset, runs **dbt**, and recomputes Elo. That routinely exceeds Vercel serverless time limits, so **`.github/workflows/fivb-pipeline.yml`** runs on a **schedule** (`0 6 * * *` UTC) on GitHub-hosted runners.

### Connecting GitHub Actions to Supabase

GitHub’s runners **cannot read** your Vercel project environment variables. There is no supported way to “inject” Vercel env into a scheduled workflow without storing credentials on GitHub. Add a single repository secret **`DATABASE_URL`** (**Settings → Secrets and variables → Actions**) using the **same** Supabase Postgres URL as on Vercel (prefer the **direct** connection on port **5432** for long `dbt` runs; include **`?sslmode=require`** if your provider expects it). The workflow fails early with a clear error if this secret is missing.

### Manual / local run

From **`fivb-pipeline/`** with Python 3.12:

```bash
pip install -r requirements-cron.txt
export DATABASE_URL='postgresql://...'
python run_full_pipeline.py
```

Same behavior as the original **`scripts/run_pipeline.sh`** (incremental raw upserts, `dbt run`, Elo compute).
