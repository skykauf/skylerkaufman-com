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

### Why Vercel triggers GitHub Actions?

The full run can take **on the order of an hour** (VIS ingest, `dbt`, Elo). That does not fit Vercel serverless limits, so **Vercel only schedules a tiny function** that **dispatches** **`.github/workflows/fivb-pipeline.yml`** on GitHub. The workflow runs on **`ubuntu-latest`** with a **180-minute** timeout.

### Daily schedule (Vercel → GitHub)

**`vercel.json`** runs **`/api/trigger-fivb-pipeline`** daily at **06:00 UTC**. That handler (with **`Authorization: Bearer $CRON_SECRET`**) calls GitHub’s API to start **`workflow_dispatch`** on **`fivb-pipeline.yml`**, passing your **`DATABASE_URL`** from Vercel as the workflow input **`database_url`**. You do **not** need a **`DATABASE_URL`** repository secret on GitHub for that path.

**Vercel environment variables:**

| Variable | Purpose |
|----------|---------|
| **`DATABASE_URL`** | Same Supabase/Postgres URL as the rest of the site (prefer direct **5432** for long `dbt` runs; add **`?sslmode=require`** if needed). |
| **`CRON_SECRET`** | Random string; Vercel sends it as `Authorization: Bearer …` on cron invocations. |
| **`GITHUB_PAT`** | Personal access token able to dispatch Actions (classic: **`repo`** + **`workflow`**; or fine-grained: **Actions: Read and write** on this repo). Alias: **`GITHUB_ACTIONS_DISPATCH_TOKEN`**. |
| **`GITHUB_REPO`** | Optional. Default **`skykauf/skylerkaufman-com`**. |
| **`GITHUB_DISPATCH_REF`** | Optional. Default **`main`**. |

**Security note:** The connection string is sent to GitHub as a **workflow input** and may appear in the Actions run UI for that job. Use a **private** repository and avoid sharing run links if that is a concern. Alternatively, leave the Vercel input empty and use only a **`DATABASE_URL`** **repository secret** when starting the workflow from the Actions tab.

### Manual / local run

From **`fivb-pipeline/`** with Python 3.12:

```bash
pip install -r requirements-cron.txt
export DATABASE_URL='postgresql://...'
python run_full_pipeline.py
```

Same behavior as the original **`scripts/run_pipeline.sh`** (incremental raw upserts, `dbt run`, Elo compute).
