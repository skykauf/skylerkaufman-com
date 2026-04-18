# skylerkaufman.com

Personal site with interactive visuals plus **Volley Chat**.  
Chat uses ChatGPT with built-in access to FIVB VIS data, with Hugging Face router as fallback.

## Architecture

- Frontend: static pages (`/` and `/volley-chat/`)
- Chat API:
  - Local dev: `server.js` at `/api/chat` and `/api/bootstrap-supabase`
  - Vercel: `api/chat.js`, `api/bootstrap-supabase.js`
- LLM: OpenAI (`OPENAI_API_KEY`) or Hugging Face router (`HF_TOKEN`)
- Tool layer: FIVB VIS-focused volleyball tools (database-backed via `DATABASE_URL`)

## Quick start (local)

```bash
npm install
npm start
```

Open [http://localhost:3000](http://localhost:3000), then click the ringed **Volley Chat** planet.

## Environment variables

Use `.env.example` as reference:

- `OPENAI_API_KEY` (optional, preferred) - enables OpenAI chat + structured function calling
- `OPENAI_MODEL` (optional) - defaults to `gpt-4o-mini`
- `OPENAI_BASE_URL` (optional) - defaults to `https://api.openai.com/v1`
- `HF_TOKEN` (optional fallback) - Hugging Face access token
- `HF_MODEL` (optional) - defaults to `meta-llama/Meta-Llama-3-8B-Instruct`
- `HF_API_URL` (optional) - defaults to `https://router.huggingface.co/v1` (router chat API; plain `https://router.huggingface.co` is auto-suffixed with `/v1`)
- `DATABASE_URL` (optional) - enables `query_db`, automatic Supabase schema bootstrap on the home page, and the scheduled FIVB job (duplicate this value into GitHub Actions secrets for the workflow)
- `SUPABASE_URL` + `SUPABASE_ANON_KEY` (optional) - enables Google OAuth sign-in for Volley Chat and saved chat history for authenticated users
- `PORT` (local only)

## Vercel deployment

1. Import repo in Vercel.
2. Add env vars (`HF_TOKEN`, optional `HF_MODEL`, optional `DATABASE_URL`).
3. Deploy.

The chat page calls `/api/chat`, which resolves to the Vercel function in production.

## Notes on tool-calling

The backend supports structured tools including:
- `find_player`
- `player_profile`
- `head_to_head_players`
- `player_teammate_history`
- `most_improved_players`
- `country_depth_report`
- `player_status`
- `tournament_lookup`
- `tournament_snapshot`
- `partnership_profile`
- `similar_players`
- `explain_ranking`
- `top_players_by_country`
- `active_players`
- `inactive_players`
- `best_finishes_by_player`
- `player_recent_matches`
- `country_matchup_record`
- `country_opponent_performance`
- `query_db` (safe read-only SQL fallback)

Active/inactive tools use a fixed definition of activity: match played within the last 365 days.

OpenAI is used first when `OPENAI_API_KEY` is set; otherwise the app falls back to the Hugging Face JSON tool-call loop.

For safety, only `SELECT`/`WITH` read-only SQL is allowed in this skeleton.

Conversation UX notes:
- The chat sends `client_context` so entities (player/country/tournament) can persist across follow-ups.
- Users can pick a response style (`balanced`, `brief`, `table`, `scout`) from the UI.
- Tool progress events include previews and timing; assistant replies include latency + confidence/freshness hints.
- Signed-in users (Supabase Auth) get persisted chat history; guest chats remain ephemeral.

### Hidden tool smoke test

`GET /api/chat-tools-smoke` runs sample calls for all chat tools and returns pass/fail + timings.

- Optional protection: set `CHAT_SMOKE_TOKEN`, then send header `x-smoke-token: <token>`.
- Endpoint is intentionally not linked in the UI.

## Supabase schema bootstrap (no manual SQL)

With **`DATABASE_URL`** set on Vercel, the home page triggers **`GET /api/bootstrap-supabase`**, which runs idempotent DDL (`CREATE SCHEMA IF NOT EXISTS` for `raw`, `staging`, `core`, `mart` and matching **`GRANT`**s). That mirrors **`fivb-pipeline/supabase_setup.sql`** so you do not need to paste SQL in the Supabase dashboard. Responses are cacheable for a few minutes to keep load low.

## FIVB beach data pipeline (Supabase)

The **`fivb-pipeline/`** directory is the full ingest and modeling stack ported from the **fivb-leaderboard** project (VIS ETL, dbt staging/core/mart views, player Elo). It writes into **Postgres** (`raw`, `staging`, `core`, `mart`).

### Why Vercel triggers GitHub Actions?

The FIVB VIS pipeline run can take **on the order of an hour** (VIS ingest, `dbt`, Elo). That does not fit Vercel serverless limits, so **Vercel only schedules a tiny function** that **dispatches** **`.github/workflows/fivb-vis-pipeline.yml`** on GitHub. The workflow runs on **`ubuntu-latest`** with a **360-minute** timeout.

### Daily schedule (Vercel → GitHub)

**`vercel.json`** runs **one** cron job (same **`Authorization: Bearer $CRON_SECRET`** as other cron routes):

| Path | Schedule (UTC) | GitHub workflows |
|------|----------------|------------------|
| **`/api/trigger-fivb-pipelines`** | 06:00 | **`fivb-vis-pipeline.yml`** (VIS ETL, dbt, Elo) **and** **`fivb-vw-statistics.yml`** (Volleyball World HTML → `raw.raw_vw_player_tournament_stats`) |

You can still call **`/api/trigger-fivb-vis-pipeline`** or **`/api/trigger-fivb-vw-statistics`** manually to dispatch a **single** workflow.

**Manual run (production):** In **Cursor**, use the **`/vercel`** integration to open this project and trigger the cron (or use the Vercel dashboard: **Project → Cron Jobs →** path **`/api/trigger-fivb-pipelines` → Run**). That uses **`CRON_SECRET`** from the project env — you do not need to paste it into a terminal or chat. For scripts, `curl` with **`Authorization: Bearer …`** only when the secret is already in your environment.

Each handler calls GitHub **`workflow_dispatch`** with your **`DATABASE_URL`** from Vercel as **`database_url`**. You do **not** need a **`DATABASE_URL`** repository secret on GitHub for that path.

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
python run_fivb_vis_pipeline.py
```

Same behavior as the original **`scripts/run_pipeline.sh`** (incremental raw upserts, `dbt run`, Elo compute).

### Pipeline runtime tuning (optional)

For daily runs, the ETL can skip reloading heavy static dimensions when they were refreshed recently:

- `ETL_EVENTS_REFRESH_HOURS` (default `168`)
- `ETL_TEAMS_REFRESH_HOURS` (default `72`)
- `ETL_PLAYERS_REFRESH_HOURS` (default `168`)
- `ETL_FORCE_DIM_REFRESH=1` to bypass skips and force a full dimension refresh
