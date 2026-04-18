/**
 * Vercel Cron / manual: dispatches the FIVB VIS GitHub Actions workflow with DATABASE_URL
 * from this project so the long-running job (VIS ETL + dbt + Elo) runs on GitHub, not Vercel.
 *
 * Vercel env:
 *   CRON_SECRET              – required; Vercel sends Authorization: Bearer … on cron
 *   DATABASE_URL             – Postgres URI (see lib/resolve-database-url.js for aliases). Not the Supabase anon/service_role keys.
 *   GITHUB_PAT               – classic PAT with repo + workflow, or fine-grained with Actions: write
 *   GITHUB_REPO              – optional, default skykauf/skylerkaufman-com
 *   GITHUB_DISPATCH_REF      – optional, default main
 *
 * GITHUB_ACTIONS_DISPATCH_TOKEN is accepted as an alias for GITHUB_PAT.
 *
 * Daily cron uses `/api/trigger-fivb-pipelines` (dispatches VIS + VW in one schedule). This path remains for manual runs.
 */

const {
  authorizeFivbGithubDispatch,
  githubDispatchWorkflow,
} = require("../lib/fivb-github-dispatch");

module.exports = async function handler(req, res) {
  try {
    const auth = authorizeFivbGithubDispatch(req, { logPrefix: "[trigger-fivb-vis-pipeline]" });
    if (!auth.ok) {
      if (auth.allow) res.setHeader("Allow", auth.allow);
      return res.status(auth.status).json(auth.json);
    }
    const { ctx } = auth;

    const gh = await githubDispatchWorkflow(ctx.owner, ctx.repoName, "fivb-vis-pipeline.yml", {
      ref: ctx.ref,
      pat: ctx.pat,
      inputs: { database_url: ctx.databaseUrl },
    });

    if (!gh.ok && gh.status === 0) {
      console.error("[trigger-fivb-vis-pipeline] fetch to GitHub failed:", gh.detail);
      return res.status(502).json({
        error: "Could not reach GitHub API.",
        detail: gh.detail,
      });
    }

    if (!gh.ok) {
      console.error("[trigger-fivb-vis-pipeline] GitHub API error:", gh.status, gh.detail.slice(0, 500));
      return res.status(502).json({
        error: "GitHub API error.",
        status: gh.status,
        detail: gh.detail.slice(0, 2000),
      });
    }

    return res.status(202).json({
      ok: true,
      message: "FIVB VIS workflow dispatched with DATABASE_URL input (long run on GitHub).",
      ref: ctx.ref,
      repo: `${ctx.owner}/${ctx.repoName}`,
    });
  } catch (err) {
    console.error("[trigger-fivb-vis-pipeline] unexpected error:", err);
    return res.status(500).json({
      error: "Unexpected handler error.",
      detail: err.message || String(err),
    });
  }
};
