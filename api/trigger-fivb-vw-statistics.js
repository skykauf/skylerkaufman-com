/**
 * Vercel Cron / manual: dispatches the FIVB Volleyball World statistics workflow only
 * (HTML sitemap ingest → raw.raw_vw_player_tournament_stats). Isolated from VIS/dbt/Elo.
 *
 * Same Vercel env as other FIVB triggers: CRON_SECRET, DATABASE_URL, GITHUB_PAT, GITHUB_REPO, GITHUB_DISPATCH_REF.
 *
 * Daily cron uses `/api/trigger-fivb-pipelines` (dispatches VIS + VW in one schedule). This path remains for manual runs.
 */

const {
  authorizeFivbGithubDispatch,
  githubDispatchWorkflow,
} = require("../lib/fivb-github-dispatch");

module.exports = async function handler(req, res) {
  try {
    const auth = authorizeFivbGithubDispatch(req, { logPrefix: "[trigger-fivb-vw-statistics]" });
    if (!auth.ok) {
      if (auth.allow) res.setHeader("Allow", auth.allow);
      return res.status(auth.status).json(auth.json);
    }
    const { ctx } = auth;

    const gh = await githubDispatchWorkflow(ctx.owner, ctx.repoName, "fivb-vw-statistics.yml", {
      ref: ctx.ref,
      pat: ctx.pat,
      inputs: { database_url: ctx.databaseUrl },
    });

    if (!gh.ok && gh.status === 0) {
      console.error("[trigger-fivb-vw-statistics] fetch to GitHub failed:", gh.detail);
      return res.status(502).json({
        error: "Could not reach GitHub API.",
        detail: gh.detail,
      });
    }

    if (!gh.ok) {
      console.error("[trigger-fivb-vw-statistics] GitHub API error:", gh.status, gh.detail.slice(0, 500));
      return res.status(502).json({
        error: "GitHub API error.",
        status: gh.status,
        detail: gh.detail.slice(0, 2000),
      });
    }

    return res.status(202).json({
      ok: true,
      message: "FIVB VW statistics workflow dispatched (HTML ingest on GitHub only).",
      ref: ctx.ref,
      repo: `${ctx.owner}/${ctx.repoName}`,
    });
  } catch (err) {
    console.error("[trigger-fivb-vw-statistics] unexpected error:", err);
    return res.status(500).json({
      error: "Unexpected handler error.",
      detail: err.message || String(err),
    });
  }
};
