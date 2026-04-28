/**
 * Vercel Cron: dispatches both FIVB GitHub Actions workflows in one request —
 * `fivb-vis-pipeline.yml` (VIS ETL + dbt + Elo) and `fivb-vw-statistics.yml` (Volleyball World HTML).
 *
 * Same env as `trigger-fivb-vis-pipeline` / `trigger-fivb-vw-statistics`: CRON_SECRET,
 * DATABASE_URL, GITHUB_PAT, GITHUB_REPO, GITHUB_DISPATCH_REF.
 */

const {
  authorizeFivbGithubDispatch,
  githubDispatchWorkflow,
} = require("../lib/fivb-github-dispatch");

module.exports = async function handler(req, res) {
  try {
    // Lock down dispatch to scheduler-only traffic; do not allow public/manual triggering.
    const cronHeader = req.headers?.["x-vercel-cron"];
    if (!cronHeader) {
      return res.status(403).json({
        error: "Forbidden. This endpoint accepts Vercel Cron requests only.",
      });
    }

    const auth = authorizeFivbGithubDispatch(req, { logPrefix: "[trigger-fivb-pipelines]" });
    if (!auth.ok) {
      if (auth.allow) res.setHeader("Allow", auth.allow);
      return res.status(auth.status).json(auth.json);
    }
    const { ctx } = auth;
    const inputs = { database_url: ctx.databaseUrl };
    const rawWorkflow = String(req.query?.workflow || "").toLowerCase();
    const workflow = rawWorkflow === "vis" || rawWorkflow === "vw" ? rawWorkflow : "both";

    const runVis = workflow === "vis" || workflow === "both";
    const runVw = workflow === "vw" || workflow === "both";
    const [vis, vw] = await Promise.all([
      runVis
        ? githubDispatchWorkflow(ctx.owner, ctx.repoName, "fivb-vis-pipeline.yml", {
            ref: ctx.ref,
            pat: ctx.pat,
            inputs,
          })
        : Promise.resolve({ ok: true, status: 0, detail: "skipped" }),
      runVw
        ? githubDispatchWorkflow(ctx.owner, ctx.repoName, "fivb-vw-statistics.yml", {
            ref: ctx.ref,
            pat: ctx.pat,
            inputs,
          })
        : Promise.resolve({ ok: true, status: 0, detail: "skipped" }),
    ]);

    const visOk = vis.ok;
    const vwOk = vw.ok;
    if (!visOk && vis.status === 0) {
      console.error("[trigger-fivb-pipelines] VIS fetch failed:", vis.detail);
    }
    if (!vwOk && vw.status === 0) {
      console.error("[trigger-fivb-pipelines] VW fetch failed:", vw.detail);
    }
    if (!visOk && vis.status !== 0) {
      console.error("[trigger-fivb-pipelines] VIS GitHub API error:", vis.status, vis.detail.slice(0, 500));
    }
    if (!vwOk && vw.status !== 0) {
      console.error("[trigger-fivb-pipelines] VW GitHub API error:", vw.status, vw.detail.slice(0, 500));
    }

    if (visOk && vwOk) {
      const message =
        workflow === "vis"
          ? "FIVB VIS workflow dispatched."
          : workflow === "vw"
            ? "FIVB VW statistics workflow dispatched."
            : "Both FIVB workflows dispatched (VIS + Volleyball World statistics on GitHub).";
      return res.status(202).json({
        ok: true,
        message,
        workflow,
        ref: ctx.ref,
        repo: `${ctx.owner}/${ctx.repoName}`,
        dispatches: {
          fivb_vis_pipeline: runVis ? { ok: true } : { ok: true, skipped: true },
          fivb_vw_statistics: runVw ? { ok: true } : { ok: true, skipped: true },
        },
      });
    }

    return res.status(502).json({
      ok: false,
      error: "One or both workflow dispatches failed.",
      workflow,
      ref: ctx.ref,
      repo: `${ctx.owner}/${ctx.repoName}`,
      dispatches: {
        fivb_vis_pipeline: !runVis
          ? { ok: true, skipped: true }
          : visOk
          ? { ok: true }
          : { ok: false, status: vis.status, detail: vis.detail.slice(0, 2000) },
        fivb_vw_statistics: !runVw
          ? { ok: true, skipped: true }
          : vwOk
          ? { ok: true }
          : { ok: false, status: vw.status, detail: vw.detail.slice(0, 2000) },
      },
    });
  } catch (err) {
    console.error("[trigger-fivb-pipelines] unexpected error:", err);
    return res.status(500).json({
      error: "Unexpected handler error.",
      detail: err.message || String(err),
    });
  }
};
