/**
 * Vercel Cron / manual: dispatches the FIVB GitHub Actions workflow with DATABASE_URL
 * from this project so the long-running job (ETL + dbt + Elo) runs on GitHub, not Vercel.
 *
 * Vercel env:
 *   CRON_SECRET              – required; Vercel sends Authorization: Bearer … on cron
 *   DATABASE_URL             – Postgres URI (see lib/resolve-database-url.js for aliases). Not the Supabase anon/service_role keys.
 *   GITHUB_PAT               – classic PAT with repo + workflow, or fine-grained with Actions: write
 *   GITHUB_REPO              – optional, default skykauf/skylerkaufman-com
 *   GITHUB_DISPATCH_REF      – optional, default main
 *
 * GITHUB_ACTIONS_DISPATCH_TOKEN is accepted as an alias for GITHUB_PAT.
 */

const { resolveDatabaseUrl } = require("../lib/resolve-database-url");

module.exports = async function handler(req, res) {
  try {
    if (req.method !== "GET" && req.method !== "POST") {
      res.setHeader("Allow", "GET, POST");
      return res.status(405).json({ error: "Method not allowed." });
    }

    const cronSecret = process.env.CRON_SECRET;
    if (!cronSecret) {
      console.error(
        "[trigger-fivb-pipeline] CRON_SECRET is not set in this environment (Vercel → Settings → Environment Variables)."
      );
      return res.status(503).json({
        error:
          "CRON_SECRET must be set so only scheduled or authenticated callers can dispatch.",
      });
    }
    const auth = req.headers.authorization || "";
    if (auth !== `Bearer ${cronSecret}`) {
      console.error("[trigger-fivb-pipeline] Authorization header missing or wrong (cron must match CRON_SECRET).");
      return res.status(401).json({ error: "Unauthorized." });
    }

    const pat = process.env.GITHUB_PAT || process.env.GITHUB_ACTIONS_DISPATCH_TOKEN;
    const repo = (process.env.GITHUB_REPO || "skykauf/skylerkaufman-com").trim();
    const databaseUrl = resolveDatabaseUrl();

    if (!pat) {
      console.error("[trigger-fivb-pipeline] GITHUB_PAT (or GITHUB_ACTIONS_DISPATCH_TOKEN) is not set.");
      return res.status(503).json({
        error: "Set GITHUB_PAT (or GITHUB_ACTIONS_DISPATCH_TOKEN) with permission to dispatch workflows.",
      });
    }
    if (!databaseUrl) {
      console.error(
        "[trigger-fivb-pipeline] No Postgres URL: set DATABASE_URL (Supabase → Settings → Database → URI), or POSTGRES_URL / SUPABASE_DB_* — not anon or service_role keys."
      );
      return res.status(503).json({
        error:
          "Postgres connection string missing. In Supabase: Project Settings → Database → Connection string (URI). Add as DATABASE_URL on Vercel (API keys are not the database URL).",
      });
    }

    const parts = repo.split("/").map((s) => s.trim()).filter(Boolean);
    if (parts.length !== 2) {
      console.error("[trigger-fivb-pipeline] Invalid GITHUB_REPO:", repo);
      return res.status(500).json({ error: "Invalid GITHUB_REPO (expected owner/repo)." });
    }
    const [owner, repoName] = parts;
    const ref = process.env.GITHUB_DISPATCH_REF || "main";

    let gh;
    try {
      gh = await fetch(
        `https://api.github.com/repos/${owner}/${repoName}/actions/workflows/fivb-pipeline.yml/dispatches`,
        {
          method: "POST",
          headers: {
            Accept: "application/vnd.github+json",
            Authorization: `Bearer ${pat}`,
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            ref,
            inputs: {
              database_url: databaseUrl,
            },
          }),
        }
      );
    } catch (err) {
      console.error("[trigger-fivb-pipeline] fetch to GitHub failed:", err);
      return res.status(502).json({
        error: "Could not reach GitHub API.",
        detail: err.message || String(err),
      });
    }

    if (!gh.ok) {
      const detail = await gh.text();
      console.error("[trigger-fivb-pipeline] GitHub API error:", gh.status, detail.slice(0, 500));
      return res.status(502).json({
        error: "GitHub API error.",
        status: gh.status,
        detail: detail.slice(0, 2000),
      });
    }

    return res.status(202).json({
      ok: true,
      message: "FIVB workflow dispatched with DATABASE_URL input (long run on GitHub).",
      ref,
      repo: `${owner}/${repoName}`,
    });
  } catch (err) {
    console.error("[trigger-fivb-pipeline] unexpected error:", err);
    return res.status(500).json({
      error: "Unexpected handler error.",
      detail: err.message || String(err),
    });
  }
};
