/**
 * Vercel Cron / manual: dispatches the FIVB GitHub Actions workflow with DATABASE_URL
 * from this project so the long-running job (ETL + dbt + Elo) runs on GitHub, not Vercel.
 *
 * Vercel env:
 *   CRON_SECRET              – required; Vercel sends Authorization: Bearer … on cron
 *   DATABASE_URL             – Supabase/Postgres URL (same as rest of the site)
 *   GITHUB_PAT               – classic PAT with repo + workflow, or fine-grained with Actions: write
 *   GITHUB_REPO              – optional, default skykauf/skylerkaufman-com
 *   GITHUB_DISPATCH_REF      – optional, default main
 *
 * GITHUB_ACTIONS_DISPATCH_TOKEN is accepted as an alias for GITHUB_PAT.
 */

module.exports = async function handler(req, res) {
  if (req.method !== "GET" && req.method !== "POST") {
    res.setHeader("Allow", "GET, POST");
    return res.status(405).json({ error: "Method not allowed." });
  }

  const cronSecret = process.env.CRON_SECRET;
  if (!cronSecret) {
    return res.status(500).json({
      error: "CRON_SECRET must be set so only scheduled or authenticated callers can dispatch.",
    });
  }
  const auth = req.headers.authorization || "";
  if (auth !== `Bearer ${cronSecret}`) {
    return res.status(401).json({ error: "Unauthorized." });
  }

  const pat = process.env.GITHUB_PAT || process.env.GITHUB_ACTIONS_DISPATCH_TOKEN;
  const repo = (process.env.GITHUB_REPO || "skykauf/skylerkaufman-com").trim();
  const databaseUrl = process.env.DATABASE_URL;

  if (!pat) {
    return res.status(503).json({
      error: "Set GITHUB_PAT (or GITHUB_ACTIONS_DISPATCH_TOKEN) with permission to dispatch workflows.",
    });
  }
  if (!databaseUrl || String(databaseUrl).trim() === "") {
    return res.status(503).json({
      error: "DATABASE_URL must be set on Vercel so it can be forwarded to the workflow run.",
    });
  }

  const parts = repo.split("/").map((s) => s.trim()).filter(Boolean);
  if (parts.length !== 2) {
    return res.status(500).json({ error: "Invalid GITHUB_REPO (expected owner/repo)." });
  }
  const [owner, repoName] = parts;
  const ref = process.env.GITHUB_DISPATCH_REF || "main";

  const gh = await fetch(
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

  if (!gh.ok) {
    const detail = await gh.text();
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
};
