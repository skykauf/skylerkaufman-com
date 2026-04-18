/**
 * Shared GitHub workflow_dispatch helpers for FIVB cron triggers (Vercel → GitHub Actions).
 */

const { resolveDatabaseUrl } = require("./resolve-database-url");

const DEFAULT_REPO = "skykauf/skylerkaufman-com";

/**
 * @param {import("http").IncomingMessage} req
 * @param {{ logPrefix?: string }} [opts]
 * @returns {{ ok: true, ctx: FivbDispatchContext } | { ok: false, status: number, json: object, allow?: string }}
 */
function authorizeFivbGithubDispatch(req, opts = {}) {
  const logPrefix = opts.logPrefix || "[fivb-github-dispatch]";
  if (req.method !== "GET" && req.method !== "POST") {
    return { ok: false, status: 405, json: { error: "Method not allowed." }, allow: "GET, POST" };
  }

  const cronSecret = process.env.CRON_SECRET;
  if (!cronSecret) {
    console.error(
      `${logPrefix} CRON_SECRET is not set in this environment (Vercel → Settings → Environment Variables).`
    );
    return {
      ok: false,
      status: 503,
      json: {
        error:
          "CRON_SECRET must be set so only scheduled or authenticated callers can dispatch.",
      },
    };
  }
  const auth = req.headers.authorization || "";
  if (auth !== `Bearer ${cronSecret}`) {
    console.error(`${logPrefix} Authorization header missing or wrong (cron must match CRON_SECRET).`);
    return { ok: false, status: 401, json: { error: "Unauthorized." } };
  }

  const pat = process.env.GITHUB_PAT || process.env.GITHUB_ACTIONS_DISPATCH_TOKEN;
  const repo = (process.env.GITHUB_REPO || DEFAULT_REPO).trim();
  const databaseUrl = resolveDatabaseUrl();

  if (!pat) {
    console.error(`${logPrefix} GITHUB_PAT (or GITHUB_ACTIONS_DISPATCH_TOKEN) is not set.`);
    return {
      ok: false,
      status: 503,
      json: {
        error: "Set GITHUB_PAT (or GITHUB_ACTIONS_DISPATCH_TOKEN) with permission to dispatch workflows.",
      },
    };
  }
  if (!databaseUrl) {
    console.error(
      `${logPrefix} No Postgres URL: set DATABASE_URL (Supabase → Settings → Database → URI), or POSTGRES_URL / SUPABASE_DB_* — not anon or service_role keys.`
    );
    return {
      ok: false,
      status: 503,
      json: {
        error:
          "Postgres connection string missing. In Supabase: Project Settings → Database → Connection string (URI). Add as DATABASE_URL on Vercel (API keys are not the database URL).",
      },
    };
  }

  const parts = repo.split("/").map((s) => s.trim()).filter(Boolean);
  if (parts.length !== 2) {
    console.error(`${logPrefix} Invalid GITHUB_REPO:`, repo);
    return { ok: false, status: 500, json: { error: "Invalid GITHUB_REPO (expected owner/repo)." } };
  }
  const [owner, repoName] = parts;
  const ref = process.env.GITHUB_DISPATCH_REF || "main";

  return {
    ok: true,
    ctx: { pat, owner, repoName, ref, databaseUrl },
  };
}

/**
 * @param {string} owner
 * @param {string} repoName
 * @param {string} workflowFilename e.g. fivb-vis-pipeline.yml
 * @param {{ ref: string, pat: string, inputs: Record<string, string> }} args
 * @returns {Promise<{ ok: boolean, status: number, detail: string }>}
 */
async function githubDispatchWorkflow(owner, repoName, workflowFilename, args) {
  const { ref, pat, inputs } = args;
  const url = `https://api.github.com/repos/${owner}/${repoName}/actions/workflows/${encodeURIComponent(
    workflowFilename
  )}/dispatches`;
  let gh;
  try {
    gh = await fetch(url, {
      method: "POST",
      headers: {
        Accept: "application/vnd.github+json",
        Authorization: `Bearer ${pat}`,
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ ref, inputs }),
    });
  } catch (err) {
    return {
      ok: false,
      status: 0,
      detail: err.message || String(err),
    };
  }
  const detail = await gh.text();
  return { ok: gh.ok, status: gh.status, detail };
}

module.exports = {
  authorizeFivbGithubDispatch,
  githubDispatchWorkflow,
};
