const { runToolSmokeTests } = require("../lib/chat-service");

function isAuthorized(req) {
  const secret = process.env.CHAT_SMOKE_TOKEN || "";
  if (!secret) return true;
  const header = req.headers["x-smoke-token"];
  return typeof header === "string" && header === secret;
}

module.exports = async function handler(req, res) {
  if (req.method !== "GET" && req.method !== "POST") {
    res.setHeader("Allow", "GET, POST");
    return res.status(405).json({ ok: false, error: "Method not allowed." });
  }
  if (!isAuthorized(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized." });
  }

  try {
    const out = await runToolSmokeTests();
    return res.status(out.ok ? 200 : 500).json(out);
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "Smoke test failed.",
      detail: err.message || String(err),
    });
  }
};
