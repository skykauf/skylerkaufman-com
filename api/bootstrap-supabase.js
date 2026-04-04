const { bootstrapSupabase } = require("../lib/bootstrap-supabase");

/**
 * GET/POST — ensures raw/staging/core/mart exist (idempotent).
 * Called from the home page on each visit; keep responses cacheable to limit DB chatter.
 */
module.exports = async function handler(req, res) {
  if (req.method !== "GET" && req.method !== "POST") {
    res.setHeader("Allow", "GET, POST");
    return res.status(405).json({ ok: false, error: "Method not allowed." });
  }

  try {
    const out = await bootstrapSupabase();
    if (out.skipped) {
      res.setHeader("Cache-Control", "private, no-store");
      return res.status(200).json({ ok: true, skipped: true, reason: out.reason });
    }
    res.setHeader(
      "Cache-Control",
      "public, max-age=300, s-maxage=300, stale-while-revalidate=600"
    );
    return res.status(200).json({ ok: true });
  } catch (err) {
    res.setHeader("Cache-Control", "private, no-store");
    return res.status(500).json({
      ok: false,
      error: err.message || String(err),
    });
  }
};
