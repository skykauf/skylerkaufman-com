const { getFivbProfile } = require("../lib/fivb-profile");

module.exports = async function handler(req, res) {
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ ok: false, error: "Method not allowed." });
  }

  try {
    const out = await getFivbProfile();
    if (out.skipped) {
      res.setHeader("Cache-Control", "private, no-store");
      return res.status(200).json(out);
    }

    res.setHeader(
      "Cache-Control",
      "public, max-age=120, s-maxage=120, stale-while-revalidate=300"
    );
    return res.status(200).json(out);
  } catch (err) {
    res.setHeader("Cache-Control", "private, no-store");
    return res.status(500).json({
      ok: false,
      error: err.message || String(err),
    });
  }
};
