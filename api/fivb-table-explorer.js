const { runFivbTableExplorer } = require("../lib/fivb-table-explorer");

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ ok: false, error: "Method not allowed." });
  }

  const action = String(req.body?.action || "");
  const filters = req.body?.filters || {};

  const out = await runFivbTableExplorer(action, filters);
  if (out?.skipped) {
    res.setHeader("Cache-Control", "private, no-store");
    return res.status(200).json(out);
  }
  if (!out?.ok) {
    return res.status(400).json(out);
  }
  res.setHeader("Cache-Control", "private, no-store");
  return res.status(200).json(out);
};
