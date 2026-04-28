const { getConversationByShareToken } = require("../../../lib/chat-history-store");

module.exports = async function handler(req, res) {
  res.setHeader("Cache-Control", "public, max-age=60, s-maxage=60, stale-while-revalidate=300");
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed." });
  }

  const rawToken = req.query?.token;
  const token = Array.isArray(rawToken) ? rawToken[0] : rawToken;
  if (!token) {
    return res.status(400).json({ error: "Missing share token." });
  }

  try {
    const out = await getConversationByShareToken(String(token));
    if (out.error) {
      const code = out.error === "Conversation not found." ? 404 : 500;
      return res.status(code).json({ error: out.error });
    }
    return res.status(200).json(out);
  } catch (err) {
    return res.status(500).json({
      error: "Failed to load shared conversation.",
      detail: err.message || String(err),
    });
  }
};
