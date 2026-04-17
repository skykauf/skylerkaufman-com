const { getAuthenticatedUser } = require("../lib/auth-supabase");
const { listConversationsForUser } = require("../lib/chat-history-store");

module.exports = async function handler(req, res) {
  res.setHeader("Cache-Control", "private, no-store");
  if (req.method !== "GET") {
    res.setHeader("Allow", "GET");
    return res.status(405).json({ error: "Method not allowed." });
  }

  const auth = await getAuthenticatedUser(req);
  if (auth.error) {
    return res.status(401).json({ error: auth.error, detail: auth.detail || null });
  }
  if (!auth.user) {
    return res.status(401).json({ error: "Authentication required." });
  }

  try {
    const out = await listConversationsForUser(auth.user.id);
    if (out.error) {
      return res.status(500).json({ error: out.error });
    }
    return res.status(200).json({ conversations: out.conversations || [] });
  } catch (err) {
    return res.status(500).json({
      error: "Failed to load chat history.",
      detail: err.message || String(err),
    });
  }
};
