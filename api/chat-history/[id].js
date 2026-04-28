const { getAuthenticatedUser } = require("../../lib/auth-supabase");
const {
  getConversationForUser,
  setConversationShareForUser,
  deleteConversationForUser,
} = require("../../lib/chat-history-store");

module.exports = async function handler(req, res) {
  res.setHeader("Cache-Control", "private, no-store");
  const rawId = req.query?.id;
  const conversationId = Array.isArray(rawId) ? rawId[0] : rawId;
  if (!conversationId) {
    return res.status(400).json({ error: "Missing conversation id." });
  }

  if (req.method !== "GET" && req.method !== "PATCH" && req.method !== "DELETE") {
    res.setHeader("Allow", "GET, PATCH, DELETE");
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
    if (req.method === "GET") {
      const out = await getConversationForUser({ userId: auth.user.id, conversationId });
      if (out.error) {
        const code = out.error === "Conversation not found." ? 404 : 500;
        return res.status(code).json({ error: out.error });
      }
      return res.status(200).json(out);
    }

    if (req.method === "PATCH") {
      const isPublic = req.body?.is_public;
      if (typeof isPublic !== "boolean") {
        return res.status(400).json({ error: "Expected boolean `is_public`." });
      }
      const out = await setConversationShareForUser({
        userId: auth.user.id,
        conversationId,
        isPublic,
      });
      if (out.error) {
        const code = out.error === "Conversation not found." ? 404 : 500;
        return res.status(code).json({ error: out.error });
      }
      return res.status(200).json({ conversation: out.conversation });
    }

    const del = await deleteConversationForUser({ userId: auth.user.id, conversationId });
    if (del.error) {
      const code = del.error === "Conversation not found." ? 404 : 500;
      return res.status(code).json({ error: del.error });
    }
    return res.status(200).json({ ok: true });
  } catch (err) {
    return res.status(500).json({
      error: "Failed to process chat history request.",
      detail: err.message || String(err),
    });
  }
};
