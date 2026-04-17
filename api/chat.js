const { generateChatReply } = require("../lib/chat-service");
const { getAuthenticatedUser } = require("../lib/auth-supabase");
const { ensureConversationForUser, appendConversationMessages } = require("../lib/chat-history-store");

function latestUserMessage(messages) {
  if (!Array.isArray(messages)) return "";
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const m = messages[i];
    if (m && m.role === "user" && typeof m.content === "string" && m.content.trim()) {
      return m.content;
    }
  }
  return "";
}

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed." });
  }

  try {
    const auth = await getAuthenticatedUser(req);
    const persistForUser = !!auth.user && !auth.error;
    const conversationHint = req.body?.conversation_id ? String(req.body.conversation_id) : null;
    const lastUserPrompt = latestUserMessage(req.body?.messages);
    let activeConversationId = null;
    if (persistForUser) {
      const ensured = await ensureConversationForUser({
        userId: auth.user.id,
        conversationId: conversationHint,
        titleHint: lastUserPrompt,
      });
      if (ensured.error) {
        return res.status(500).json({ error: ensured.error });
      }
      activeConversationId = ensured.conversation.id;
    }

    const wantsStream = !!req.body?.stream;
    if (wantsStream) {
      res.setHeader("Content-Type", "application/x-ndjson; charset=utf-8");
      res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
      res.setHeader("Connection", "keep-alive");
      if (typeof res.flushHeaders === "function") res.flushHeaders();
      const writeEvent = (evt) => {
        res.write(`${JSON.stringify(evt)}\n`);
      };
      writeEvent({ type: "start" });
      const result = await generateChatReply(req.body?.messages, {
        onProgress: (evt) => writeEvent(evt),
        clientContext: req.body?.client_context || {},
        responseStyle: req.body?.response_style || "balanced",
      });
      if (persistForUser) {
        const saved = await appendConversationMessages({
          userId: auth.user.id,
          conversationId: activeConversationId,
          userMessage: lastUserPrompt,
          assistantMessage: result?.body?.content || "",
          assistantMeta: result?.body?.meta || null,
        });
        if (saved.error) {
          return res.status(500).json({ error: saved.error });
        }
      }
      if (persistForUser && result && result.body && typeof result.body === "object") {
        result.body.conversation_id = activeConversationId;
      }
      writeEvent({ type: "final", status: result.status, body: result.body });
      return res.end();
    }

    const result = await generateChatReply(req.body?.messages, {
      clientContext: req.body?.client_context || {},
      responseStyle: req.body?.response_style || "balanced",
    });
    if (persistForUser) {
      const saved = await appendConversationMessages({
        userId: auth.user.id,
        conversationId: activeConversationId,
        userMessage: lastUserPrompt,
        assistantMessage: result?.body?.content || "",
        assistantMeta: result?.body?.meta || null,
      });
      if (saved.error) {
        return res.status(500).json({ error: saved.error });
      }
      if (result && result.body && typeof result.body === "object") {
        result.body.conversation_id = activeConversationId;
      }
    }
    return res.status(result.status).json(result.body);
  } catch (err) {
    if (req.body?.stream) {
      res.write(
        `${JSON.stringify({
          type: "error",
          status: 500,
          error: "Chat request failed.",
          detail: err.message || String(err),
        })}\n`
      );
      return res.end();
    }
    return res.status(500).json({
      error: "Chat request failed.",
      detail: err.message || String(err),
    });
  }
};
