const { generateChatReply } = require("../lib/chat-service");

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed." });
  }

  try {
    const result = await generateChatReply(req.body?.messages);
    return res.status(result.status).json(result.body);
  } catch (err) {
    return res.status(500).json({
      error: "Chat request failed.",
      detail: err.message || String(err),
    });
  }
};
