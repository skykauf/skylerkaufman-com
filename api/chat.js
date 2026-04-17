const { generateChatReply } = require("../lib/chat-service");

module.exports = async function handler(req, res) {
  if (req.method !== "POST") {
    res.setHeader("Allow", "POST");
    return res.status(405).json({ error: "Method not allowed." });
  }

  try {
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
      });
      writeEvent({ type: "final", status: result.status, body: result.body });
      return res.end();
    }

    const result = await generateChatReply(req.body?.messages);
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
