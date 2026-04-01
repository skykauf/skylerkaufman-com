const express = require("express");
const path = require("path");
const { generateChatReply } = require("./lib/chat-service");

const app = express();
const PORT = Number(process.env.PORT) || 3000;

app.use(express.json({ limit: "512kb" }));

app.post("/api/chat", async (req, res) => {
  try {
    const result = await generateChatReply(req.body?.messages);
    res.status(result.status).json(result.body);
  } catch (err) {
    res.status(500).json({
      error: "Chat request failed.",
      detail: err.message || String(err),
    });
  }
});

app.use(express.static(path.join(__dirname)));

app.listen(PORT, () => {
  console.log(`Site + API -> http://localhost:${PORT}`);
});
