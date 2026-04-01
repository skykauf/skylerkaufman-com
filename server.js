const express = require("express");
const path = require("path");

const app = express();
const PORT = Number(process.env.PORT) || 3000;
const OLLAMA_URL = (process.env.OLLAMA_URL || "http://127.0.0.1:11434").replace(
  /\/$/,
  ""
);
const OLLAMA_MODEL = process.env.OLLAMA_MODEL || "llama3.2";

app.use(express.json({ limit: "512kb" }));

app.post("/api/chat", async (req, res) => {
  try {
    const { messages } = req.body;
    if (!Array.isArray(messages) || messages.length === 0) {
      return res.status(400).json({ error: "Expected a non-empty messages array." });
    }

    const sanitized = messages
      .slice(-40)
      .map((m) => {
        if (!m || typeof m.content !== "string") return null;
        const role = m.role === "assistant" ? "assistant" : "user";
        return { role, content: m.content.slice(0, 24000) };
      })
      .filter(Boolean);

    if (sanitized.length === 0) {
      return res.status(400).json({ error: "No valid messages." });
    }

    const ollamaRes = await fetch(`${OLLAMA_URL}/api/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        model: OLLAMA_MODEL,
        messages: [
          {
            role: "system",
            content:
              "You are a helpful assistant inside Volley Chat, a small local demo on a personal site. Be concise, friendly, and clear.",
          },
          ...sanitized,
        ],
        stream: false,
      }),
    });

    const raw = await ollamaRes.text();
    if (!ollamaRes.ok) {
      return res.status(502).json({
        error: "Local model request failed. Is Ollama running with this model pulled?",
        detail: raw.slice(0, 800),
      });
    }

    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      return res.status(502).json({ error: "Invalid response from Ollama.", detail: raw.slice(0, 200) });
    }

    const content = data.message?.content ?? "";
    res.json({ role: "assistant", content });
  } catch (err) {
    console.error(err);
    res.status(500).json({
      error: "Could not reach the local model server.",
      detail: err.message || String(err),
    });
  }
});

app.use(express.static(path.join(__dirname)));

app.listen(PORT, () => {
  console.log(`Site + local chat API → http://localhost:${PORT}`);
  console.log(`Ollama → ${OLLAMA_URL} (model: ${OLLAMA_MODEL})`);
});
