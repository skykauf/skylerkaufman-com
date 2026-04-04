const express = require("express");
const path = require("path");
const { generateChatReply } = require("./lib/chat-service");
const { bootstrapSupabase } = require("./lib/bootstrap-supabase");

const app = express();
const PORT = Number(process.env.PORT) || 3000;

app.use(express.json({ limit: "512kb" }));

async function handleBootstrapSupabase(req, res) {
  try {
    const out = await bootstrapSupabase();
    if (out.skipped) {
      res.setHeader("Cache-Control", "private, no-store");
      return res.status(200).json({ ok: true, skipped: true, reason: out.reason });
    }
    res.setHeader(
      "Cache-Control",
      "public, max-age=300, s-maxage=300, stale-while-revalidate=600"
    );
    return res.status(200).json({ ok: true });
  } catch (err) {
    res.setHeader("Cache-Control", "private, no-store");
    return res.status(500).json({
      ok: false,
      error: err.message || String(err),
    });
  }
}

app.get("/api/bootstrap-supabase", handleBootstrapSupabase);
app.post("/api/bootstrap-supabase", handleBootstrapSupabase);

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
