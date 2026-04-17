const express = require("express");
const path = require("path");
const { generateChatReply } = require("./lib/chat-service");
const { bootstrapSupabase } = require("./lib/bootstrap-supabase");
const { getFivbProfile } = require("./lib/fivb-profile");

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

app.get("/api/fivb-profile", async (req, res) => {
  try {
    const out = await getFivbProfile();
    if (out.skipped) {
      res.setHeader("Cache-Control", "private, no-store");
      return res.status(200).json(out);
    }
    res.setHeader(
      "Cache-Control",
      "public, max-age=120, s-maxage=120, stale-while-revalidate=300"
    );
    return res.status(200).json(out);
  } catch (err) {
    res.setHeader("Cache-Control", "private, no-store");
    return res.status(500).json({
      ok: false,
      error: err.message || String(err),
    });
  }
});

app.post("/api/chat", async (req, res) => {
  try {
    const wantsStream = !!req.body?.stream;
    if (wantsStream) {
      res.setHeader("Content-Type", "application/x-ndjson; charset=utf-8");
      res.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
      res.setHeader("Connection", "keep-alive");
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
});

app.use(express.static(path.join(__dirname)));

app.listen(PORT, () => {
  console.log(`Site + API -> http://localhost:${PORT}`);
});
