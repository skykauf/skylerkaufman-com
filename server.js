const express = require("express");
const path = require("path");
const { generateChatReply, runToolSmokeTests } = require("./lib/chat-service");
const { bootstrapSupabase } = require("./lib/bootstrap-supabase");
const { getFivbProfile } = require("./lib/fivb-profile");
const { runFivbTableExplorer } = require("./lib/fivb-table-explorer");
const { getAuthenticatedUser } = require("./lib/auth-supabase");
const {
  ensureConversationForUser,
  appendConversationMessages,
  listConversationsForUser,
  getConversationForUser,
  deleteConversationForUser,
} = require("./lib/chat-history-store");

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

app.post("/api/fivb-table-explorer", async (req, res) => {
  const action = String(req.body?.action || "");
  const filters = req.body?.filters || {};
  const out = await runFivbTableExplorer(action, filters);
  if (out?.skipped) {
    res.setHeader("Cache-Control", "private, no-store");
    return res.status(200).json(out);
  }
  if (!out?.ok) {
    return res.status(400).json(out);
  }
  res.setHeader("Cache-Control", "private, no-store");
  return res.status(200).json(out);
});

async function handleChatToolsSmoke(req, res) {
  const secret = process.env.CHAT_SMOKE_TOKEN || "";
  if (secret) {
    const header = req.headers["x-smoke-token"];
    if (typeof header !== "string" || header !== secret) {
      return res.status(401).json({ ok: false, error: "Unauthorized." });
    }
  }
  try {
    const out = await runToolSmokeTests();
    return res.status(out.ok ? 200 : 500).json(out);
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "Smoke test failed.",
      detail: err.message || String(err),
    });
  }
}

app.get("/api/chat-tools-smoke", handleChatToolsSmoke);
app.post("/api/chat-tools-smoke", handleChatToolsSmoke);

app.get("/api/auth-config", (_req, res) => {
  const url = (process.env.SUPABASE_URL || "").trim().replace(/\/$/, "");
  const anonKey = (process.env.SUPABASE_ANON_KEY || "").trim();
  const enabled = !!(url && anonKey);
  res.setHeader(
    "Cache-Control",
    "public, max-age=120, s-maxage=120, stale-while-revalidate=300"
  );
  return res.status(200).json({
    enabled,
    url: enabled ? url : "",
    anonKey: enabled ? anonKey : "",
  });
});

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

app.post("/api/chat", async (req, res) => {
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
});

app.get("/api/chat-history", async (req, res) => {
  res.setHeader("Cache-Control", "private, no-store");
  const auth = await getAuthenticatedUser(req);
  if (auth.error) return res.status(401).json({ error: auth.error, detail: auth.detail || null });
  if (!auth.user) return res.status(401).json({ error: "Authentication required." });
  try {
    const out = await listConversationsForUser(auth.user.id);
    if (out.error) return res.status(500).json({ error: out.error });
    return res.status(200).json({ conversations: out.conversations || [] });
  } catch (err) {
    return res.status(500).json({ error: "Failed to load chat history.", detail: err.message || String(err) });
  }
});

app.get("/api/chat-history/:id", async (req, res) => {
  res.setHeader("Cache-Control", "private, no-store");
  const auth = await getAuthenticatedUser(req);
  if (auth.error) return res.status(401).json({ error: auth.error, detail: auth.detail || null });
  if (!auth.user) return res.status(401).json({ error: "Authentication required." });
  try {
    const out = await getConversationForUser({ userId: auth.user.id, conversationId: req.params.id });
    if (out.error) return res.status(out.error === "Conversation not found." ? 404 : 500).json({ error: out.error });
    return res.status(200).json(out);
  } catch (err) {
    return res
      .status(500)
      .json({ error: "Failed to load conversation.", detail: err.message || String(err) });
  }
});

app.delete("/api/chat-history/:id", async (req, res) => {
  res.setHeader("Cache-Control", "private, no-store");
  const auth = await getAuthenticatedUser(req);
  if (auth.error) return res.status(401).json({ error: auth.error, detail: auth.detail || null });
  if (!auth.user) return res.status(401).json({ error: "Authentication required." });
  try {
    const out = await deleteConversationForUser({ userId: auth.user.id, conversationId: req.params.id });
    if (out.error) return res.status(out.error === "Conversation not found." ? 404 : 500).json({ error: out.error });
    return res.status(200).json({ ok: true });
  } catch (err) {
    return res
      .status(500)
      .json({ error: "Failed to delete conversation.", detail: err.message || String(err) });
  }
});

app.use(express.static(path.join(__dirname)));

app.listen(PORT, () => {
  console.log(`Site + API -> http://localhost:${PORT}`);
});
