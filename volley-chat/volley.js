(() => {
  /** User-facing identifiers: show snake_case, never camelCase (only transforms strings with a lower→upper boundary). */
  function displaySnake(str) {
    const s = String(str ?? "");
    if (!s) return s;
    if (!/[a-z][A-Z]/.test(s)) return s;
    return s
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .replace(/([A-Z])([A-Z][a-z])/g, "$1_$2")
      .toLowerCase();
  }

  const thread = document.getElementById("thread");
  const form = document.getElementById("composer");
  const input = document.getElementById("input");
  const sendBtn = document.getElementById("send");
  const statusEl = document.getElementById("status");
  const modelMetaEl = document.getElementById("modelMeta");
  const toolActivityEl = document.getElementById("toolActivity");
  const responseStyleEl = document.getElementById("responseStyle");
  const contextBarEl = document.getElementById("contextBar");
  const googleSignInBtn = document.getElementById("googleSignIn");
  const signOutBtn = document.getElementById("signOut");
  const authStatusEl = document.getElementById("authStatus");
  const historyListEl = document.getElementById("historyList");
  const newChatBtn = document.getElementById("newChat");
  const toolsExplorerBodyEl = document.getElementById("toolsExplorerBody");

  const tabKey = "volley-chat-conversation-id";
  let canAuth = false;
  let supabase = null;

  /** @type {{ role: string, content: string }[]} */
  const history = [];
  let conversations = [];
  let activeConversationId = sessionStorage.getItem(tabKey) || null;
  let accessToken = "";
  let currentUser = null;
  let sessionContext = {};
  let modelLabel = "Unknown";
  let runMetrics = { modelMs: 0, toolMs: 0, toolCount: 0, startedAt: 0 };

  function setStatus(text) {
    statusEl.textContent = text || "";
  }

  function setModelMeta(provider, model) {
    if (!provider || !model) return;
    modelLabel = `${provider} · ${model}`;
    modelMetaEl.textContent = modelLabel;
  }

  function clearThread() {
    thread.innerHTML = "";
  }

  function renderHistoryList() {
    historyListEl.innerHTML = "";
    if (!currentUser) {
      const empty = document.createElement("div");
      empty.className = "volley-history-item-meta";
      empty.textContent = "Sign in to save and load chat history.";
      historyListEl.appendChild(empty);
      return;
    }
    if (!Array.isArray(conversations) || conversations.length === 0) {
      const empty = document.createElement("div");
      empty.className = "volley-history-item-meta";
      empty.textContent = "No saved chats yet.";
      historyListEl.appendChild(empty);
      return;
    }
    conversations.forEach((c) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = `volley-history-item${c.id === activeConversationId ? " active" : ""}`;
      const title = document.createElement("div");
      title.className = "volley-history-item-title";
      title.textContent = c.title || "New chat";
      const meta = document.createElement("div");
      meta.className = "volley-history-item-meta";
      const when = c.updated_at ? new Date(c.updated_at).toLocaleString() : "";
      meta.textContent = when;
      btn.appendChild(title);
      btn.appendChild(meta);
      btn.addEventListener("click", () => {
        loadConversation(c.id);
      });
      historyListEl.appendChild(btn);
    });
  }

  function renderFromHistory() {
    clearThread();
    history.forEach((m) => appendBubble(m.role, m.content));
  }

  async function apiJson(path, options = {}) {
    const headers = { ...(options.headers || {}) };
    if (accessToken) headers.Authorization = `Bearer ${accessToken}`;
    const res = await fetch(path, { ...options, headers });
    const text = await res.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = {};
    }
    if (!res.ok) {
      throw new Error(data.error || `Request failed (${res.status})`);
    }
    return data;
  }

  async function refreshConversations() {
    if (!currentUser) {
      conversations = [];
      renderHistoryList();
      return;
    }
    const data = await apiJson("/api/chat-history");
    conversations = Array.isArray(data.conversations) ? data.conversations : [];
    if (activeConversationId && !conversations.find((c) => c.id === activeConversationId)) {
      activeConversationId = null;
      sessionStorage.removeItem(tabKey);
    }
    renderHistoryList();
  }

  async function loadConversation(id) {
    if (!id || !currentUser) return;
    const data = await apiJson(`/api/chat-history/${encodeURIComponent(id)}`);
    const msgs = Array.isArray(data.messages) ? data.messages : [];
    history.length = 0;
    msgs.forEach((m) => {
      if (m && (m.role === "user" || m.role === "assistant") && typeof m.content === "string") {
        history.push({ role: m.role, content: m.content });
      }
    });
    activeConversationId = id;
    sessionStorage.setItem(tabKey, id);
    renderHistoryList();
    renderFromHistory();
    setStatus("");
  }

  function startNewChat() {
    history.length = 0;
    resetContext();
    clearToolActivity();
    clearThread();
    activeConversationId = null;
    sessionStorage.removeItem(tabKey);
    renderHistoryList();
    setStatus(currentUser ? "New saved chat will start on your next message." : "Guest chat reset.");
  }

  function setAuthUi() {
    if (!canAuth) {
      googleSignInBtn.hidden = true;
      signOutBtn.hidden = true;
      authStatusEl.textContent = "Guest mode (auth not configured).";
      return;
    }
    if (currentUser) {
      googleSignInBtn.hidden = true;
      signOutBtn.hidden = false;
      authStatusEl.textContent = currentUser.email ? `Signed in: ${currentUser.email}` : "Signed in";
      return;
    }
    googleSignInBtn.hidden = false;
    signOutBtn.hidden = true;
    authStatusEl.textContent = "Guest mode (ephemeral chat)";
  }

  function resetContext() {
    sessionContext = {};
    renderContextBar();
  }

  function renderContextBar() {
    contextBarEl.innerHTML = "";
    const entries = [];
    if (sessionContext.country_code) entries.push(["Country", sessionContext.country_code]);
    if (sessionContext.player_name) entries.push(["Player", sessionContext.player_name]);
    if (!sessionContext.player_name && sessionContext.player_id) entries.push(["Player ID", String(sessionContext.player_id)]);
    if (sessionContext.tournament_id) entries.push(["Tournament", String(sessionContext.tournament_id)]);
    if (sessionContext.gender) entries.push(["Gender", String(sessionContext.gender)]);
    if (entries.length === 0) return;
    entries.forEach(([k, v]) => {
      const chip = document.createElement("span");
      chip.className = "volley-context-chip";
      chip.textContent = `${k}: ${v}`;
      contextBarEl.appendChild(chip);
    });
    const clear = document.createElement("button");
    clear.type = "button";
    clear.className = "volley-context-chip clear";
    clear.textContent = "Clear context";
    clear.addEventListener("click", resetContext);
    contextBarEl.appendChild(clear);
  }

  function formatErrorMessage(status, data, rawText) {
    if (status === 404) {
      return "Chat API route not found (/api/chat). Deploy latest code or run the local server with `npm start`.";
    }
    if (status === 401 || status === 403) {
      return "Chat API auth failed. Check `HF_TOKEN` in project environment variables.";
    }
    if (status === 429) {
      return "Rate limited by model provider. Wait a moment and try again.";
    }
    if (status >= 500) {
      const detail = data?.detail || rawText;
      if (detail && typeof detail === "string") {
        return `Server error (${status}): ${detail.slice(0, 240)}`;
      }
      return `Server error (${status}). Check function logs for /api/chat.`;
    }
    if (typeof data?.error === "string") {
      return `${data.error} (${status})`;
    }
    return `Request failed (${status}). Check env vars and deployment config.`;
  }

  function appendBubble(role, content, metaText = "") {
    const div = document.createElement("div");
    div.className = `volley-msg ${role}`;
    const label = document.createElement("div");
    label.className = "volley-role";
    label.textContent = role === "user" ? "You" : modelLabel;
    const body = document.createElement("div");
    body.textContent = content;
    div.appendChild(label);
    div.appendChild(body);
    if (metaText) {
      const meta = document.createElement("div");
      meta.className = "volley-msg-meta";
      meta.textContent = metaText;
      div.appendChild(meta);
    }
    thread.appendChild(div);
    thread.scrollTop = thread.scrollHeight;
  }

  function clearToolActivity() {
    toolActivityEl.innerHTML = "";
  }

  function pushToolActivity(text, active = false, detail = null) {
    const line = document.createElement("div");
    line.className = `volley-tool-line${active ? " active" : ""}`;
    line.textContent = text;
    toolActivityEl.appendChild(line);
    if (detail) {
      const d = document.createElement("details");
      d.className = "volley-tool-detail";
      const s = document.createElement("summary");
      s.textContent = "Details";
      d.appendChild(s);
      const pre = document.createElement("pre");
      pre.className = "volley-tool-pre";
      pre.textContent = JSON.stringify(detail, null, 2);
      d.appendChild(pre);
      const actions = buildQuickActions(detail);
      if (actions.length > 0) {
        const wrap = document.createElement("div");
        wrap.className = "volley-quick-actions";
        actions.forEach((a) => {
          const btn = document.createElement("button");
          btn.type = "button";
          btn.className = "volley-quick-btn";
          btn.textContent = a.label;
          btn.addEventListener("click", () => {
            input.value = a.prompt;
            input.focus();
          });
          wrap.appendChild(btn);
        });
        d.appendChild(wrap);
      }
      toolActivityEl.appendChild(d);
    }
    toolActivityEl.scrollTop = toolActivityEl.scrollHeight;
  }

  function buildQuickActions(detail) {
    if (!Array.isArray(detail)) return [];
    const out = [];
    for (const row of detail.slice(0, 3)) {
      if (row && row.player_id && row.player_name) {
        out.push({
          label: `Profile: ${row.player_name}`,
          prompt: `Show player profile for player_id ${row.player_id}.`,
        });
      } else if (row && row.tournament_id && row.tournament_name) {
        out.push({
          label: `Snapshot: ${row.tournament_name}`,
          prompt: `Show tournament snapshot for tournament_id ${row.tournament_id}.`,
        });
      }
    }
    return out;
  }

  function stageLabel(stage) {
    if (stage === "plan_tools") return "Planning tool usage";
    if (stage === "final_answer") return "Composing final answer";
    if (stage === "hf_first_pass") return "Model first pass";
    if (stage === "hf_second_pass") return "Model second pass";
    return stage || "Model step";
  }

  function formatDuration(ms) {
    const n = Number(ms);
    if (!Number.isFinite(n) || n < 0) return "";
    if (n < 1000) return `${Math.round(n)}ms`;
    return `${(n / 1000).toFixed(1)}s`;
  }

  function handleProgressEvent(evt) {
    if (!evt || typeof evt !== "object") return;
    if (evt.type === "start") {
      clearToolActivity();
      pushToolActivity("Starting request…", true);
      runMetrics = { modelMs: 0, toolMs: 0, toolCount: 0, startedAt: Date.now() };
      return;
    }
    if (evt.type === "model_start") {
      const modelName = evt.model ? ` (${evt.model})` : "";
      pushToolActivity(`${stageLabel(evt.stage)}${modelName}…`, true);
      return;
    }
    if (evt.type === "model_done") {
      const d = formatDuration(evt.duration_ms);
      runMetrics.modelMs += Number(evt.duration_ms || 0);
      pushToolActivity(`${stageLabel(evt.stage)} done${d ? ` in ${d}` : ""}.`);
      return;
    }
    if (evt.type === "tool_start") {
      pushToolActivity(`Running tool: ${displaySnake(evt.tool)}`, true);
      return;
    }
    if (evt.type === "tool_done") {
      const rowInfo = typeof evt.rows === "number" ? ` (${evt.rows} rows)` : "";
      const d = formatDuration(evt.duration_ms);
      runMetrics.toolCount += 1;
      runMetrics.toolMs += Number(evt.duration_ms || 0);
      pushToolActivity(
        `${displaySnake(evt.tool)} ${evt.ok ? "done" : "failed"}${rowInfo}${d ? ` in ${d}` : ""}.`,
        false,
        evt.preview || null
      );
      return;
    }
    if (evt.type === "error") {
      pushToolActivity(`Error: ${evt.detail || evt.error || "request failed"}`);
    }
  }

  async function sendMessage() {
    const text = input.value.trim();
    if (!text) return;

    input.value = "";
    appendBubble("user", text);
    history.push({ role: "user", content: text });

    sendBtn.disabled = true;
    setStatus("Thinking…");
    clearToolActivity();
    pushToolActivity("Waiting for model…", true);

    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
        },
        body: JSON.stringify({
          messages: history,
          stream: true,
          client_context: sessionContext,
          response_style: responseStyleEl.value || "balanced",
          conversation_id: activeConversationId,
        }),
      });
      if (!res.ok || !res.body) {
        const rawText = await res.text();
        let data = {};
        try {
          data = rawText ? JSON.parse(rawText) : {};
        } catch {
          data = {};
        }
        const msg = formatErrorMessage(res.status, data, rawText);
        setModelMeta(data?.provider, data?.model);
        setStatus(msg);
        history.pop();
        thread.removeChild(thread.lastElementChild);
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";
      let finalEvent = null;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let idx;
        while ((idx = buf.indexOf("\n")) !== -1) {
          const line = buf.slice(0, idx).trim();
          buf = buf.slice(idx + 1);
          if (!line) continue;
          let evt;
          try {
            evt = JSON.parse(line);
          } catch {
            continue;
          }
          handleProgressEvent(evt);
          if (evt.type === "final") {
            finalEvent = evt;
          } else if (evt.type === "error") {
            finalEvent = { status: evt.status || 500, body: { error: evt.error, detail: evt.detail } };
          }
        }
      }

      if (!finalEvent) {
        throw new Error("No final event from chat stream.");
      }

      if (finalEvent.status < 200 || finalEvent.status >= 300) {
        const msg = formatErrorMessage(finalEvent.status, finalEvent.body || {}, "");
        setModelMeta(finalEvent.body?.provider, finalEvent.body?.model);
        setStatus(msg);
        history.pop();
        thread.removeChild(thread.lastElementChild);
        return;
      }

      const data = finalEvent.body || {};
      setModelMeta(data?.provider, data?.model);
      if (data?.conversation_id) {
        activeConversationId = String(data.conversation_id);
        sessionStorage.setItem(tabKey, activeConversationId);
      }
      sessionContext = data.context && typeof data.context === "object" ? data.context : sessionContext;
      renderContextBar();
      const reply = data.content ?? "";
      history.push({ role: "assistant", content: reply });
      const total = runMetrics.startedAt ? Date.now() - runMetrics.startedAt : 0;
      const freshness = data?.meta?.freshness_hint ? `Freshness: ${new Date(data.meta.freshness_hint).toLocaleDateString()}` : "";
      const metaText = `Latency: total ${formatDuration(total)} | model ${formatDuration(runMetrics.modelMs)} | tools ${formatDuration(runMetrics.toolMs)} (${runMetrics.toolCount})` +
        (data?.meta?.confidence ? ` | confidence ${data.meta.confidence}` : "") +
        (freshness ? ` | ${freshness}` : "");
      appendBubble("assistant", reply, metaText);
      if (currentUser) {
        await refreshConversations();
      }
      setStatus("");
    } catch (e) {
      setStatus("Network error. Check internet connection or server availability.");
      pushToolActivity(`Network error: ${e.message || String(e)}`);
      history.pop();
      thread.removeChild(thread.lastElementChild);
    } finally {
      sendBtn.disabled = false;
      input.focus();
    }
  }

  form.addEventListener("submit", (e) => {
    e.preventDefault();
    sendMessage();
  });

  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });

  newChatBtn.addEventListener("click", () => {
    startNewChat();
  });

  async function initAuth() {
    try {
      const cfgRes = await fetch("/api/auth-config", { method: "GET" });
      if (!cfgRes.ok) throw new Error("Auth config unavailable.");
      const cfg = await cfgRes.json();
      if (!cfg || !cfg.enabled) {
        canAuth = false;
        setAuthUi();
        renderHistoryList();
        return;
      }
      if (!window.supabase || !window.supabase.createClient) {
        canAuth = false;
        setAuthUi();
        renderHistoryList();
        return;
      }
      canAuth = true;
      supabase = window.supabase.createClient(cfg.url, cfg.anonKey);
    } catch (_) {
      canAuth = false;
      setAuthUi();
      renderHistoryList();
      return;
    }

    googleSignInBtn.addEventListener("click", async () => {
      const redirectTo = `${window.location.origin}/volley-chat/`;
      const { error } = await supabase.auth.signInWithOAuth({
        provider: "google",
        options: { redirectTo },
      });
      if (error) {
        setStatus(error.message || "Sign in failed.");
      }
    });

    signOutBtn.addEventListener("click", async () => {
      await supabase.auth.signOut();
      currentUser = null;
      accessToken = "";
      conversations = [];
      activeConversationId = null;
      sessionStorage.removeItem(tabKey);
      startNewChat();
      setAuthUi();
      renderHistoryList();
    });

    supabase.auth.onAuthStateChange((_event, session) => {
      currentUser = session?.user || null;
      accessToken = session?.access_token || "";
      setAuthUi();
      if (currentUser) {
        refreshConversations().then(async () => {
          if (activeConversationId) {
            try {
              await loadConversation(activeConversationId);
            } catch (_) {
              startNewChat();
            }
          }
        });
      } else {
        conversations = [];
        renderHistoryList();
      }
    });

    supabase.auth.getSession().then(({ data }) => {
      currentUser = data?.session?.user || null;
      accessToken = data?.session?.access_token || "";
      setAuthUi();
      if (currentUser) {
        refreshConversations().then(async () => {
          if (activeConversationId) {
            try {
              await loadConversation(activeConversationId);
            } catch (_) {
              startNewChat();
            }
          }
        });
      } else {
        renderHistoryList();
      }
    });
  }

  function createToolDetailsEl(t) {
    const details = document.createElement("details");
    details.className = "volley-explorer-tool";
    const summary = document.createElement("summary");
    const nameSpan = document.createElement("span");
    nameSpan.className = "volley-explorer-name";
    nameSpan.textContent = displaySnake(t.name) || "(unnamed)";
    summary.appendChild(nameSpan);
    details.appendChild(summary);
    const desc = document.createElement("p");
    desc.className = "volley-explorer-desc";
    desc.textContent = t.description || "No description.";
    details.appendChild(desc);
    const params = Array.isArray(t.parameters) ? t.parameters : [];
    if (params.length > 0) {
      const ul = document.createElement("ul");
      ul.className = "volley-explorer-params";
      params.forEach((p) => {
        const li = document.createElement("li");
        const line = document.createElement("span");
        line.className = "volley-explorer-param-meta";
        const nm = document.createElement("span");
        nm.className = "volley-explorer-param-name";
        nm.textContent = displaySnake(p.name);
        line.appendChild(nm);
        const reqLabel = p.required ? ", required" : ", optional";
        line.appendChild(document.createTextNode(` — ${p.type || "unknown"}${reqLabel}`));
        li.appendChild(line);
        if (p.description) {
          const pd = document.createElement("span");
          pd.className = "volley-explorer-param-desc";
          pd.textContent = p.description;
          li.appendChild(pd);
        }
        ul.appendChild(li);
      });
      details.appendChild(ul);
    }
    return details;
  }

  async function loadToolsExplorer() {
    if (!toolsExplorerBodyEl) return;
    toolsExplorerBodyEl.className = "volley-tools-explorer-body volley-tools-explorer-loading";
    toolsExplorerBodyEl.textContent = "Loading tool index…";
    try {
      const res = await fetch("/api/chat-tools", { method: "GET" });
      const text = await res.text();
      let data = {};
      try {
        data = text ? JSON.parse(text) : {};
      } catch {
        data = {};
      }
      if (!res.ok) {
        toolsExplorerBodyEl.className = "volley-tools-explorer-body volley-tools-explorer-error";
        toolsExplorerBodyEl.textContent =
          "Could not load the tools list. Deploy the latest code with /api/chat-tools or run the local server.";
        return;
      }
      toolsExplorerBodyEl.className = "volley-tools-explorer-body";
      toolsExplorerBodyEl.innerHTML = "";
      const sections = Array.isArray(data.sections) ? data.sections : [];
      if (sections.length > 0) {
        sections.forEach((sec) => {
          const wrap = document.createElement("section");
          wrap.className = "volley-explorer-section";
          const h = document.createElement("h3");
          h.className = "volley-explorer-section-title";
          h.textContent = displaySnake(sec.title) || "Tools";
          wrap.appendChild(h);
          const list = Array.isArray(sec.tools) ? sec.tools : [];
          list.forEach((t) => {
            wrap.appendChild(createToolDetailsEl(t));
          });
          toolsExplorerBodyEl.appendChild(wrap);
        });
      } else {
        const tools = Array.isArray(data.tools) ? data.tools : [];
        tools.forEach((t) => {
          toolsExplorerBodyEl.appendChild(createToolDetailsEl(t));
        });
      }
    } catch (e) {
      toolsExplorerBodyEl.className = "volley-tools-explorer-body volley-tools-explorer-error";
      toolsExplorerBodyEl.textContent = `Could not load tools: ${e.message || String(e)}`;
    }
  }

  initAuth();
  renderContextBar();
  loadToolsExplorer();
})();
