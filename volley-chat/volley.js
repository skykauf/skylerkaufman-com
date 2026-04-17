(() => {
  const thread = document.getElementById("thread");
  const form = document.getElementById("composer");
  const input = document.getElementById("input");
  const sendBtn = document.getElementById("send");
  const statusEl = document.getElementById("status");
  const modelMetaEl = document.getElementById("modelMeta");
  const toolActivityEl = document.getElementById("toolActivity");

  /** @type {{ role: string, content: string }[]} */
  const history = [];
  let modelLabel = "Unknown";

  function setStatus(text) {
    statusEl.textContent = text || "";
  }

  function setModelMeta(provider, model) {
    if (!provider || !model) return;
    modelLabel = `${provider} · ${model}`;
    modelMetaEl.textContent = modelLabel;
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

  function appendBubble(role, content) {
    const div = document.createElement("div");
    div.className = `volley-msg ${role}`;
    const label = document.createElement("div");
    label.className = "volley-role";
    label.textContent = role === "user" ? "You" : modelLabel;
    const body = document.createElement("div");
    body.textContent = content;
    div.appendChild(label);
    div.appendChild(body);
    thread.appendChild(div);
    thread.scrollTop = thread.scrollHeight;
  }

  function clearToolActivity() {
    toolActivityEl.innerHTML = "";
  }

  function pushToolActivity(text, active = false) {
    const line = document.createElement("div");
    line.className = `volley-tool-line${active ? " active" : ""}`;
    line.textContent = text;
    toolActivityEl.appendChild(line);
    toolActivityEl.scrollTop = toolActivityEl.scrollHeight;
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
      return;
    }
    if (evt.type === "model_start") {
      const modelName = evt.model ? ` (${evt.model})` : "";
      pushToolActivity(`${stageLabel(evt.stage)}${modelName}…`, true);
      return;
    }
    if (evt.type === "model_done") {
      const d = formatDuration(evt.duration_ms);
      pushToolActivity(`${stageLabel(evt.stage)} done${d ? ` in ${d}` : ""}.`);
      return;
    }
    if (evt.type === "tool_start") {
      pushToolActivity(`Running tool: ${evt.tool}`, true);
      return;
    }
    if (evt.type === "tool_done") {
      const rowInfo = typeof evt.rows === "number" ? ` (${evt.rows} rows)` : "";
      const d = formatDuration(evt.duration_ms);
      pushToolActivity(`${evt.tool} ${evt.ok ? "done" : "failed"}${rowInfo}${d ? ` in ${d}` : ""}.`);
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
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messages: history, stream: true }),
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
      const reply = data.content ?? "";
      history.push({ role: "assistant", content: reply });
      appendBubble("assistant", reply);
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
})();
