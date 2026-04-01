(() => {
  const thread = document.getElementById("thread");
  const form = document.getElementById("composer");
  const input = document.getElementById("input");
  const sendBtn = document.getElementById("send");
  const statusEl = document.getElementById("status");

  /** @type {{ role: string, content: string }[]} */
  const history = [];

  function setStatus(text) {
    statusEl.textContent = text || "";
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
    label.textContent = role === "user" ? "You" : "Local model";
    const body = document.createElement("div");
    body.textContent = content;
    div.appendChild(label);
    div.appendChild(body);
    thread.appendChild(div);
    thread.scrollTop = thread.scrollHeight;
  }

  async function sendMessage() {
    const text = input.value.trim();
    if (!text) return;

    input.value = "";
    appendBubble("user", text);
    history.push({ role: "user", content: text });

    sendBtn.disabled = true;
    setStatus("Thinking…");

    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messages: history }),
      });

      const rawText = await res.text();
      let data = {};
      try {
        data = rawText ? JSON.parse(rawText) : {};
      } catch {
        data = {};
      }

      if (!res.ok) {
        const msg = formatErrorMessage(res.status, data, rawText);
        setStatus(msg);
        history.pop();
        thread.removeChild(thread.lastElementChild);
        return;
      }

      const reply = data.content ?? "";
      history.push({ role: "assistant", content: reply });
      appendBubble("assistant", reply);
      setStatus("");
    } catch (e) {
      setStatus("Network error. Check internet connection or server availability.");
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
