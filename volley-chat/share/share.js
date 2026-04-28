(() => {
  const titleEl = document.getElementById("sharedTitle");
  const statusEl = document.getElementById("status");
  const threadEl = document.getElementById("thread");
  const formEl = document.getElementById("composer");
  const inputEl = document.getElementById("input");
  const sendBtnEl = document.getElementById("send");
  const history = [];

  function setStatus(text) {
    statusEl.textContent = text || "";
  }

  function appendBubble(role, content) {
    const div = document.createElement("div");
    div.className = `volley-msg ${role === "user" ? "user" : "assistant"}`;
    const label = document.createElement("div");
    label.className = "volley-role";
    label.textContent = role === "user" ? "User" : "Assistant";
    const body = document.createElement("div");
    body.textContent = String(content || "");
    div.appendChild(label);
    div.appendChild(body);
    threadEl.appendChild(div);
    threadEl.scrollTop = threadEl.scrollHeight;
  }

  async function sendMessage() {
    const text = String(inputEl.value || "").trim();
    if (!text) return;
    inputEl.value = "";
    history.push({ role: "user", content: text });
    appendBubble("user", text);
    setStatus("Thinking…");
    sendBtnEl.disabled = true;
    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messages: history }),
      });
      const raw = await res.text();
      let data = {};
      try {
        data = raw ? JSON.parse(raw) : {};
      } catch {
        data = {};
      }
      if (!res.ok) {
        throw new Error(data.error || `Request failed (${res.status})`);
      }
      const reply = String(data.content || "");
      history.push({ role: "assistant", content: reply });
      appendBubble("assistant", reply);
      setStatus("");
    } catch (err) {
      history.pop();
      if (threadEl.lastElementChild) threadEl.removeChild(threadEl.lastElementChild);
      setStatus(err.message || "Could not continue conversation.");
    } finally {
      sendBtnEl.disabled = false;
      inputEl.focus();
    }
  }

  async function loadSharedConversation() {
    const params = new URLSearchParams(window.location.search || "");
    const token = params.get("token") || "";
    if (!token) {
      setStatus("Missing share token.");
      return;
    }
    setStatus("Loading shared chat…");
    try {
      const res = await fetch(`/api/chat-history/public/${encodeURIComponent(token)}`, { method: "GET" });
      const text = await res.text();
      let data = {};
      try {
        data = text ? JSON.parse(text) : {};
      } catch {
        data = {};
      }
      if (!res.ok) {
        setStatus(data.error || "Could not load shared conversation.");
        return;
      }
      titleEl.textContent = data?.conversation?.title ? `Title: ${data.conversation.title}` : "Shared chat";
      threadEl.innerHTML = "";
      const messages = Array.isArray(data.messages) ? data.messages : [];
      history.length = 0;
      messages.forEach((m) => {
        if (!m || typeof m.content !== "string") return;
        if (m.role === "user" || m.role === "assistant") {
          history.push({ role: m.role, content: m.content });
        }
        appendBubble(m.role, m.content);
      });
      setStatus(messages.length > 0 ? "" : "This shared conversation has no messages.");
    } catch (err) {
      setStatus(err.message || "Could not load shared conversation.");
    }
  }

  formEl.addEventListener("submit", (e) => {
    e.preventDefault();
    sendMessage();
  });

  inputEl.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  });

  loadSharedConversation();
})();
