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

      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        const msg =
          data.error ||
          `Request failed (${res.status}). Is the site running with \`npm start\` and Ollama up?`;
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
      setStatus("Network error — is the server running?");
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
