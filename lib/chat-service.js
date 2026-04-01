const { Pool } = require("pg");

const HF_MODEL = process.env.HF_MODEL || "meta-llama/Meta-Llama-3-8B-Instruct";
const HF_API_URL = (process.env.HF_API_URL || "https://router.huggingface.co").replace(/\/$/, "");
const HF_TOKEN = process.env.HF_TOKEN || "";

let pool;
function getPool() {
  if (!process.env.DATABASE_URL) return null;
  if (!pool) {
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      max: 3,
      idleTimeoutMillis: 10000,
      connectionTimeoutMillis: 5000,
      ssl: process.env.PGSSLMODE === "disable" ? false : { rejectUnauthorized: false },
    });
  }
  return pool;
}

function sanitizeMessages(messages) {
  if (!Array.isArray(messages) || messages.length === 0) return [];
  return messages
    .slice(-24)
    .map((m) => {
      if (!m || typeof m.content !== "string") return null;
      const role = m.role === "assistant" ? "assistant" : "user";
      return { role, content: m.content.slice(0, 8000) };
    })
    .filter(Boolean);
}

function buildPrompt(messages, toolResult) {
  const intro = [
    "You are Volley Chat, a helpful assistant.",
    "When database access is needed, reply ONLY with compact JSON:",
    '{"tool":"query_db","sql":"SELECT ..."}',
    "Use read-only SQL only (SELECT/CTE).",
    "If no tool is needed, answer normally.",
  ].join("\n");

  const convo = messages
    .map((m) => `${m.role === "assistant" ? "Assistant" : "User"}: ${m.content}`)
    .join("\n");

  const toolContext = toolResult
    ? `\nTool result (JSON rows): ${JSON.stringify(toolResult).slice(0, 6000)}\nNow answer the user.`
    : "";

  return `${intro}\n${convo}${toolContext}\nAssistant:`;
}

function extractToolCall(text) {
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start === -1 || end === -1 || end <= start) return null;
  try {
    const parsed = JSON.parse(text.slice(start, end + 1));
    if (parsed.tool === "query_db" && typeof parsed.sql === "string") {
      return parsed;
    }
  } catch {
    return null;
  }
  return null;
}

function isSafeReadOnlySql(sql) {
  const s = sql.trim().toLowerCase();
  if (!(s.startsWith("select") || s.startsWith("with"))) return false;
  const banned = ["insert ", "update ", "delete ", "drop ", "alter ", "truncate ", "grant ", "revoke "];
  return !banned.some((kw) => s.includes(kw));
}

async function callHf(prompt) {
  if (!HF_TOKEN) {
    throw new Error("HF_TOKEN is missing.");
  }
  const res = await fetch(`${HF_API_URL}/models/${HF_MODEL}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${HF_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      inputs: prompt,
      parameters: {
        max_new_tokens: 350,
        temperature: 0.7,
        return_full_text: false,
      },
    }),
  });

  const raw = await res.text();
  if (!res.ok) throw new Error(`HF error ${res.status}: ${raw.slice(0, 400)}`);

  let data;
  try {
    data = JSON.parse(raw);
  } catch {
    throw new Error(`Invalid HF response: ${raw.slice(0, 200)}`);
  }
  const text = Array.isArray(data) ? data[0]?.generated_text : data?.generated_text;
  if (!text || typeof text !== "string") throw new Error("HF returned empty text.");
  return text.trim();
}

async function maybeRunQuery(sql) {
  const db = getPool();
  if (!db) return { error: "DATABASE_URL is not set." };
  if (!isSafeReadOnlySql(sql)) return { error: "Only read-only SELECT/CTE queries are allowed." };
  try {
    const result = await db.query(sql);
    return result.rows.slice(0, 200);
  } catch (err) {
    return { error: err.message || "Database query failed." };
  }
}

async function generateChatReply(messages) {
  const clean = sanitizeMessages(messages);
  if (clean.length === 0) {
    return { status: 400, body: { error: "Expected a non-empty messages array." } };
  }

  const first = await callHf(buildPrompt(clean));
  const toolCall = extractToolCall(first);
  if (!toolCall) {
    return { status: 200, body: { role: "assistant", content: first } };
  }

  const rows = await maybeRunQuery(toolCall.sql);
  const second = await callHf(buildPrompt(clean, rows));
  return { status: 200, body: { role: "assistant", content: second, tool: "query_db" } };
}

module.exports = { generateChatReply };
