const { Pool } = require("pg");
const { resolveDatabaseUrl } = require("./resolve-database-url");

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";
const OPENAI_BASE_URL = (process.env.OPENAI_BASE_URL || "https://api.openai.com/v1").replace(/\/$/, "");

const HF_MODEL = process.env.HF_MODEL || "meta-llama/Meta-Llama-3-8B-Instruct";
const HF_API_URL_RAW = (process.env.HF_API_URL || "https://router.huggingface.co/v1").replace(/\/$/, "");
const HF_TOKEN = process.env.HF_TOKEN || "";

const TOOL_LIMIT_MAX = 100;

const TOOL_DEFS = [
  {
    type: "function",
    function: {
      name: "top_players_by_country",
      description: "Return top FIVB beach volleyball Elo-ranked players for a country, optionally filtered by gender.",
      parameters: {
        type: "object",
        properties: {
          country_code: { type: "string", description: "3-letter country code, e.g. USA, BRA, NOR." },
          gender: { type: "string", description: "Optional gender code, often 0/1 or M/W in the source." },
          limit: { type: "integer", description: "Max rows, default 10." },
        },
        required: ["country_code"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "best_finishes_by_player",
      description: "Return the player's best tournament finishes with tournament context.",
      parameters: {
        type: "object",
        properties: {
          player_name: { type: "string", description: "Full or partial player name." },
          limit: { type: "integer", description: "Max rows, default 10." },
        },
        required: ["player_name"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "player_recent_matches",
      description: "Return recent matches for a player from match_mart.",
      parameters: {
        type: "object",
        properties: {
          player_name: { type: "string", description: "Full or partial player name." },
          limit: { type: "integer", description: "Max rows, default 10." },
        },
        required: ["player_name"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "country_matchup_record",
      description: "Head-to-head match record between two countries.",
      parameters: {
        type: "object",
        properties: {
          country_a: { type: "string", description: "First country code." },
          country_b: { type: "string", description: "Second country code." },
          since_date: { type: "string", description: "Optional yyyy-mm-dd filter." },
          limit: { type: "integer", description: "Max recent matches returned, default 20." },
        },
        required: ["country_a", "country_b"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "query_db",
      description: "Run a safe read-only SQL query against analytics tables.",
      parameters: {
        type: "object",
        properties: {
          sql: { type: "string" },
        },
        required: ["sql"],
      },
    },
  },
];

function hfChatCompletionsUrl() {
  let base = HF_API_URL_RAW;
  if (!base.endsWith("/v1")) {
    base = `${base}/v1`;
  }
  return `${base}/chat/completions`;
}

let pool;
let poolForUrl;
function activeProviderMeta() {
  if (OPENAI_API_KEY) {
    return { provider: "ChatGPT", model: OPENAI_MODEL };
  }
  return { provider: "Hugging Face", model: HF_MODEL };
}

function normalizeUrlForNodePg(url) {
  if (!url || typeof url !== "string") return url;
  let raw = url.trim();
  if (raw.startsWith("postgres://")) {
    raw = `postgresql://${raw.slice("postgres://".length)}`;
  }
  try {
    const parsed = new URL(raw);
    // Let pg SSL options below control TLS behavior; dashboard-provided URI params
    // can force cert validation modes that fail in serverless runtimes.
    ["sslmode", "sslrootcert", "sslcert", "sslkey", "sslcrl"].forEach((k) => {
      parsed.searchParams.delete(k);
    });
    return parsed.toString();
  } catch {
    return raw;
  }
}

function sslConfigForUrl(url) {
  if (process.env.PGSSLMODE === "disable") return false;
  const host = (() => {
    try {
      return new URL(url).hostname || "";
    } catch {
      return "";
    }
  })();
  if (host === "localhost" || host === "127.0.0.1") return false;
  return {
    rejectUnauthorized: false,
    checkServerIdentity: () => undefined,
  };
}

function getPool() {
  const url = normalizeUrlForNodePg(resolveDatabaseUrl());
  if (!url) return null;
  if (!pool || poolForUrl !== url) {
    poolForUrl = url;
    pool = new Pool({
      connectionString: url,
      max: 3,
      idleTimeoutMillis: 10000,
      connectionTimeoutMillis: 5000,
      ssl: sslConfigForUrl(url),
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

function clampLimit(v, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n) || n < 1) return fallback;
  return Math.min(Math.floor(n), TOOL_LIMIT_MAX);
}

function emitProgress(onProgress, event) {
  if (typeof onProgress !== "function") return;
  try {
    onProgress({ ts: Date.now(), ...event });
  } catch (_) {
    /* ignore progress callback errors */
  }
}

function normalizeGenderFilter(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return null;
  if (["m", "male", "men", "man", "0"].includes(raw)) return ["0", "m", "male", "men"];
  if (["w", "f", "female", "women", "woman", "1"].includes(raw)) {
    return ["1", "w", "f", "female", "women"];
  }
  return [raw];
}

function isSafeReadOnlySql(sql) {
  const s = String(sql || "").trim().toLowerCase();
  if (!(s.startsWith("select") || s.startsWith("with"))) return false;
  const banned = ["insert ", "update ", "delete ", "drop ", "alter ", "truncate ", "grant ", "revoke "];
  return !banned.some((kw) => s.includes(kw));
}

async function queryRows(sql, params) {
  const db = getPool();
  if (!db) return { error: "DATABASE_URL is not set." };
  try {
    const result = await db.query(sql, params);
    return result.rows.slice(0, 200);
  } catch (err) {
    const msg = (err && err.message ? String(err.message) : "Database query failed.");
    if (/certificate|self signed|unable to verify/i.test(msg)) {
      return {
        error:
          "Database TLS/certificate verification failed. Check DATABASE_URL host/port and SSL settings; Supabase URIs should use sslmode=require.",
        detail: msg,
      };
    }
    return { error: err.message || "Database query failed." };
  }
}

async function runTool(name, args, onProgress) {
  emitProgress(onProgress, { type: "tool_start", tool: name, args });
  if (name === "top_players_by_country") {
    const country = String(args.country_code || "").trim().toUpperCase();
    if (!country) return { error: "country_code is required." };
    const limit = clampLimit(args.limit, 10);
    const params = [country];
    let sql = `
      SELECT
        player_id,
        player_name,
        player_country_code,
        gender,
        ROUND(elo_rating::numeric, 1) AS elo_rating,
        matches_played,
        wins,
        losses,
        last_match_played_at,
        last_match_tournament_name
      FROM mart.player_elo_latest
      WHERE player_country_code = $1
    `;
    const genderFilter = normalizeGenderFilter(args.gender);
    if (genderFilter) {
      params.push(genderFilter);
      sql += ` AND lower(coalesce(gender::text, '')) = ANY($${params.length}::text[])`;
    }
    params.push(limit);
    sql += ` ORDER BY elo_rating DESC NULLS LAST LIMIT $${params.length}`;
    const out = await queryRows(sql, params);
    emitProgress(onProgress, { type: "tool_done", tool: name, ok: !out?.error, rows: Array.isArray(out) ? out.length : null });
    return out;
  }

  if (name === "best_finishes_by_player") {
    const player = String(args.player_name || "").trim();
    if (!player) return { error: "player_name is required." };
    const limit = clampLimit(args.limit, 10);
    const out = await queryRows(
      `
      WITH ids AS (
        SELECT DISTINCT player_id, full_name
        FROM staging.stg_fivb_players
        WHERE full_name ILIKE $1
        ORDER BY full_name
        LIMIT 10
      )
      SELECT
        i.player_id,
        i.full_name AS player_name,
        s.tournament_id,
        t.name AS tournament_name,
        t.season AS tournament_season,
        s.finishing_pos,
        s.points,
        s.prize_money
      FROM ids i
      JOIN core.dim_team_tournaments d
        ON d.player_a_id = i.player_id OR d.player_b_id = i.player_id
      JOIN core.fct_tournament_standings s
        ON s.tournament_id = d.tournament_id AND s.team_id = d.team_id
      LEFT JOIN core.dim_tournaments t
        ON t.tournament_id = s.tournament_id
      ORDER BY i.full_name, s.finishing_pos ASC NULLS LAST, t.season DESC NULLS LAST
      LIMIT $2
      `,
      [`%${player}%`, limit]
    );
    emitProgress(onProgress, { type: "tool_done", tool: name, ok: !out?.error, rows: Array.isArray(out) ? out.length : null });
    return out;
  }

  if (name === "player_recent_matches") {
    const player = String(args.player_name || "").trim();
    if (!player) return { error: "player_name is required." };
    const limit = clampLimit(args.limit, 10);
    const out = await queryRows(
      `
      WITH ids AS (
        SELECT DISTINCT player_id, full_name
        FROM staging.stg_fivb_players
        WHERE full_name ILIKE $1
        ORDER BY full_name
        LIMIT 5
      )
      SELECT
        i.player_id,
        i.full_name AS player_name,
        m.match_id,
        m.match_date,
        m.tournament_name,
        m.season AS tournament_season,
        m.round_name,
        m.team1_display_name,
        m.team2_display_name,
        m.score_sets,
        CASE WHEN m.winner_team_id = m.team1_id THEN m.team1_display_name
             WHEN m.winner_team_id = m.team2_id THEN m.team2_display_name
             ELSE NULL
        END AS winner_team
      FROM ids i
      JOIN mart.match_mart m
        ON i.player_id IN (
          m.team1_player_a_id,
          m.team1_player_b_id,
          m.team2_player_a_id,
          m.team2_player_b_id
        )
      ORDER BY m.match_date DESC NULLS LAST, m.match_id DESC
      LIMIT $2
      `,
      [`%${player}%`, limit]
    );
    emitProgress(onProgress, { type: "tool_done", tool: name, ok: !out?.error, rows: Array.isArray(out) ? out.length : null });
    return out;
  }

  if (name === "country_matchup_record") {
    const a = String(args.country_a || "").trim().toUpperCase();
    const b = String(args.country_b || "").trim().toUpperCase();
    if (!a || !b) return { error: "country_a and country_b are required." };
    const limit = clampLimit(args.limit, 20);
    const since = args.since_date ? String(args.since_date).trim() : null;
    const rows = await queryRows(
      `
      SELECT
        m.match_id,
        m.match_date,
        m.tournament_name,
        m.season AS tournament_season,
        m.team1_country_code,
        m.team2_country_code,
        m.team1_display_name,
        m.team2_display_name,
        m.score_sets,
        CASE WHEN m.winner_team_id = m.team1_id THEN m.team1_country_code
             WHEN m.winner_team_id = m.team2_id THEN m.team2_country_code
             ELSE NULL END AS winner_country
      FROM mart.match_mart m
      WHERE (
        (m.team1_country_code = $1 AND m.team2_country_code = $2)
        OR
        (m.team1_country_code = $2 AND m.team2_country_code = $1)
      )
      AND ($3::date IS NULL OR m.match_date >= $3::date)
      ORDER BY m.match_date DESC NULLS LAST, m.match_id DESC
      LIMIT $4
      `,
      [a, b, since, limit]
    );
    if (!Array.isArray(rows)) {
      emitProgress(onProgress, { type: "tool_done", tool: name, ok: false, rows: null });
      return rows;
    }
    const summary = rows.reduce(
      (acc, r) => {
        if (r.winner_country === a) acc.country_a_wins += 1;
        else if (r.winner_country === b) acc.country_b_wins += 1;
        return acc;
      },
      { country_a: a, country_b: b, country_a_wins: 0, country_b_wins: 0, sample_matches: rows.length }
    );
    const out = { summary, matches: rows };
    emitProgress(onProgress, { type: "tool_done", tool: name, ok: true, rows: rows.length });
    return out;
  }

  if (name === "query_db") {
    const sql = String(args.sql || "");
    if (!isSafeReadOnlySql(sql)) return { error: "Only read-only SELECT/CTE SQL is allowed." };
    const out = await queryRows(sql, []);
    emitProgress(onProgress, { type: "tool_done", tool: name, ok: !out?.error, rows: Array.isArray(out) ? out.length : null });
    return out;
  }

  emitProgress(onProgress, { type: "tool_done", tool: name, ok: false, rows: null });
  return { error: `Unknown tool: ${name}` };
}

async function callOpenAiCompat({ url, token, model, messages, tools, toolChoice }) {
  const payload = {
    model,
    messages,
    temperature: 0.5,
    max_tokens: 600,
  };
  if (tools) payload.tools = tools;
  if (toolChoice) payload.tool_choice = toolChoice;
  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  const raw = await res.text();
  if (!res.ok) throw new Error(`Model API error ${res.status}: ${raw.slice(0, 600)}`);
  try {
    return JSON.parse(raw);
  } catch {
    throw new Error(`Invalid model response JSON: ${raw.slice(0, 300)}`);
  }
}

async function generateWithOpenAiTools(clean, onProgress) {
  const meta = activeProviderMeta();
  const system = {
    role: "system",
    content:
      "You are Volley Chat for FIVB beach volleyball analytics only. " +
      "Treat ranking questions like 'top USA men by Elo' as volleyball data questions and call tools for factual answers. " +
      "Prefer tool calls for stats/facts and keep answers concise. " +
      "If a tool returns ambiguous players, ask a quick clarification question.",
  };
  const msgs = [system, ...clean];
  emitProgress(onProgress, { type: "model_start", stage: "plan_tools", provider: meta.provider, model: meta.model });
  const first = await callOpenAiCompat({
    url: `${OPENAI_BASE_URL}/chat/completions`,
    token: OPENAI_API_KEY,
    model: OPENAI_MODEL,
    messages: msgs,
    tools: TOOL_DEFS,
    toolChoice: "auto",
  });
  emitProgress(onProgress, { type: "model_done", stage: "plan_tools" });

  const choice = first.choices?.[0]?.message || {};
  const toolCalls = Array.isArray(choice.tool_calls) ? choice.tool_calls : [];
  if (toolCalls.length === 0) {
    return { status: 200, body: { role: "assistant", content: choice.content || "", ...meta } };
  }

  msgs.push({
    role: "assistant",
    content: choice.content || "",
    tool_calls: toolCalls,
  });

  for (const tc of toolCalls) {
    let args = {};
    try {
      args = JSON.parse(tc.function?.arguments || "{}");
    } catch {
      args = {};
    }
    const result = await runTool(tc.function?.name, args, onProgress);
    msgs.push({
      role: "tool",
      tool_call_id: tc.id,
      content: JSON.stringify(result),
    });
  }

  emitProgress(onProgress, { type: "model_start", stage: "final_answer", provider: meta.provider, model: meta.model });
  const second = await callOpenAiCompat({
    url: `${OPENAI_BASE_URL}/chat/completions`,
    token: OPENAI_API_KEY,
    model: OPENAI_MODEL,
    messages: msgs,
    tools: TOOL_DEFS,
    toolChoice: "none",
  });
  emitProgress(onProgress, { type: "model_done", stage: "final_answer" });
  const finalText = second.choices?.[0]?.message?.content || "I could not generate a response.";
  return { status: 200, body: { role: "assistant", content: finalText, tool: "structured_tools", ...meta } };
}

function buildHfPrompt(messages, toolResult) {
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

function extractHfToolCall(text) {
  const start = text.indexOf("{");
  const end = text.lastIndexOf("}");
  if (start === -1 || end === -1 || end <= start) return null;
  try {
    const parsed = JSON.parse(text.slice(start, end + 1));
    if (parsed.tool === "query_db" && typeof parsed.sql === "string") return parsed;
  } catch {
    return null;
  }
  return null;
}

async function callHf(prompt) {
  if (!HF_TOKEN) throw new Error("HF_TOKEN is missing.");
  const data = await callOpenAiCompat({
    url: hfChatCompletionsUrl(),
    token: HF_TOKEN,
    model: HF_MODEL,
    messages: [{ role: "user", content: prompt }],
  });
  const text = data.choices?.[0]?.message?.content;
  if (!text || typeof text !== "string") throw new Error("HF returned empty text.");
  return text.trim();
}

async function generateWithHfFallback(clean, onProgress) {
  const meta = activeProviderMeta();
  emitProgress(onProgress, { type: "model_start", stage: "hf_first_pass", provider: meta.provider, model: meta.model });
  const first = await callHf(buildHfPrompt(clean));
  emitProgress(onProgress, { type: "model_done", stage: "hf_first_pass" });
  const toolCall = extractHfToolCall(first);
  if (!toolCall) return { status: 200, body: { role: "assistant", content: first, ...meta } };
  const rows = await runTool("query_db", { sql: toolCall.sql }, onProgress);
  emitProgress(onProgress, { type: "model_start", stage: "hf_second_pass", provider: meta.provider, model: meta.model });
  const second = await callHf(buildHfPrompt(clean, rows));
  emitProgress(onProgress, { type: "model_done", stage: "hf_second_pass" });
  return { status: 200, body: { role: "assistant", content: second, tool: "query_db", ...meta } };
}

async function generateChatReply(messages, options = {}) {
  const onProgress = options.onProgress;
  const clean = sanitizeMessages(messages);
  if (clean.length === 0) {
    return { status: 400, body: { error: "Expected a non-empty messages array.", ...activeProviderMeta() } };
  }
  if (OPENAI_API_KEY) return generateWithOpenAiTools(clean, onProgress);
  return generateWithHfFallback(clean, onProgress);
}

module.exports = { generateChatReply };
