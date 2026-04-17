const { Pool } = require("pg");
const { resolveDatabaseUrl } = require("./resolve-database-url");

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4o-mini";
const OPENAI_BASE_URL = (process.env.OPENAI_BASE_URL || "https://api.openai.com/v1").replace(/\/$/, "");

const HF_MODEL = process.env.HF_MODEL || "meta-llama/Meta-Llama-3-8B-Instruct";
const HF_API_URL_RAW = (process.env.HF_API_URL || "https://router.huggingface.co/v1").replace(/\/$/, "");
const HF_TOKEN = process.env.HF_TOKEN || "";

const TOOL_LIMIT_MAX = 100;
const ACTIVE_PLAYER_DAYS = 365;

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
          country_b: { type: "string", description: "Second country code for direct head-to-head." },
          gender: { type: "string", description: "Optional gender filter (men/women or M/W/0/1)." },
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
      name: "country_opponent_performance",
      description:
        "Rank one country's performance versus all opponent countries by win rate; use for questions like 'who has USA performed best against'.",
      parameters: {
        type: "object",
        properties: {
          country_code: { type: "string", description: "Target country code (3-letter)." },
          gender: { type: "string", description: "Optional gender filter (men/women or M/W/0/1)." },
          since_date: { type: "string", description: "Optional yyyy-mm-dd filter." },
          min_matches: { type: "integer", description: "Optional minimum matches per opponent, default 3." },
          sort_by: {
            type: "string",
            description: "Sort mode: best (default) for highest win rate, worst for lowest win rate.",
          },
          limit: { type: "integer", description: "Max opponent countries returned, default 20." },
        },
        required: ["country_code"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "active_players",
      description:
        "Return active FIVB beach volleyball players who played in the last 365 days, optionally filtered by country/gender.",
      parameters: {
        type: "object",
        properties: {
          country_code: { type: "string", description: "Optional 3-letter country code (USA, BRA, NOR)." },
          gender: { type: "string", description: "Optional gender filter (men/women or M/W/0/1)." },
          limit: { type: "integer", description: "Max rows, default 25." },
        },
      },
    },
  },
  {
    type: "function",
    function: {
      name: "inactive_players",
      description:
        "Return inactive FIVB beach volleyball players whose last match was more than 365 days ago.",
      parameters: {
        type: "object",
        properties: {
          country_code: { type: "string", description: "Optional 3-letter country code (USA, BRA, NOR)." },
          gender: { type: "string", description: "Optional gender filter (men/women or M/W/0/1)." },
          limit: { type: "integer", description: "Max rows, default 25." },
        },
      },
    },
  },
  {
    type: "function",
    function: {
      name: "find_player",
      description: "Find likely player matches by name with optional country filter.",
      parameters: {
        type: "object",
        properties: {
          name_query: { type: "string", description: "Full or partial player name." },
          country_code: { type: "string", description: "Optional 3-letter country code." },
          limit: { type: "integer", description: "Max rows, default 10." },
        },
        required: ["name_query"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "player_profile",
      description: "Get a player profile snapshot (Elo, W/L, last tournament) by player id or name.",
      parameters: {
        type: "object",
        properties: {
          player_id: { type: "integer", description: "Preferred unique player id." },
          player_name: { type: "string", description: "Fallback full or partial name." },
        },
      },
    },
  },
  {
    type: "function",
    function: {
      name: "full_player_profile",
      description:
        "Get a complete player profile including bio fields, form snapshot, teammate history, similar players, and recent matches.",
      parameters: {
        type: "object",
        properties: {
          player_id: { type: "integer", description: "Preferred unique player id." },
          player_name: { type: "string", description: "Fallback full or partial name." },
          teammate_limit: { type: "integer", description: "Max teammates returned, default 8." },
          similar_limit: { type: "integer", description: "Max similar players returned, default 8." },
          recent_matches_limit: { type: "integer", description: "Max recent matches returned, default 8." },
        },
      },
    },
  },
  {
    type: "function",
    function: {
      name: "head_to_head_players",
      description: "Head-to-head history between two players.",
      parameters: {
        type: "object",
        properties: {
          player_a_id: { type: "integer" },
          player_b_id: { type: "integer" },
          limit: { type: "integer", description: "Max recent meetings, default 20." },
        },
        required: ["player_a_id", "player_b_id"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "player_teammate_history",
      description: "Show teammate history for a player with match counts and outcomes.",
      parameters: {
        type: "object",
        properties: {
          player_id: { type: "integer" },
          limit: { type: "integer", description: "Max teammates returned, default 20." },
        },
        required: ["player_id"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "most_improved_players",
      description: "Players with the largest Elo increase over the last 365 days.",
      parameters: {
        type: "object",
        properties: {
          country_code: { type: "string", description: "Optional 3-letter country code." },
          gender: { type: "string", description: "Optional gender filter (men/women or M/W/0/1)." },
          limit: { type: "integer", description: "Max rows, default 20." },
        },
      },
    },
  },
  {
    type: "function",
    function: {
      name: "country_depth_report",
      description: "Country depth snapshot: active counts, top Elo, median Elo, and avg wins.",
      parameters: {
        type: "object",
        properties: {
          country_code: { type: "string", description: "3-letter country code." },
          gender: { type: "string", description: "Optional gender filter (men/women or M/W/0/1)." },
          top_n: { type: "integer", description: "How many top players to include, default 10." },
        },
        required: ["country_code"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "player_status",
      description: "Return a compact status snapshot for a player (active/inactive, Elo, last event).",
      parameters: {
        type: "object",
        properties: {
          player_id: { type: "integer", description: "Preferred unique player id." },
          player_name: { type: "string", description: "Fallback full or partial name." },
        },
      },
    },
  },
  {
    type: "function",
    function: {
      name: "tournament_lookup",
      description: "Find tournaments by fuzzy name or location to get candidate tournament ids.",
      parameters: {
        type: "object",
        properties: {
          query: { type: "string", description: "Tournament name/city/country search text." },
          limit: { type: "integer", description: "Max rows, default 10." },
        },
        required: ["query"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "tournament_snapshot",
      description: "Tournament snapshot with participant strength and standings highlights.",
      parameters: {
        type: "object",
        properties: {
          tournament_id: { type: "integer", description: "Tournament id from lookup." },
        },
        required: ["tournament_id"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "partnership_profile",
      description: "Profile a partnership between two players: matches together, win rate, last event.",
      parameters: {
        type: "object",
        properties: {
          player_a_id: { type: "integer" },
          player_b_id: { type: "integer" },
          limit: { type: "integer", description: "Max recent matches listed, default 20." },
        },
        required: ["player_a_id", "player_b_id"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "similar_players",
      description: "Find players similar to a target by Elo band, activity, and country/gender context.",
      parameters: {
        type: "object",
        properties: {
          player_id: { type: "integer" },
          country_code: { type: "string", description: "Optional country filter override." },
          limit: { type: "integer", description: "Max rows, default 10." },
        },
        required: ["player_id"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "explain_ranking",
      description: "Explain why one player ranks above another based on Elo and activity context.",
      parameters: {
        type: "object",
        properties: {
          player_a_id: { type: "integer" },
          player_b_id: { type: "integer" },
        },
        required: ["player_a_id", "player_b_id"],
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

function inferFreshnessFromRows(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return null;
  const candidateKeys = ["last_match_played_at", "match_date", "latest_date", "as_of_date", "tournament_end_date"];
  let best = null;
  for (const r of rows) {
    if (!r || typeof r !== "object") continue;
    for (const k of candidateKeys) {
      const v = r[k];
      if (!v) continue;
      const t = Date.parse(v);
      if (Number.isFinite(t) && (!best || t > best)) best = t;
    }
  }
  return best ? new Date(best).toISOString() : null;
}

function summarizeToolOutput(out) {
  if (Array.isArray(out)) {
    return {
      ok: true,
      rows: out.length,
      preview: out.slice(0, 5),
      freshness_hint: inferFreshnessFromRows(out),
    };
  }
  if (out && typeof out === "object" && Array.isArray(out.matches)) {
    return {
      ok: !out.error,
      rows: out.matches.length,
      preview: out.matches.slice(0, 5),
      freshness_hint: inferFreshnessFromRows(out.matches),
    };
  }
  if (out && typeof out === "object" && Array.isArray(out.top_players)) {
    return {
      ok: !out.error,
      rows: out.top_players.length,
      preview: out.top_players.slice(0, 5),
      freshness_hint: inferFreshnessFromRows(out.top_players),
    };
  }
  return {
    ok: !(out && out.error),
    rows: null,
    preview: out && typeof out === "object" ? out : null,
    freshness_hint: null,
  };
}

function mergeContext(prev, nextPatch) {
  return {
    country_code: nextPatch.country_code || prev.country_code || null,
    player_id: nextPatch.player_id || prev.player_id || null,
    player_name: nextPatch.player_name || prev.player_name || null,
    tournament_id: nextPatch.tournament_id || prev.tournament_id || null,
    gender: nextPatch.gender || prev.gender || null,
  };
}

function updateContextFromTool(prev, name, args, out) {
  let patch = {};
  if (name === "top_players_by_country" || name === "active_players" || name === "inactive_players" || name === "country_depth_report") {
    patch.country_code = args.country_code ? String(args.country_code).toUpperCase() : null;
    patch.gender = args.gender ? String(args.gender) : null;
  }
  if (
    name === "player_profile" ||
    name === "full_player_profile" ||
    name === "player_status" ||
    name === "player_teammate_history" ||
    name === "similar_players"
  ) {
    if (args.player_id) patch.player_id = Number(args.player_id);
  }
  if (name === "tournament_snapshot" && args.tournament_id) {
    patch.tournament_id = Number(args.tournament_id);
  }
  if (Array.isArray(out) && out[0]) {
    const first = out[0];
    if (!patch.player_id && first.player_id) patch.player_id = Number(first.player_id);
    if (!patch.player_name && first.player_name) patch.player_name = String(first.player_name);
    if (!patch.country_code && first.player_country_code) patch.country_code = String(first.player_country_code).toUpperCase();
    if (!patch.gender && first.gender) patch.gender = String(first.gender);
    if (!patch.tournament_id && first.tournament_id) patch.tournament_id = Number(first.tournament_id);
  }
  if (out && typeof out === "object" && out.summary && typeof out.summary === "object") {
    if (!patch.country_code && out.summary.country_a) patch.country_code = String(out.summary.country_a).toUpperCase();
    if (!patch.tournament_id && out.summary.tournament_id) patch.tournament_id = Number(out.summary.tournament_id);
  }
  return mergeContext(prev, patch);
}

function normalizeGenderFilter(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return null;
  const compact = raw.replace(/[^a-z0-9]/g, "");
  if (!compact) return null;
  if (
    [
      "0",
      "m",
      "male",
      "males",
      "man",
      "mans",
      "men",
      "mens",
      "boy",
      "boys",
      "guy",
      "guys",
      "masculine",
    ].includes(compact)
  ) {
    return ["0", "m", "male", "men"];
  }
  if (
    [
      "1",
      "f",
      "w",
      "female",
      "females",
      "woman",
      "womans",
      "women",
      "womens",
      "girl",
      "girls",
      "lady",
      "ladies",
      "feminine",
    ].includes(compact)
  ) {
    return ["1", "w", "f", "female", "women"];
  }
  return [compact];
}

function normalizeCountryCodeCandidates(value) {
  const raw = String(value || "").trim().toUpperCase();
  if (!raw) return [];
  const out = [raw];
  if (raw.length === 3) out.push(raw.slice(0, 2));
  return [...new Set(out)];
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
  const startedAt = Date.now();
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
    const summary = summarizeToolOutput(out);
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summary.ok,
      rows: summary.rows,
      preview: summary.preview,
      freshness_hint: summary.freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "best_finishes_by_player") {
    const player = String(args.player_name || "").trim();
    if (!player) return { error: "player_name is required." };
    const limit = clampLimit(args.limit, 10);
    const out = await queryRows(
      `
      WITH ids AS (
        SELECT DISTINCT p.player_id, p.full_name
        FROM staging.stg_fivb_players p
        WHERE p.full_name ILIKE $1
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
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "player_recent_matches") {
    const player = String(args.player_name || "").trim();
    if (!player) return { error: "player_name is required." };
    const limit = clampLimit(args.limit, 10);
    const out = await queryRows(
      `
      WITH ids AS (
        SELECT DISTINCT p.player_id, p.full_name
        FROM staging.stg_fivb_players p
        WHERE p.full_name ILIKE $1
        ORDER BY p.full_name
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
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "country_matchup_record") {
    const a = String(args.country_a || "").trim().toUpperCase();
    const b = String(args.country_b || "").trim().toUpperCase();
    if (!a || !b) return { error: "country_a and country_b are required." };
    const aCandidates = normalizeCountryCodeCandidates(a);
    const bCandidates = normalizeCountryCodeCandidates(b);
    const limit = clampLimit(args.limit, 20);
    const since = args.since_date ? String(args.since_date).trim() : null;
    const genderValues = normalizeGenderFilter(args.gender);
    const genderSql = genderValues ? "AND (LOWER(COALESCE(m.tournament_gender::text, '')) = ANY($5::text[]))" : "";
    const rows = await queryRows(
      `
      WITH normalized_matches AS (
        SELECT
          m.match_id,
          m.match_date,
          m.tournament_name,
          m.season AS tournament_season,
          m.team1_display_name,
          m.team2_display_name,
          m.score_sets,
          m.winner_team_id,
          m.team1_id,
          m.team2_id,
          m.tournament_gender,
          COALESCE(NULLIF(UPPER(COALESCE(p1a.country_code, p1b.country_code)), ''), UPPER(m.team1_country_code)) AS team1_country_code_norm,
          COALESCE(NULLIF(UPPER(COALESCE(p2a.country_code, p2b.country_code)), ''), UPPER(m.team2_country_code)) AS team2_country_code_norm
        FROM mart.match_mart m
        LEFT JOIN staging.stg_fivb_players p1a ON p1a.player_id = m.team1_player_a_id
        LEFT JOIN staging.stg_fivb_players p1b ON p1b.player_id = m.team1_player_b_id
        LEFT JOIN staging.stg_fivb_players p2a ON p2a.player_id = m.team2_player_a_id
        LEFT JOIN staging.stg_fivb_players p2b ON p2b.player_id = m.team2_player_b_id
      )
      SELECT
        nm.match_id,
        nm.match_date,
        nm.tournament_name,
        nm.tournament_season,
        nm.team1_country_code_norm AS team1_country_code,
        nm.team2_country_code_norm AS team2_country_code,
        nm.team1_display_name,
        nm.team2_display_name,
        nm.score_sets,
        CASE
          WHEN nm.winner_team_id = nm.team1_id AND nm.team1_country_code_norm = ANY($1::text[]) THEN $3::text
          WHEN nm.winner_team_id = nm.team2_id AND nm.team2_country_code_norm = ANY($1::text[]) THEN $3::text
          WHEN nm.winner_team_id = nm.team1_id AND nm.team1_country_code_norm = ANY($2::text[]) THEN $4::text
          WHEN nm.winner_team_id = nm.team2_id AND nm.team2_country_code_norm = ANY($2::text[]) THEN $4::text
          ELSE NULL
        END AS winner_country
      FROM normalized_matches nm
      WHERE (
        (nm.team1_country_code_norm = ANY($1::text[]) AND nm.team2_country_code_norm = ANY($2::text[]))
        OR
        (nm.team1_country_code_norm = ANY($2::text[]) AND nm.team2_country_code_norm = ANY($1::text[]))
      )
      AND ($6::date IS NULL OR nm.match_date >= $6::date)
      ${genderSql}
      ORDER BY nm.match_date DESC NULLS LAST, nm.match_id DESC
      LIMIT $${genderValues ? 8 : 7}
      `,
      genderValues
        ? [aCandidates, bCandidates, a, b, genderValues, since, limit]
        : [aCandidates, bCandidates, a, b, since, limit]
    );
    if (!Array.isArray(rows)) {
      emitProgress(onProgress, {
        type: "tool_done",
        tool: name,
        ok: false,
        rows: null,
        duration_ms: Date.now() - startedAt,
      });
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
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: true,
      rows: rows.length,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "country_opponent_performance") {
    const country = String(args.country_code || "").trim().toUpperCase();
    if (!country) return { error: "country_code is required." };
    const countryCandidates = normalizeCountryCodeCandidates(country);
    const limit = clampLimit(args.limit, 20);
    const since = args.since_date ? String(args.since_date).trim() : null;
    const minMatches = Math.max(1, Math.floor(Number(args.min_matches) || 3));
    const sortByRaw = String(args.sort_by || "best").trim().toLowerCase();
    const sortBy = sortByRaw === "worst" ? "worst" : "best";
    const genderValues = normalizeGenderFilter(args.gender);
    const params = [countryCandidates, since, limit];
    const genderSql = genderValues ? "AND (LOWER(COALESCE(nm.tournament_gender::text, '')) = ANY($4::text[]))" : "";
    if (genderValues) params.push(genderValues);
    params.push(minMatches);
    const orderBy = sortBy === "worst" ? "win_pct ASC" : "win_pct DESC";
    const baseSql = `
      WITH country_matches AS (
        SELECT
          CASE
            WHEN nm.team1_country_code_norm = ANY($1::text[]) THEN nm.team2_country_code_norm
            WHEN nm.team2_country_code_norm = ANY($1::text[]) THEN nm.team1_country_code_norm
            ELSE NULL
          END AS opponent_country,
          CASE
            WHEN nm.winner_team_id = nm.team1_id AND nm.team1_country_code_norm = ANY($1::text[]) THEN 1
            WHEN nm.winner_team_id = nm.team2_id AND nm.team2_country_code_norm = ANY($1::text[]) THEN 1
            ELSE 0
          END AS won_match,
          nm.match_date
        FROM (
          SELECT
            m.match_id,
            m.match_date,
            m.winner_team_id,
            m.team1_id,
            m.team2_id,
            m.tournament_gender,
            COALESCE(NULLIF(UPPER(COALESCE(p1a.country_code, p1b.country_code)), ''), UPPER(m.team1_country_code)) AS team1_country_code_norm,
            COALESCE(NULLIF(UPPER(COALESCE(p2a.country_code, p2b.country_code)), ''), UPPER(m.team2_country_code)) AS team2_country_code_norm
          FROM mart.match_mart m
          LEFT JOIN staging.stg_fivb_players p1a ON p1a.player_id = m.team1_player_a_id
          LEFT JOIN staging.stg_fivb_players p1b ON p1b.player_id = m.team1_player_b_id
          LEFT JOIN staging.stg_fivb_players p2a ON p2a.player_id = m.team2_player_a_id
          LEFT JOIN staging.stg_fivb_players p2b ON p2b.player_id = m.team2_player_b_id
        ) nm
        WHERE (nm.team1_country_code_norm = ANY($1::text[]) OR nm.team2_country_code_norm = ANY($1::text[]))
        AND ($2::date IS NULL OR nm.match_date >= $2::date)
        ${genderSql}
      )
      SELECT
        opponent_country,
        COUNT(*)::int AS matches_played,
        SUM(won_match)::int AS wins,
        (COUNT(*)::int - SUM(won_match)::int) AS losses,
        ROUND((SUM(won_match)::numeric / NULLIF(COUNT(*), 0)), 3) AS win_pct,
        MAX(match_date) AS last_match_date
      FROM country_matches
      WHERE opponent_country IS NOT NULL
        AND opponent_country <> ALL($1::text[])
      GROUP BY opponent_country
      HAVING COUNT(*) >= $${params.length}
      ORDER BY ${orderBy}, matches_played DESC, opponent_country ASC
      LIMIT $3
      `;
    let rows = await queryRows(
      baseSql,
      params
    );
    let minMatchesUsed = minMatches;
    if (Array.isArray(rows) && rows.length === 0 && minMatches > 1) {
      const relaxedParams = [...params];
      relaxedParams[relaxedParams.length - 1] = 1;
      const relaxedRows = await queryRows(baseSql, relaxedParams);
      if (Array.isArray(relaxedRows) && relaxedRows.length > 0) {
        rows = relaxedRows;
        minMatchesUsed = 1;
      }
    }
    if (!Array.isArray(rows)) {
      emitProgress(onProgress, {
        type: "tool_done",
        tool: name,
        ok: false,
        rows: null,
        duration_ms: Date.now() - startedAt,
      });
      return rows;
    }
    const out = {
      summary: {
        country_a: country,
        mode: "opponent_performance",
        sort_by: sortBy,
        min_matches_requested: minMatches,
        min_matches_used: minMatchesUsed,
        sample_opponents: rows.length,
      },
      matches: rows,
    };
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: true,
      rows: rows.length,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "active_players") {
    const params = [];
    const limit = clampLimit(args.limit, 25);
    const daysBack = ACTIVE_PLAYER_DAYS;

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
        last_match_tournament_name,
        last_match_tournament_season
      FROM mart.player_elo_latest
      WHERE last_match_played_at >= current_date - ($1::int * interval '1 day')
    `;
    params.push(daysBack);

    const country = String(args.country_code || "").trim().toUpperCase();
    if (country) {
      params.push(country);
      sql += ` AND player_country_code = $${params.length}`;
    }

    const genderFilter = normalizeGenderFilter(args.gender);
    if (genderFilter) {
      params.push(genderFilter);
      sql += ` AND lower(coalesce(gender::text, '')) = ANY($${params.length}::text[])`;
    }

    params.push(limit);
    sql += ` ORDER BY elo_rating DESC NULLS LAST, last_match_played_at DESC NULLS LAST LIMIT $${params.length}`;

    const out = await queryRows(sql, params);
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "inactive_players") {
    const params = [];
    const limit = clampLimit(args.limit, 25);
    const daysBack = ACTIVE_PLAYER_DAYS;

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
        last_match_tournament_name,
        last_match_tournament_season
      FROM mart.player_elo_latest
      WHERE last_match_played_at < current_date - ($1::int * interval '1 day')
    `;
    params.push(daysBack);

    const country = String(args.country_code || "").trim().toUpperCase();
    if (country) {
      params.push(country);
      sql += ` AND player_country_code = $${params.length}`;
    }

    const genderFilter = normalizeGenderFilter(args.gender);
    if (genderFilter) {
      params.push(genderFilter);
      sql += ` AND lower(coalesce(gender::text, '')) = ANY($${params.length}::text[])`;
    }

    params.push(limit);
    sql += ` ORDER BY elo_rating DESC NULLS LAST, last_match_played_at DESC NULLS LAST LIMIT $${params.length}`;

    const out = await queryRows(sql, params);
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "find_player") {
    const q = String(args.name_query || "").trim();
    if (!q) return { error: "name_query is required." };
    const limit = clampLimit(args.limit, 10);
    const params = [`%${q}%`];
    let sql = `
      SELECT
        p.player_id,
        p.full_name AS player_name,
        p.country_code AS player_country_code,
        pel.gender,
        ROUND(pel.elo_rating::numeric, 1) AS elo_rating,
        pel.last_match_played_at,
        pel.last_match_tournament_name
      FROM staging.stg_fivb_players p
      LEFT JOIN mart.player_elo_latest pel ON pel.player_id = p.player_id
      WHERE p.full_name ILIKE $1
    `;
    const country = String(args.country_code || "").trim().toUpperCase();
    if (country) {
      params.push(country);
      sql += ` AND p.country_code = $${params.length}`;
    }
    params.push(limit);
    sql += ` ORDER BY pel.last_match_played_at DESC NULLS LAST, pel.elo_rating DESC NULLS LAST LIMIT $${params.length}`;
    const out = await queryRows(sql, params);
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "player_profile") {
    const pid = Number(args.player_id);
    const pname = String(args.player_name || "").trim();
    if (!Number.isFinite(pid) && !pname) return { error: "Provide player_id or player_name." };
    const out = await queryRows(
      `
      WITH candidate AS (
        SELECT p.player_id, p.full_name, p.country_code
        FROM staging.stg_fivb_players p
        WHERE ($1::bigint IS NOT NULL AND p.player_id = $1::bigint)
           OR ($1::bigint IS NULL AND p.full_name ILIKE $2)
        ORDER BY CASE WHEN $1::bigint IS NOT NULL THEN 0 ELSE 1 END, p.full_name
        LIMIT 1
      )
      SELECT
        c.player_id,
        c.full_name AS player_name,
        c.country_code AS player_country_code,
        pel.gender,
        ROUND(pel.elo_rating::numeric, 1) AS elo_rating,
        pel.matches_played,
        pel.wins,
        pel.losses,
        pel.last_match_played_at,
        pel.last_match_tournament_name,
        pel.last_match_tournament_season
      FROM candidate c
      LEFT JOIN mart.player_elo_latest pel ON pel.player_id = c.player_id
      `,
      [Number.isFinite(pid) ? Math.floor(pid) : null, pname ? `%${pname}%` : null]
    );
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "full_player_profile") {
    const pid = Number(args.player_id);
    const pname = String(args.player_name || "").trim();
    if (!Number.isFinite(pid) && !pname) return { error: "Provide player_id or player_name." };
    const teammateLimit = clampLimit(args.teammate_limit, 8);
    const similarLimit = clampLimit(args.similar_limit, 8);
    const recentLimit = clampLimit(args.recent_matches_limit, 8);
    const resolvedRows = await queryRows(
      `
      SELECT p.player_id
      FROM staging.stg_fivb_players p
      WHERE ($1::bigint IS NOT NULL AND p.player_id = $1::bigint)
         OR ($1::bigint IS NULL AND p.full_name ILIKE $2)
      ORDER BY CASE WHEN $1::bigint IS NOT NULL THEN 0 ELSE 1 END, p.full_name
      LIMIT 1
      `,
      [Number.isFinite(pid) ? Math.floor(pid) : null, pname ? `%${pname}%` : null]
    );
    if (!Array.isArray(resolvedRows)) {
      emitProgress(onProgress, {
        type: "tool_done",
        tool: name,
        ok: false,
        rows: null,
        preview: resolvedRows && typeof resolvedRows === "object" ? resolvedRows : null,
        freshness_hint: null,
        duration_ms: Date.now() - startedAt,
      });
      return resolvedRows;
    }
    if (!resolvedRows[0]?.player_id) {
      return { error: "Player not found." };
    }
    const resolvedPlayerId = Number(resolvedRows[0].player_id);

    const profileRows = await queryRows(
      `
      SELECT
        p.player_id,
        p.full_name AS player_name,
        p.country_code AS player_country_code,
        pel.gender,
        p.birth_date,
        ROUND(pel.player_height_inches::numeric, 1) AS height_inches,
        ROUND(pel.elo_rating::numeric, 1) AS elo_rating,
        pel.matches_played,
        pel.wins,
        pel.losses,
        pel.last_match_played_at,
        pel.last_match_tournament_name,
        pel.last_match_tournament_season
      FROM staging.stg_fivb_players p
      LEFT JOIN mart.player_elo_latest pel ON pel.player_id = p.player_id
      WHERE p.player_id = $1::bigint
      LIMIT 1
      `,
      [resolvedPlayerId]
    );
    if (!Array.isArray(profileRows)) return profileRows;
    const profile = profileRows[0] || null;
    if (profile) {
      const heightInches = Number(profile.height_inches);
      profile.height_cm = Number.isFinite(heightInches) ? Number((heightInches * 2.54).toFixed(1)) : null;
      profile.weight_kg = null;
      profile.age_years = profile.birth_date
        ? Math.floor((Date.now() - new Date(profile.birth_date).getTime()) / (365.25 * 24 * 60 * 60 * 1000))
        : null;
    }

    const teammateHistory = await queryRows(
      `
      WITH player_matches AS (
        SELECT
          m.match_id,
          m.match_date,
          m.winner_team_id,
          CASE
            WHEN $1::bigint IN (m.team1_player_a_id, m.team1_player_b_id) THEN m.team1_id
            WHEN $1::bigint IN (m.team2_player_a_id, m.team2_player_b_id) THEN m.team2_id
            ELSE NULL
          END AS player_team_id,
          CASE
            WHEN $1::bigint IN (m.team1_player_a_id, m.team1_player_b_id)
              THEN CASE WHEN m.team1_player_a_id = $1::bigint THEN m.team1_player_b_id ELSE m.team1_player_a_id END
            WHEN $1::bigint IN (m.team2_player_a_id, m.team2_player_b_id)
              THEN CASE WHEN m.team2_player_a_id = $1::bigint THEN m.team2_player_b_id ELSE m.team2_player_a_id END
            ELSE NULL
          END AS teammate_id
        FROM mart.match_mart m
        WHERE $1::bigint IN (m.team1_player_a_id, m.team1_player_b_id, m.team2_player_a_id, m.team2_player_b_id)
      )
      SELECT
        pm.teammate_id AS player_id,
        p.full_name AS player_name,
        p.country_code AS player_country_code,
        COUNT(*) AS matches_together,
        SUM(CASE WHEN pm.winner_team_id = pm.player_team_id THEN 1 ELSE 0 END) AS wins_together,
        MAX(pm.match_date) AS last_played_together
      FROM player_matches pm
      LEFT JOIN staging.stg_fivb_players p ON p.player_id = pm.teammate_id
      WHERE pm.teammate_id IS NOT NULL
      GROUP BY pm.teammate_id, p.full_name, p.country_code
      ORDER BY matches_together DESC, wins_together DESC, last_played_together DESC NULLS LAST
      LIMIT $2
      `,
      [resolvedPlayerId, teammateLimit]
    );
    if (!Array.isArray(teammateHistory)) return teammateHistory;

    const similarPlayers = await queryRows(
      `
      WITH target AS (
        SELECT player_id, gender, elo_rating
        FROM mart.player_elo_latest
        WHERE player_id = $1::bigint
        LIMIT 1
      )
      SELECT
        c.player_id,
        c.player_name,
        c.player_country_code,
        c.gender,
        ROUND(c.elo_rating::numeric, 1) AS elo_rating,
        ROUND(ABS(c.elo_rating - t.elo_rating)::numeric, 1) AS elo_gap,
        c.last_match_played_at
      FROM mart.player_elo_latest c
      JOIN target t ON c.gender = t.gender
      WHERE c.player_id <> t.player_id
        AND c.last_match_played_at >= current_date - ($2::int * interval '1 day')
        AND c.elo_rating BETWEEN t.elo_rating - 75 AND t.elo_rating + 75
      ORDER BY elo_gap ASC, c.last_match_played_at DESC NULLS LAST
      LIMIT $3
      `,
      [resolvedPlayerId, ACTIVE_PLAYER_DAYS, similarLimit]
    );
    if (!Array.isArray(similarPlayers)) return similarPlayers;

    const recentMatches = await queryRows(
      `
      SELECT
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
      FROM mart.match_mart m
      WHERE $1::bigint IN (
        m.team1_player_a_id,
        m.team1_player_b_id,
        m.team2_player_a_id,
        m.team2_player_b_id
      )
      ORDER BY m.match_date DESC NULLS LAST, m.match_id DESC
      LIMIT $2
      `,
      [resolvedPlayerId, recentLimit]
    );
    if (!Array.isArray(recentMatches)) return recentMatches;

    const out = {
      profile,
      teammate_history: teammateHistory,
      similar_players: similarPlayers,
      recent_matches: recentMatches,
      data_gaps: {
        weight_kg: "Not currently available in analytics mart.",
      },
    };
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: true,
      rows: profile ? 1 : 0,
      preview: profile ? [profile] : [],
      freshness_hint: inferFreshnessFromRows([profile, ...recentMatches].filter(Boolean)),
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "head_to_head_players") {
    const a = Number(args.player_a_id);
    const b = Number(args.player_b_id);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return { error: "player_a_id and player_b_id are required." };
    const limit = clampLimit(args.limit, 20);
    const rows = await queryRows(
      `
      WITH relevant AS (
        SELECT
          m.match_id,
          m.match_date,
          m.tournament_name,
          m.season AS tournament_season,
          m.team1_id,
          m.team2_id,
          m.winner_team_id,
          m.score_sets,
          m.team1_player_a_id,
          m.team1_player_b_id,
          m.team2_player_a_id,
          m.team2_player_b_id,
          m.team1_display_name,
          m.team2_display_name
        FROM mart.match_mart m
        WHERE $1::bigint IN (m.team1_player_a_id, m.team1_player_b_id, m.team2_player_a_id, m.team2_player_b_id)
          AND $2::bigint IN (m.team1_player_a_id, m.team1_player_b_id, m.team2_player_a_id, m.team2_player_b_id)
      )
      SELECT
        match_id,
        match_date,
        tournament_name,
        tournament_season,
        team1_display_name,
        team2_display_name,
        score_sets,
        CASE
          WHEN winner_team_id = team1_id AND $1::bigint IN (team1_player_a_id, team1_player_b_id) THEN 'A'
          WHEN winner_team_id = team2_id AND $1::bigint IN (team2_player_a_id, team2_player_b_id) THEN 'A'
          WHEN winner_team_id = team1_id AND $2::bigint IN (team1_player_a_id, team1_player_b_id) THEN 'B'
          WHEN winner_team_id = team2_id AND $2::bigint IN (team2_player_a_id, team2_player_b_id) THEN 'B'
          ELSE NULL
        END AS winner_side
      FROM relevant
      ORDER BY match_date DESC NULLS LAST, match_id DESC
      LIMIT $3
      `,
      [Math.floor(a), Math.floor(b), limit]
    );
    if (!Array.isArray(rows)) {
      emitProgress(onProgress, {
        type: "tool_done",
        tool: name,
        ok: false,
        rows: null,
        preview: rows && typeof rows === "object" ? rows : null,
        freshness_hint: null,
        duration_ms: Date.now() - startedAt,
      });
      return rows;
    }
    const summary = rows.reduce(
      (acc, r) => {
        if (r.winner_side === "A") acc.player_a_wins += 1;
        if (r.winner_side === "B") acc.player_b_wins += 1;
        return acc;
      },
      { player_a_id: Math.floor(a), player_b_id: Math.floor(b), player_a_wins: 0, player_b_wins: 0, sample_matches: rows.length }
    );
    const out = { summary, matches: rows };
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: true,
      rows: rows.length,
      preview: rows.slice(0, 5),
      freshness_hint: inferFreshnessFromRows(rows),
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "player_teammate_history") {
    const pid = Number(args.player_id);
    if (!Number.isFinite(pid)) return { error: "player_id is required." };
    const limit = clampLimit(args.limit, 20);
    const out = await queryRows(
      `
      WITH player_matches AS (
        SELECT
          m.match_id,
          m.match_date,
          m.winner_team_id,
          CASE
            WHEN $1::bigint IN (m.team1_player_a_id, m.team1_player_b_id) THEN m.team1_id
            WHEN $1::bigint IN (m.team2_player_a_id, m.team2_player_b_id) THEN m.team2_id
            ELSE NULL
          END AS player_team_id,
          CASE
            WHEN $1::bigint IN (m.team1_player_a_id, m.team1_player_b_id)
              THEN CASE WHEN m.team1_player_a_id = $1::bigint THEN m.team1_player_b_id ELSE m.team1_player_a_id END
            WHEN $1::bigint IN (m.team2_player_a_id, m.team2_player_b_id)
              THEN CASE WHEN m.team2_player_a_id = $1::bigint THEN m.team2_player_b_id ELSE m.team2_player_a_id END
            ELSE NULL
          END AS teammate_id
        FROM mart.match_mart m
        WHERE $1::bigint IN (m.team1_player_a_id, m.team1_player_b_id, m.team2_player_a_id, m.team2_player_b_id)
      )
      SELECT
        pm.teammate_id AS player_id,
        p.full_name AS player_name,
        p.country_code AS player_country_code,
        COUNT(*) AS matches_together,
        SUM(CASE WHEN pm.winner_team_id = pm.player_team_id THEN 1 ELSE 0 END) AS wins_together,
        MAX(pm.match_date) AS last_played_together
      FROM player_matches pm
      LEFT JOIN staging.stg_fivb_players p ON p.player_id = pm.teammate_id
      WHERE pm.teammate_id IS NOT NULL
      GROUP BY pm.teammate_id, p.full_name, p.country_code
      ORDER BY matches_together DESC, wins_together DESC, last_played_together DESC NULLS LAST
      LIMIT $2
      `,
      [Math.floor(pid), limit]
    );
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "most_improved_players") {
    const limit = clampLimit(args.limit, 20);
    const params = [ACTIVE_PLAYER_DAYS];
    let sql = `
      WITH latest AS (
        SELECT player_id, gender, MAX(as_of_date) AS latest_date
        FROM mart.player_elo_history
        GROUP BY player_id, gender
      ),
      baseline AS (
        SELECT
          e.player_id,
          e.gender,
          MIN(e.as_of_date) AS baseline_date
        FROM mart.player_elo_history e
        JOIN latest l ON l.player_id = e.player_id AND l.gender = e.gender
        WHERE e.as_of_date >= l.latest_date - ($1::int * interval '1 day')
        GROUP BY e.player_id, e.gender
      ),
      paired AS (
        SELECT
          l.player_id,
          l.gender,
          l.latest_date,
          b.baseline_date,
          el.elo_rating AS latest_elo,
          eb.elo_rating AS baseline_elo
        FROM latest l
        JOIN baseline b ON b.player_id = l.player_id AND b.gender = l.gender
        JOIN mart.player_elo_history el
          ON el.player_id = l.player_id AND el.gender = l.gender AND el.as_of_date = l.latest_date
        JOIN mart.player_elo_history eb
          ON eb.player_id = b.player_id AND eb.gender = b.gender AND eb.as_of_date = b.baseline_date
      )
      SELECT
        p.player_id,
        p.player_name,
        p.player_country_code,
        p.gender,
        ROUND((pa.latest_elo - pa.baseline_elo)::numeric, 1) AS elo_change,
        ROUND(pa.baseline_elo::numeric, 1) AS baseline_elo,
        ROUND(pa.latest_elo::numeric, 1) AS latest_elo,
        pa.baseline_date,
        pa.latest_date
      FROM paired pa
      JOIN mart.player_elo_latest p ON p.player_id = pa.player_id AND p.gender = pa.gender
      WHERE p.last_match_played_at >= current_date - ($1::int * interval '1 day')
    `;
    const country = String(args.country_code || "").trim().toUpperCase();
    if (country) {
      params.push(country);
      sql += ` AND p.player_country_code = $${params.length}`;
    }
    const genderFilter = normalizeGenderFilter(args.gender);
    if (genderFilter) {
      params.push(genderFilter);
      sql += ` AND lower(coalesce(p.gender::text, '')) = ANY($${params.length}::text[])`;
    }
    params.push(limit);
    sql += ` ORDER BY elo_change DESC NULLS LAST, latest_elo DESC NULLS LAST LIMIT $${params.length}`;
    const out = await queryRows(sql, params);
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "country_depth_report") {
    const country = String(args.country_code || "").trim().toUpperCase();
    if (!country) return { error: "country_code is required." };
    const topN = clampLimit(args.top_n, 10);
    const params = [country, ACTIVE_PLAYER_DAYS];
    let genderWhere = "";
    const genderFilter = normalizeGenderFilter(args.gender);
    if (genderFilter) {
      params.push(genderFilter);
      genderWhere = ` AND lower(coalesce(gender::text, '')) = ANY($${params.length}::text[])`;
    }
    const topRows = await queryRows(
      `
      SELECT
        player_id,
        player_name,
        player_country_code,
        gender,
        ROUND(elo_rating::numeric, 1) AS elo_rating,
        matches_played,
        wins,
        losses,
        last_match_played_at
      FROM mart.player_elo_latest
      WHERE player_country_code = $1
        AND last_match_played_at >= current_date - ($2::int * interval '1 day')
        ${genderWhere}
      ORDER BY elo_rating DESC NULLS LAST
      LIMIT ${topN}
      `,
      params
    );
    const summaryRows = await queryRows(
      `
      SELECT
        COUNT(*)::int AS active_player_count,
        ROUND(MAX(elo_rating)::numeric, 1) AS top_elo,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY elo_rating)::numeric, 1) AS median_elo,
        ROUND(AVG(CASE WHEN matches_played > 0 THEN wins::numeric / matches_played ELSE NULL END)::numeric, 3) AS avg_win_rate
      FROM mart.player_elo_latest
      WHERE player_country_code = $1
        AND last_match_played_at >= current_date - ($2::int * interval '1 day')
        ${genderWhere}
      `,
      params
    );
    if (!Array.isArray(topRows) || !Array.isArray(summaryRows)) {
      const errOut = Array.isArray(topRows) ? summaryRows : topRows;
      emitProgress(onProgress, {
        type: "tool_done",
        tool: name,
        ok: false,
        rows: null,
        preview: Array.isArray(topRows) ? summaryRows : topRows,
        freshness_hint: null,
        duration_ms: Date.now() - startedAt,
      });
      return errOut;
    }
    const out = { summary: summaryRows[0] || {}, top_players: topRows };
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: true,
      rows: topRows.length,
      preview: topRows.slice(0, 5),
      freshness_hint: inferFreshnessFromRows(topRows),
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "player_status") {
    const pid = Number(args.player_id);
    const pname = String(args.player_name || "").trim();
    if (!Number.isFinite(pid) && !pname) return { error: "Provide player_id or player_name." };
    const out = await queryRows(
      `
      WITH candidate AS (
        SELECT p.player_id, p.full_name, p.country_code
        FROM staging.stg_fivb_players p
        WHERE ($1::bigint IS NOT NULL AND p.player_id = $1::bigint)
           OR ($1::bigint IS NULL AND p.full_name ILIKE $2)
        ORDER BY CASE WHEN $1::bigint IS NOT NULL THEN 0 ELSE 1 END, p.full_name
        LIMIT 1
      )
      SELECT
        c.player_id,
        c.full_name AS player_name,
        c.country_code AS player_country_code,
        pel.gender,
        ROUND(pel.elo_rating::numeric, 1) AS elo_rating,
        pel.matches_played,
        pel.wins,
        pel.losses,
        pel.last_match_played_at,
        pel.last_match_tournament_name,
        pel.last_match_tournament_season,
        CASE
          WHEN pel.last_match_played_at >= current_date - ($3::int * interval '1 day') THEN 'active'
          ELSE 'inactive'
        END AS activity_status
      FROM candidate c
      LEFT JOIN mart.player_elo_latest pel ON pel.player_id = c.player_id
      `,
      [Number.isFinite(pid) ? Math.floor(pid) : null, pname ? `%${pname}%` : null, ACTIVE_PLAYER_DAYS]
    );
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "tournament_lookup") {
    const q = String(args.query || "").trim();
    if (!q) return { error: "query is required." };
    const limit = clampLimit(args.limit, 10);
    const out = await queryRows(
      `
      SELECT
        tournament_id,
        tournament_name,
        season,
        tournament_city,
        tournament_country_code,
        tournament_country_name,
        tournament_start_date,
        tournament_end_date,
        tournament_tier
      FROM mart.tournament_mart
      WHERE tournament_name ILIKE $1
         OR tournament_city ILIKE $1
         OR tournament_country_name ILIKE $1
         OR tournament_country_code ILIKE $1
      ORDER BY tournament_start_date DESC NULLS LAST, tournament_id DESC
      LIMIT $2
      `,
      [`%${q}%`, limit]
    );
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "tournament_snapshot") {
    const tid = Number(args.tournament_id);
    if (!Number.isFinite(tid)) return { error: "tournament_id is required." };
    const summaryRows = await queryRows(
      `
      WITH participants AS (
        SELECT DISTINCT
          pel.player_id,
          pel.elo_rating
        FROM mart.match_mart m
        JOIN mart.player_elo_latest pel
          ON pel.player_id IN (m.team1_player_a_id, m.team1_player_b_id, m.team2_player_a_id, m.team2_player_b_id)
        WHERE m.tournament_id = $1::bigint
      )
      SELECT
        t.tournament_id,
        t.tournament_name,
        t.season,
        t.tournament_city,
        t.tournament_country_code,
        t.tournament_country_name,
        t.tournament_start_date,
        t.tournament_end_date,
        t.tournament_tier,
        COUNT(DISTINCT p.player_id)::int AS distinct_players,
        ROUND(AVG(p.elo_rating)::numeric, 1) AS avg_player_elo,
        ROUND(MAX(p.elo_rating)::numeric, 1) AS max_player_elo
      FROM mart.tournament_mart t
      LEFT JOIN participants p ON TRUE
      WHERE t.tournament_id = $1::bigint
      GROUP BY
        t.tournament_id, t.tournament_name, t.season, t.tournament_city, t.tournament_country_code,
        t.tournament_country_name, t.tournament_start_date, t.tournament_end_date, t.tournament_tier
      `,
      [Math.floor(tid)]
    );
    const standings = await queryRows(
      `
      SELECT
        finishing_pos,
        team_name,
        points,
        prize_money
      FROM core.fct_tournament_standings
      WHERE tournament_id = $1::bigint
      ORDER BY finishing_pos ASC NULLS LAST
      LIMIT 8
      `,
      [Math.floor(tid)]
    );
    if (!Array.isArray(summaryRows) || !Array.isArray(standings)) {
      const errOut = Array.isArray(summaryRows) ? standings : summaryRows;
      emitProgress(onProgress, {
        type: "tool_done",
        tool: name,
        ok: false,
        rows: null,
        preview: Array.isArray(summaryRows) ? standings : summaryRows,
        freshness_hint: null,
        duration_ms: Date.now() - startedAt,
      });
      return errOut;
    }
    const out = { summary: summaryRows[0] || {}, standings };
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: true,
      rows: standings.length,
      preview: standings.slice(0, 5),
      freshness_hint: inferFreshnessFromRows(standings),
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "partnership_profile") {
    const a = Number(args.player_a_id);
    const b = Number(args.player_b_id);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return { error: "player_a_id and player_b_id are required." };
    const limit = clampLimit(args.limit, 20);
    const rows = await queryRows(
      `
      WITH pair_matches AS (
        SELECT
          m.match_id,
          m.match_date,
          m.tournament_name,
          m.season AS tournament_season,
          m.team1_id,
          m.team2_id,
          m.winner_team_id,
          m.score_sets,
          m.team1_display_name,
          m.team2_display_name,
          CASE
            WHEN $1::bigint IN (m.team1_player_a_id, m.team1_player_b_id)
             AND $2::bigint IN (m.team1_player_a_id, m.team1_player_b_id)
              THEN m.team1_id
            WHEN $1::bigint IN (m.team2_player_a_id, m.team2_player_b_id)
             AND $2::bigint IN (m.team2_player_a_id, m.team2_player_b_id)
              THEN m.team2_id
            ELSE NULL
          END AS pair_team_id
        FROM mart.match_mart m
        WHERE ($1::bigint IN (m.team1_player_a_id, m.team1_player_b_id, m.team2_player_a_id, m.team2_player_b_id))
          AND ($2::bigint IN (m.team1_player_a_id, m.team1_player_b_id, m.team2_player_a_id, m.team2_player_b_id))
      )
      SELECT
        match_id,
        match_date,
        tournament_name,
        tournament_season,
        score_sets,
        team1_display_name,
        team2_display_name,
        CASE WHEN winner_team_id = pair_team_id THEN true ELSE false END AS pair_won
      FROM pair_matches
      WHERE pair_team_id IS NOT NULL
      ORDER BY match_date DESC NULLS LAST, match_id DESC
      LIMIT $3
      `,
      [Math.floor(a), Math.floor(b), limit]
    );
    if (!Array.isArray(rows)) {
      emitProgress(onProgress, {
        type: "tool_done",
        tool: name,
        ok: false,
        rows: null,
        preview: rows && typeof rows === "object" ? rows : null,
        freshness_hint: null,
        duration_ms: Date.now() - startedAt,
      });
      return rows;
    }
    const summary = rows.reduce(
      (acc, r) => {
        acc.matches += 1;
        if (r.pair_won) acc.wins += 1;
        return acc;
      },
      { player_a_id: Math.floor(a), player_b_id: Math.floor(b), matches: 0, wins: 0, losses: 0 }
    );
    summary.losses = summary.matches - summary.wins;
    summary.win_rate = summary.matches > 0 ? Number((summary.wins / summary.matches).toFixed(3)) : null;
    const out = { summary, recent_matches: rows };
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: true,
      rows: rows.length,
      preview: rows.slice(0, 5),
      freshness_hint: inferFreshnessFromRows(rows),
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "similar_players") {
    const pid = Number(args.player_id);
    if (!Number.isFinite(pid)) return { error: "player_id is required." };
    const limit = clampLimit(args.limit, 10);
    const params = [Math.floor(pid), ACTIVE_PLAYER_DAYS];
    let countryWhere = "";
    const country = String(args.country_code || "").trim().toUpperCase();
    if (country) {
      params.push(country);
      countryWhere = ` AND c.player_country_code = $${params.length}`;
    }
    const out = await queryRows(
      `
      WITH target AS (
        SELECT player_id, player_country_code, gender, elo_rating
        FROM mart.player_elo_latest
        WHERE player_id = $1::bigint
        LIMIT 1
      )
      SELECT
        c.player_id,
        c.player_name,
        c.player_country_code,
        c.gender,
        ROUND(c.elo_rating::numeric, 1) AS elo_rating,
        ROUND(ABS(c.elo_rating - t.elo_rating)::numeric, 1) AS elo_gap,
        c.last_match_played_at
      FROM mart.player_elo_latest c
      JOIN target t ON c.gender = t.gender
      WHERE c.player_id <> t.player_id
        AND c.last_match_played_at >= current_date - ($2::int * interval '1 day')
        AND c.elo_rating BETWEEN t.elo_rating - 75 AND t.elo_rating + 75
        ${countryWhere}
      ORDER BY elo_gap ASC, c.last_match_played_at DESC NULLS LAST
      LIMIT ${limit}
      `,
      params
    );
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "explain_ranking") {
    const a = Number(args.player_a_id);
    const b = Number(args.player_b_id);
    if (!Number.isFinite(a) || !Number.isFinite(b)) return { error: "player_a_id and player_b_id are required." };
    const rows = await queryRows(
      `
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
        CASE
          WHEN last_match_played_at >= current_date - ($3::int * interval '1 day') THEN 'active'
          ELSE 'inactive'
        END AS activity_status
      FROM mart.player_elo_latest
      WHERE player_id IN ($1::bigint, $2::bigint)
      ORDER BY elo_rating DESC NULLS LAST
      `,
      [Math.floor(a), Math.floor(b), ACTIVE_PLAYER_DAYS]
    );
    if (!Array.isArray(rows)) {
      emitProgress(onProgress, {
        type: "tool_done",
        tool: name,
        ok: false,
        rows: null,
        preview: rows && typeof rows === "object" ? rows : null,
        freshness_hint: null,
        duration_ms: Date.now() - startedAt,
      });
      return rows;
    }
    let explanation = "Insufficient data.";
    if (rows.length === 2) {
      const top = rows[0];
      const other = rows[1];
      const diff = Number((top.elo_rating - other.elo_rating).toFixed(1));
      explanation = `${top.player_name} ranks above ${other.player_name} primarily due to a ${diff} Elo point edge.`;
    }
    const out = { explanation, players: rows };
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: true,
      rows: rows.length,
      preview: rows.slice(0, 5),
      freshness_hint: inferFreshnessFromRows(rows),
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  if (name === "query_db") {
    const sql = String(args.sql || "");
    if (!isSafeReadOnlySql(sql)) return { error: "Only read-only SELECT/CTE SQL is allowed." };
    const out = await queryRows(sql, []);
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: summarizeToolOutput(out).ok,
      rows: summarizeToolOutput(out).rows,
      preview: summarizeToolOutput(out).preview,
      freshness_hint: summarizeToolOutput(out).freshness_hint,
      duration_ms: Date.now() - startedAt,
    });
    return out;
  }

  emitProgress(onProgress, {
    type: "tool_done",
    tool: name,
    ok: false,
    rows: null,
    duration_ms: Date.now() - startedAt,
  });
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

function styleInstruction(style) {
  const s = String(style || "balanced").toLowerCase();
  if (s === "brief") return "Use short bullet points and be concise.";
  if (s === "table") return "Prefer compact markdown tables when returning lists.";
  if (s === "scout") return "Write as a short scouting report with key takeaways.";
  return "Use balanced concise answers with clear structure.";
}

function buildMetaFromToolRuns(toolRuns) {
  const freshness = toolRuns.map((r) => r.freshness_hint).filter(Boolean).sort().reverse()[0] || null;
  const hadError = toolRuns.some((r) => !r.ok);
  return {
    confidence: hadError ? "medium" : toolRuns.length > 0 ? "high" : "medium",
    freshness_hint: freshness,
    tools_used: toolRuns.map((r) => r.tool),
  };
}

async function generateWithOpenAiTools(clean, onProgress, options = {}) {
  const meta = activeProviderMeta();
  let sessionContext = mergeContext({}, options.clientContext || {});
  const responseStyle = options.responseStyle || "balanced";
  const toolRuns = [];
  const system = {
    role: "system",
    content:
      "You are Volley Chat for FIVB beach volleyball analytics only. " +
      "Treat ranking questions like 'top USA men by Elo' as volleyball data questions and call tools for factual answers. " +
      "For country questions phrased like 'best against' or 'worst against', use country_opponent_performance (not country_matchup_record). " +
      "Prefer tool calls for stats/facts and keep answers concise. " +
      "If a tool returns ambiguous players, ask a quick clarification question. " +
      styleInstruction(responseStyle) +
      ` Current context: ${JSON.stringify(sessionContext)}.`,
  };
  const msgs = [system, ...clean];
  const planStart = Date.now();
  emitProgress(onProgress, { type: "model_start", stage: "plan_tools", provider: meta.provider, model: meta.model });
  const first = await callOpenAiCompat({
    url: `${OPENAI_BASE_URL}/chat/completions`,
    token: OPENAI_API_KEY,
    model: OPENAI_MODEL,
    messages: msgs,
    tools: TOOL_DEFS,
    toolChoice: "auto",
  });
  emitProgress(onProgress, { type: "model_done", stage: "plan_tools", duration_ms: Date.now() - planStart });

  const choice = first.choices?.[0]?.message || {};
  const toolCalls = Array.isArray(choice.tool_calls) ? choice.tool_calls : [];
  if (toolCalls.length === 0) {
    return {
      status: 200,
      body: {
        role: "assistant",
        content: choice.content || "",
        context: sessionContext,
        response_style: responseStyle,
        meta: buildMetaFromToolRuns(toolRuns),
        ...meta,
      },
    };
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
    const toolName = tc.function?.name;
    const result = await runTool(toolName, args, onProgress);
    const summary = summarizeToolOutput(result);
    toolRuns.push({ tool: toolName, ...summary });
    sessionContext = updateContextFromTool(sessionContext, toolName, args, result);
    msgs.push({
      role: "tool",
      tool_call_id: tc.id,
      content: JSON.stringify(result),
    });
  }

  const finalStart = Date.now();
  emitProgress(onProgress, { type: "model_start", stage: "final_answer", provider: meta.provider, model: meta.model });
  const second = await callOpenAiCompat({
    url: `${OPENAI_BASE_URL}/chat/completions`,
    token: OPENAI_API_KEY,
    model: OPENAI_MODEL,
    messages: msgs,
    tools: TOOL_DEFS,
    toolChoice: "none",
  });
  emitProgress(onProgress, { type: "model_done", stage: "final_answer", duration_ms: Date.now() - finalStart });
  const finalText = second.choices?.[0]?.message?.content || "I could not generate a response.";
  return {
    status: 200,
    body: {
      role: "assistant",
      content: finalText,
      tool: "structured_tools",
      context: sessionContext,
      response_style: responseStyle,
      meta: buildMetaFromToolRuns(toolRuns),
      ...meta,
    },
  };
}

function buildHfPrompt(messages, toolResult, context, responseStyle) {
  const intro = [
    "You are Volley Chat, a helpful assistant.",
    "When database access is needed, reply ONLY with compact JSON:",
    '{"tool":"query_db","sql":"SELECT ..."}',
    "Use read-only SQL only (SELECT/CTE).",
    "If no tool is needed, answer normally.",
    styleInstruction(responseStyle),
    `Current context: ${JSON.stringify(context || {})}`,
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

async function generateWithHfFallback(clean, onProgress, options = {}) {
  const meta = activeProviderMeta();
  let sessionContext = mergeContext({}, options.clientContext || {});
  const responseStyle = options.responseStyle || "balanced";
  const toolRuns = [];
  const firstStart = Date.now();
  emitProgress(onProgress, { type: "model_start", stage: "hf_first_pass", provider: meta.provider, model: meta.model });
  const first = await callHf(buildHfPrompt(clean, null, sessionContext, responseStyle));
  emitProgress(onProgress, { type: "model_done", stage: "hf_first_pass", duration_ms: Date.now() - firstStart });
  const toolCall = extractHfToolCall(first);
  if (!toolCall) {
    return {
      status: 200,
      body: {
        role: "assistant",
        content: first,
        context: sessionContext,
        response_style: responseStyle,
        meta: buildMetaFromToolRuns(toolRuns),
        ...meta,
      },
    };
  }
  const rows = await runTool("query_db", { sql: toolCall.sql }, onProgress);
  const summary = summarizeToolOutput(rows);
  toolRuns.push({ tool: "query_db", ...summary });
  sessionContext = updateContextFromTool(sessionContext, "query_db", { sql: toolCall.sql }, rows);
  const secondStart = Date.now();
  emitProgress(onProgress, { type: "model_start", stage: "hf_second_pass", provider: meta.provider, model: meta.model });
  const second = await callHf(buildHfPrompt(clean, rows, sessionContext, responseStyle));
  emitProgress(onProgress, { type: "model_done", stage: "hf_second_pass", duration_ms: Date.now() - secondStart });
  return {
    status: 200,
    body: {
      role: "assistant",
      content: second,
      tool: "query_db",
      context: sessionContext,
      response_style: responseStyle,
      meta: buildMetaFromToolRuns(toolRuns),
      ...meta,
    },
  };
}

async function generateChatReply(messages, options = {}) {
  const onProgress = options.onProgress;
  const clean = sanitizeMessages(messages);
  if (clean.length === 0) {
    return { status: 400, body: { error: "Expected a non-empty messages array.", ...activeProviderMeta() } };
  }
  if (OPENAI_API_KEY) return generateWithOpenAiTools(clean, onProgress, options);
  return generateWithHfFallback(clean, onProgress, options);
}

async function runToolSmokeTests() {
  const startedAt = Date.now();
  const db = getPool();
  if (!db) {
    return {
      ok: false,
      error: "DATABASE_URL is not set.",
      provider: activeProviderMeta(),
      elapsed_ms: Date.now() - startedAt,
      results: [],
    };
  }

  const sample = await queryRows(
    `
    SELECT player_id, player_country_code
    FROM mart.player_elo_latest
    WHERE player_id IS NOT NULL
    ORDER BY last_match_played_at DESC NULLS LAST
    LIMIT 2
    `,
    []
  );
  if (!Array.isArray(sample) || sample.length === 0) {
    return {
      ok: false,
      error: "No players found in mart.player_elo_latest for smoke test.",
      provider: activeProviderMeta(),
      elapsed_ms: Date.now() - startedAt,
      results: [],
    };
  }

  const playerA = Number(sample[0].player_id);
  const playerB = Number((sample[1] && sample[1].player_id) || sample[0].player_id);
  const country = String(sample[0].player_country_code || "USA").toUpperCase();
  const tRows = await queryRows(
    `
    SELECT tournament_id
    FROM mart.match_mart
    WHERE tournament_id IS NOT NULL
    ORDER BY match_date DESC NULLS LAST
    LIMIT 1
    `,
    []
  );
  const tournamentId = Array.isArray(tRows) && tRows[0] ? Number(tRows[0].tournament_id) : null;
  const opponentRows = await queryRows(
    `
    SELECT
      CASE
        WHEN team1_country_code = $1 THEN team2_country_code
        WHEN team2_country_code = $1 THEN team1_country_code
        ELSE NULL
      END AS opponent_country
    FROM mart.match_mart
    WHERE (team1_country_code = $1 OR team2_country_code = $1)
      AND COALESCE(
        CASE
          WHEN team1_country_code = $1 THEN team2_country_code
          WHEN team2_country_code = $1 THEN team1_country_code
          ELSE NULL
        END,
        ''
      ) <> ''
    ORDER BY match_date DESC NULLS LAST
    LIMIT 1
    `,
    [country]
  );
  const matchupOpponent =
    Array.isArray(opponentRows) && opponentRows[0] && opponentRows[0].opponent_country
      ? String(opponentRows[0].opponent_country).toUpperCase()
      : country === "USA"
        ? "CAN"
        : "USA";

  const cases = [
    { name: "player_status", args: { player_id: playerA } },
    { name: "find_player", args: { name_query: "Smith", limit: 5 } },
    { name: "player_profile", args: { player_id: playerA } },
    { name: "full_player_profile", args: { player_id: playerA, teammate_limit: 5, similar_limit: 5, recent_matches_limit: 5 } },
    { name: "head_to_head_players", args: { player_a_id: playerA, player_b_id: playerB, limit: 5 } },
    { name: "partnership_profile", args: { player_a_id: playerA, player_b_id: playerB, limit: 5 } },
    { name: "similar_players", args: { player_id: playerA, limit: 5 } },
    { name: "explain_ranking", args: { player_a_id: playerA, player_b_id: playerB } },
    { name: "player_teammate_history", args: { player_id: playerA, limit: 5 } },
    { name: "most_improved_players", args: { country_code: country, limit: 5 } },
    { name: "country_depth_report", args: { country_code: country, top_n: 5 } },
    { name: "tournament_lookup", args: { query: country, limit: 5 } },
    ...(Number.isFinite(tournamentId) ? [{ name: "tournament_snapshot", args: { tournament_id: tournamentId } }] : []),
    { name: "top_players_by_country", args: { country_code: country, limit: 5 } },
    { name: "active_players", args: { country_code: country, limit: 5 } },
    { name: "inactive_players", args: { country_code: country, limit: 5 } },
    { name: "best_finishes_by_player", args: { player_name: "Smith", limit: 5 } },
    { name: "player_recent_matches", args: { player_name: "Smith", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "women", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "womens", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "women's", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "female", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "f", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "1", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "men", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "mens", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "men's", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "male", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "m", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: matchupOpponent, gender: "0", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "women", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "womens", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "women's", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "female", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "f", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "1", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "men", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "mens", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "men's", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "male", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "m", min_matches: 2, sort_by: "best", limit: 5 } },
    { name: "country_opponent_performance", args: { country_code: country, gender: "0", min_matches: 2, sort_by: "best", limit: 5 } },
  ];

  const results = [];
  for (const c of cases) {
    const t0 = Date.now();
    try {
      const out = await runTool(c.name, c.args);
      const ok = !(out && typeof out === "object" && !Array.isArray(out) && out.error);
      const rows = Array.isArray(out)
        ? out.length
        : out && Array.isArray(out.matches)
          ? out.matches.length
          : out && Array.isArray(out.top_players)
            ? out.top_players.length
            : null;
      results.push({
        tool: c.name,
        ok,
        duration_ms: Date.now() - t0,
        rows,
        error: ok ? null : String(out.error || "tool failed"),
      });
    } catch (err) {
      results.push({
        tool: c.name,
        ok: false,
        duration_ms: Date.now() - t0,
        rows: null,
        error: err.message || String(err),
      });
    }
  }

  return {
    ok: results.every((r) => r.ok),
    provider: activeProviderMeta(),
    elapsed_ms: Date.now() - startedAt,
    results,
  };
}

module.exports = { generateChatReply, runToolSmokeTests };
