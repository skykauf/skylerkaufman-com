const { Pool } = require("pg");
const { resolveDatabaseUrl } = require("./resolve-database-url");

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";
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
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: !out?.error,
      rows: Array.isArray(out) ? out.length : null,
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
      ok: !out?.error,
      rows: Array.isArray(out) ? out.length : null,
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
      ok: !out?.error,
      rows: Array.isArray(out) ? out.length : null,
      duration_ms: Date.now() - startedAt,
    });
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
    sql += ` ORDER BY last_match_played_at DESC NULLS LAST, elo_rating DESC NULLS LAST LIMIT $${params.length}`;

    const out = await queryRows(sql, params);
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: !out?.error,
      rows: Array.isArray(out) ? out.length : null,
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
    sql += ` ORDER BY last_match_played_at DESC NULLS LAST, elo_rating DESC NULLS LAST LIMIT $${params.length}`;

    const out = await queryRows(sql, params);
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: !out?.error,
      rows: Array.isArray(out) ? out.length : null,
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
      ok: !out?.error,
      rows: Array.isArray(out) ? out.length : null,
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
      ok: !out?.error,
      rows: Array.isArray(out) ? out.length : null,
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
      emitProgress(onProgress, { type: "tool_done", tool: name, ok: false, rows: null, duration_ms: Date.now() - startedAt });
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
    emitProgress(onProgress, { type: "tool_done", tool: name, ok: true, rows: rows.length, duration_ms: Date.now() - startedAt });
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
      ok: !out?.error,
      rows: Array.isArray(out) ? out.length : null,
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
      ok: !out?.error,
      rows: Array.isArray(out) ? out.length : null,
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
      emitProgress(onProgress, { type: "tool_done", tool: name, ok: false, rows: null, duration_ms: Date.now() - startedAt });
      return errOut;
    }
    const out = { summary: summaryRows[0] || {}, top_players: topRows };
    emitProgress(onProgress, { type: "tool_done", tool: name, ok: true, rows: topRows.length, duration_ms: Date.now() - startedAt });
    return out;
  }

  if (name === "query_db") {
    const sql = String(args.sql || "");
    if (!isSafeReadOnlySql(sql)) return { error: "Only read-only SELECT/CTE SQL is allowed." };
    const out = await queryRows(sql, []);
    emitProgress(onProgress, {
      type: "tool_done",
      tool: name,
      ok: !out?.error,
      rows: Array.isArray(out) ? out.length : null,
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
  const firstStart = Date.now();
  emitProgress(onProgress, { type: "model_start", stage: "hf_first_pass", provider: meta.provider, model: meta.model });
  const first = await callHf(buildHfPrompt(clean));
  emitProgress(onProgress, { type: "model_done", stage: "hf_first_pass", duration_ms: Date.now() - firstStart });
  const toolCall = extractHfToolCall(first);
  if (!toolCall) return { status: 200, body: { role: "assistant", content: first, ...meta } };
  const rows = await runTool("query_db", { sql: toolCall.sql }, onProgress);
  const secondStart = Date.now();
  emitProgress(onProgress, { type: "model_start", stage: "hf_second_pass", provider: meta.provider, model: meta.model });
  const second = await callHf(buildHfPrompt(clean, rows));
  emitProgress(onProgress, { type: "model_done", stage: "hf_second_pass", duration_ms: Date.now() - secondStart });
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

  const cases = [
    { name: "find_player", args: { name_query: "Smith", limit: 5 } },
    { name: "player_profile", args: { player_id: playerA } },
    { name: "head_to_head_players", args: { player_a_id: playerA, player_b_id: playerB, limit: 5 } },
    { name: "player_teammate_history", args: { player_id: playerA, limit: 5 } },
    { name: "most_improved_players", args: { country_code: country, limit: 5 } },
    { name: "country_depth_report", args: { country_code: country, top_n: 5 } },
    { name: "top_players_by_country", args: { country_code: country, limit: 5 } },
    { name: "active_players", args: { country_code: country, limit: 5 } },
    { name: "inactive_players", args: { country_code: country, limit: 5 } },
    { name: "best_finishes_by_player", args: { player_name: "Smith", limit: 5 } },
    { name: "player_recent_matches", args: { player_name: "Smith", limit: 5 } },
    { name: "country_matchup_record", args: { country_a: country, country_b: "BRA", limit: 5 } },
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
