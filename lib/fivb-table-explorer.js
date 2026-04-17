const { Pool } = require("pg");
const { resolveDatabaseUrl } = require("./resolve-database-url");

let pool;
let poolForUrl;

function normalizeUrlForNodePg(url) {
  if (!url || typeof url !== "string") return url;
  let raw = url.trim();
  if (raw.startsWith("postgres://")) {
    raw = `postgresql://${raw.slice("postgres://".length)}`;
  }
  try {
    const parsed = new URL(raw);
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
      connectionTimeoutMillis: 7000,
      ssl: sslConfigForUrl(url),
    });
  }
  return pool;
}

function toInt(value, fallback, min, max) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  const clamped = Math.floor(n);
  return Math.min(max, Math.max(min, clamped));
}

function normalizeGenderFilter(value) {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  if (!raw || raw === "either" || raw === "all") return null;
  if (raw === "male" || raw === "m" || raw === "0") return "0";
  if (raw === "female" || raw === "f" || raw === "1") return "1";
  return null;
}

async function searchPlayers(filters = {}) {
  const db = getPool();
  if (!db) return { ok: false, skipped: true, reason: "DATABASE_URL not configured" };

  const limit = toInt(filters.limit, 50, 1, 200);
  const params = [];
  const where = [];

  if (filters.name) {
    params.push(`%${String(filters.name).trim()}%`);
    where.push(`full_name ILIKE $${params.length}`);
  }
  if (filters.country_code) {
    params.push(String(filters.country_code).trim().toUpperCase());
    where.push(`country_code = $${params.length}`);
  }
  const gender = normalizeGenderFilter(filters.gender);
  if (gender) {
    params.push(gender);
    where.push(`gender::text = $${params.length}`);
  }

  params.push(limit);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const sql = `
    SELECT
      player_id,
      full_name,
      first_name,
      last_name,
      country_code,
      gender,
      birth_date,
      height_cm,
      CASE
        WHEN (payload->>'Weight') ~ '^\\d+(\\.\\d+)?$'
        THEN round(((payload->>'Weight')::numeric / 1000000)::numeric, 1)
        ELSE NULL
      END AS weight_kg_est,
      ingested_at
    FROM raw.raw_fivb_players
    ${whereSql}
    ORDER BY full_name ASC
    LIMIT $${params.length}
  `;

  const { rows } = await db.query(sql, params);
  return { ok: true, rows };
}

async function getPlayerHistory(filters = {}) {
  const db = getPool();
  if (!db) return { ok: false, skipped: true, reason: "DATABASE_URL not configured" };

  const historyLimit = toInt(filters.history_limit, 200, 1, 1000);
  let playerId = filters.player_id ? String(filters.player_id).trim() : null;
  if (playerId && !/^\d+$/.test(playerId)) {
    return { ok: false, error: "player_id must be numeric digits." };
  }

  if (!playerId && filters.player_name) {
    const lookup = await db.query(
      `
      SELECT player_id
      FROM raw.raw_fivb_players
      WHERE full_name ILIKE $1
      ORDER BY full_name ASC
      LIMIT 1
      `,
      [`%${String(filters.player_name).trim()}%`]
    );
    playerId = lookup.rows?.[0]?.player_id ? String(lookup.rows[0].player_id) : null;
  }

  if (!playerId) {
    return { ok: false, error: "Provide player_id or player_name to load history." };
  }

  const profile = await db.query(
    `
    SELECT
      player_id,
      full_name,
      country_code,
      gender,
      birth_date,
      height_cm,
      CASE
        WHEN (payload->>'Weight') ~ '^\\d+(\\.\\d+)?$'
        THEN round(((payload->>'Weight')::numeric / 1000000)::numeric, 1)
        ELSE NULL
      END AS weight_kg_est
    FROM raw.raw_fivb_players
    WHERE player_id = $1::bigint
    LIMIT 1
    `,
    [playerId]
  );

  const timeline = await db.query(
    `
    WITH player_matches AS (
      SELECT
        match_id,
        payload->>'LocalDate' AS local_date,
        payload->>'TournamentName' AS tournament_name,
        payload->>'RoundName' AS round_name,
        payload->>'TeamAName' AS team_a_name,
        payload->>'TeamBName' AS team_b_name,
        score_sets
      FROM raw.raw_fivb_matches
      WHERE
        CASE WHEN (payload->>'NoPlayerA1') ~ '^\\d+$' THEN (payload->>'NoPlayerA1')::bigint END = $1::bigint
        OR CASE WHEN (payload->>'NoPlayerA2') ~ '^\\d+$' THEN (payload->>'NoPlayerA2')::bigint END = $1::bigint
        OR CASE WHEN (payload->>'NoPlayerB1') ~ '^\\d+$' THEN (payload->>'NoPlayerB1')::bigint END = $1::bigint
        OR CASE WHEN (payload->>'NoPlayerB2') ~ '^\\d+$' THEN (payload->>'NoPlayerB2')::bigint END = $1::bigint
    ),
    player_elo AS (
      SELECT
        match_id,
        as_of_date,
        elo_rating,
        gender AS elo_gender
      FROM core.player_elo_history
      WHERE player_id = $1::bigint
    )
    SELECT
      COALESCE(pm.match_id, pe.match_id)::text AS match_id,
      pm.local_date,
      pm.tournament_name,
      pm.round_name,
      pm.team_a_name,
      pm.team_b_name,
      pm.score_sets,
      pe.as_of_date,
      pe.elo_rating,
      pe.elo_gender
    FROM player_matches pm
    FULL OUTER JOIN player_elo pe
      ON pe.match_id = pm.match_id
    ORDER BY
      COALESCE(
        CASE
          WHEN pm.local_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
          THEN pm.local_date::date::timestamp
          ELSE NULL
        END,
        pe.as_of_date
      ) DESC NULLS LAST,
      COALESCE(pm.match_id, pe.match_id) DESC
    LIMIT $2
    `,
    [playerId, historyLimit]
  );

  return {
    ok: true,
    player_id: playerId,
    profile: profile.rows?.[0] || null,
    timeline: timeline.rows || [],
  };
}

async function runFivbTableExplorer(action, filters = {}) {
  try {
    if (action === "search_players") {
      return await searchPlayers(filters);
    }
    if (action === "player_history") {
      return await getPlayerHistory(filters);
    }
    return {
      ok: false,
      error: "Unsupported action. Use 'search_players' or 'player_history'.",
    };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
}

module.exports = { runFivbTableExplorer };
