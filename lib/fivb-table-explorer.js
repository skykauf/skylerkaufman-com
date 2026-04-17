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

async function getPlayerDirectory(filters = {}) {
  const db = getPool();
  if (!db) return { ok: false, skipped: true, reason: "DATABASE_URL not configured" };

  const limit = toInt(filters.limit, 12000, 100, 50000);
  const gender = normalizeGenderFilter(filters.gender);
  if (!gender) {
    return { ok: false, error: "Provide gender (male/female)." };
  }

  const sql = `
    SELECT
      player_id,
      full_name,
      country_code,
      gender
    FROM raw.raw_fivb_players
    WHERE gender::text = $1
      AND full_name IS NOT NULL
      AND trim(full_name) <> ''
    ORDER BY full_name ASC
    LIMIT $2
  `;
  const { rows } = await db.query(sql, [gender, limit]);
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
        score_sets,
        CASE
          WHEN (payload->>'NoPlayerA1') ~ '^\\d+$' THEN (payload->>'NoPlayerA1')::bigint
          ELSE NULL
        END AS no_player_a1,
        CASE
          WHEN (payload->>'NoPlayerA2') ~ '^\\d+$' THEN (payload->>'NoPlayerA2')::bigint
          ELSE NULL
        END AS no_player_a2,
        CASE
          WHEN (payload->>'NoPlayerB1') ~ '^\\d+$' THEN (payload->>'NoPlayerB1')::bigint
          ELSE NULL
        END AS no_player_b1,
        CASE
          WHEN (payload->>'NoPlayerB2') ~ '^\\d+$' THEN (payload->>'NoPlayerB2')::bigint
          ELSE NULL
        END AS no_player_b2
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
      pm.no_player_a1,
      pm.no_player_a2,
      pm.no_player_b1,
      pm.no_player_b2,
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

function normalizeSetRows(rows) {
  const total = rows.reduce((sum, row) => sum + Number(row.rows || 0), 0);
  if (!total) return [];
  return rows.map((row) => ({
    loser_points: Number(row.loser_points),
    rows: Number(row.rows),
    pct: Number(row.rows) / total,
  }));
}

async function getMatchupCalibration() {
  const db = getPool();
  if (!db) return { ok: false, skipped: true, reason: "DATABASE_URL not configured" };

  const sql = `
    WITH base AS (
      SELECT
        CASE
          WHEN (payload->>'PointsTeamASet1') ~ '^\\d+$' THEN (payload->>'PointsTeamASet1')::int
          ELSE NULL
        END AS a1,
        CASE
          WHEN (payload->>'PointsTeamBSet1') ~ '^\\d+$' THEN (payload->>'PointsTeamBSet1')::int
          ELSE NULL
        END AS b1,
        CASE
          WHEN (payload->>'PointsTeamASet2') ~ '^\\d+$' THEN (payload->>'PointsTeamASet2')::int
          ELSE NULL
        END AS a2,
        CASE
          WHEN (payload->>'PointsTeamBSet2') ~ '^\\d+$' THEN (payload->>'PointsTeamBSet2')::int
          ELSE NULL
        END AS b2,
        CASE
          WHEN (payload->>'PointsTeamASet3') ~ '^\\d+$' THEN (payload->>'PointsTeamASet3')::int
          ELSE NULL
        END AS a3,
        CASE
          WHEN (payload->>'PointsTeamBSet3') ~ '^\\d+$' THEN (payload->>'PointsTeamBSet3')::int
          ELSE NULL
        END AS b3
      FROM raw.raw_fivb_matches
    ),
    sets AS (
      SELECT 1 AS set_no, GREATEST(a1, b1) AS winner_points, LEAST(a1, b1) AS loser_points
      FROM base
      WHERE a1 IS NOT NULL AND b1 IS NOT NULL AND GREATEST(a1, b1) >= 21 AND ABS(a1 - b1) >= 2
      UNION ALL
      SELECT 2 AS set_no, GREATEST(a2, b2) AS winner_points, LEAST(a2, b2) AS loser_points
      FROM base
      WHERE a2 IS NOT NULL AND b2 IS NOT NULL AND GREATEST(a2, b2) >= 21 AND ABS(a2 - b2) >= 2
      UNION ALL
      SELECT 3 AS set_no, GREATEST(a3, b3) AS winner_points, LEAST(a3, b3) AS loser_points
      FROM base
      WHERE a3 IS NOT NULL AND b3 IS NOT NULL AND GREATEST(a3, b3) >= 15 AND ABS(a3 - b3) >= 2
    )
    SELECT
      CASE WHEN set_no = 3 THEN 15 ELSE 21 END AS target_points,
      (winner_points > CASE WHEN set_no = 3 THEN 15 ELSE 21 END) AS is_overtime,
      loser_points,
      COUNT(*)::bigint AS rows
    FROM sets
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 4 DESC, 3 DESC
  `;

  const { rows } = await db.query(sql);
  const grouped = {
    set21: { regular: [], overtime: [] },
    set15: { regular: [], overtime: [] },
  };

  for (const row of rows) {
    const target = Number(row.target_points) === 15 ? "set15" : "set21";
    const bucket = row.is_overtime ? "overtime" : "regular";
    grouped[target][bucket].push(row);
  }

  const out = {
    set21: {
      regular: normalizeSetRows(grouped.set21.regular),
      overtime: normalizeSetRows(grouped.set21.overtime),
    },
    set15: {
      regular: normalizeSetRows(grouped.set15.regular),
      overtime: normalizeSetRows(grouped.set15.overtime),
    },
  };

  return { ok: true, calibration: out };
}

async function runFivbTableExplorer(action, filters = {}) {
  try {
    if (action === "search_players") {
      return await searchPlayers(filters);
    }
    if (action === "player_directory") {
      return await getPlayerDirectory(filters);
    }
    if (action === "player_history") {
      return await getPlayerHistory(filters);
    }
    if (action === "matchup_calibration") {
      return await getMatchupCalibration();
    }
    return {
      ok: false,
      error:
        "Unsupported action. Use 'search_players', 'player_directory', 'player_history', or 'matchup_calibration'.",
    };
  } catch (err) {
    return { ok: false, error: err.message || String(err) };
  }
}

module.exports = { runFivbTableExplorer };
