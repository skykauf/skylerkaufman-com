const { Client } = require("pg");

/**
 * Idempotent DDL for FIVB pipeline schemas (matches fivb-pipeline/supabase_setup.sql).
 * Safe to run on every request; typically completes in a few milliseconds.
 */
const SETUP_SQL = `
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
DO $grant$
BEGIN
  EXECUTE format('GRANT USAGE, CREATE ON SCHEMA raw TO %I', current_user);
  EXECUTE format('GRANT USAGE, CREATE ON SCHEMA staging TO %I', current_user);
  EXECUTE format('GRANT USAGE, CREATE ON SCHEMA core TO %I', current_user);
  EXECUTE format('GRANT USAGE, CREATE ON SCHEMA mart TO %I', current_user);
END
$grant$;
`;

function pgSslOption() {
  if (process.env.PGSSLMODE === "disable") return false;
  return { rejectUnauthorized: false };
}

/**
 * @returns {Promise<{ ok: boolean, skipped?: boolean, reason?: string }>}
 */
async function bootstrapSupabase() {
  const connectionString = process.env.DATABASE_URL;
  if (!connectionString || String(connectionString).trim() === "") {
    return { ok: false, skipped: true, reason: "DATABASE_URL not configured" };
  }

  const client = new Client({
    connectionString,
    ssl: pgSslOption(),
    connectionTimeoutMillis: 8000,
  });

  try {
    await client.connect();
    await client.query(SETUP_SQL);
    return { ok: true };
  } finally {
    try {
      await client.end();
    } catch (_) {
      /* ignore */
    }
  }
}

module.exports = { bootstrapSupabase };
