const { Client } = require("pg");
const { resolveDatabaseUrl } = require("./resolve-database-url");

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

function isExpectedBootstrapError(err) {
  if (!err) return false;
  const pgCode = typeof err.code === "string" ? err.code : "";
  if (["28P01", "3D000", "42501", "53300"].includes(pgCode)) return true;
  const networkCode = typeof err.errno === "string" ? err.errno : "";
  if (["ECONNREFUSED", "ENOTFOUND", "EHOSTUNREACH", "ETIMEDOUT"].includes(networkCode)) return true;
  const message = String(err.message || "").toLowerCase();
  return (
    message.includes("connection terminated") ||
    message.includes("timeout expired") ||
    message.includes("password authentication failed") ||
    message.includes("self-signed certificate") ||
    message.includes("certificate")
  );
}

/**
 * @returns {Promise<{ ok: boolean, skipped?: boolean, reason?: string }>}
 */
async function bootstrapSupabase() {
  const connectionString = normalizeUrlForNodePg(resolveDatabaseUrl());
  if (!connectionString) {
    return { ok: false, skipped: true, reason: "DATABASE_URL not configured" };
  }

  const client = new Client({
    connectionString,
    ssl: sslConfigForUrl(connectionString),
    connectionTimeoutMillis: 8000,
  });

  try {
    await client.connect();
    await client.query(SETUP_SQL);
    return { ok: true };
  } catch (err) {
    if (isExpectedBootstrapError(err)) {
      return {
        ok: false,
        skipped: true,
        reason: `Bootstrap unavailable: ${err.message || "database not reachable"}`,
      };
    }
    throw err;
  } finally {
    try {
      await client.end();
    } catch (_) {
      /* ignore */
    }
  }
}

module.exports = { bootstrapSupabase };
