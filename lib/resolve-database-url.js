/**
 * Server-side Postgres URL. Supabase API keys are not valid here.
 *
 * Set one of:
 *   DATABASE_URL — preferred (Supabase: Settings → Database → Connection string → URI)
 *   POSTGRES_URL, POSTGRES_PRISMA_URL, POSTGRES_URL_NON_POOLING, SUPABASE_DB_URL — common aliases
 * Or all of: SUPABASE_DB_HOST, SUPABASE_DB_PASSWORD (optional: SUPABASE_DB_USER, SUPABASE_DB_PORT, SUPABASE_DB_NAME)
 */

function resolveDatabaseUrl() {
  const candidates = [
    process.env.DATABASE_URL,
    process.env.POSTGRES_URL,
    process.env.POSTGRES_PRISMA_URL,
    process.env.POSTGRES_URL_NON_POOLING,
    process.env.SUPABASE_DB_URL,
  ];
  for (const c of candidates) {
    if (c && String(c).trim()) return String(c).trim();
  }

  const host = process.env.SUPABASE_DB_HOST && String(process.env.SUPABASE_DB_HOST).trim();
  const password = process.env.SUPABASE_DB_PASSWORD && String(process.env.SUPABASE_DB_PASSWORD).trim();
  if (host && password) {
    const user = encodeURIComponent(process.env.SUPABASE_DB_USER || "postgres");
    const encPass = encodeURIComponent(password);
    const port = process.env.SUPABASE_DB_PORT || "5432";
    const database = (process.env.SUPABASE_DB_NAME || "postgres").replace(/^\//, "");
    return `postgresql://${user}:${encPass}@${host}:${port}/${database}`;
  }

  return null;
}

module.exports = { resolveDatabaseUrl };
