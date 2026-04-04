-- Optional reference: the same DDL runs automatically from the site via GET /api/bootstrap-supabase
-- when DATABASE_URL is set (e.g. on Vercel). You can still run this in the SQL editor if you prefer.
-- The ETL also creates raw; these grants help dbt on staging/core/mart. Adjust if your login role is not postgres.

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
