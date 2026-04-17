const { Client } = require("pg");
const { resolveDatabaseUrl } = require("./resolve-database-url");

function pgSslOption() {
  if (process.env.PGSSLMODE === "disable") return false;
  return { rejectUnauthorized: false };
}

async function runQuery(client, text) {
  const { rows } = await client.query(text);
  return rows;
}

async function getFivbProfile() {
  const connectionString = resolveDatabaseUrl();
  if (!connectionString) {
    return { ok: false, skipped: true, reason: "DATABASE_URL not configured" };
  }

  const client = new Client({
    connectionString,
    ssl: pgSslOption(),
    connectionTimeoutMillis: 8000,
  });

  try {
    await client.connect();

    const freshness = await runQuery(
      client,
      `
      select 'raw_fivb_events' as table_name, max(ingested_at) as latest_ingested_at, count(*)::bigint as rows from raw.raw_fivb_events
      union all
      select 'raw_fivb_matches', max(ingested_at), count(*)::bigint from raw.raw_fivb_matches
      union all
      select 'raw_fivb_players', max(ingested_at), count(*)::bigint from raw.raw_fivb_players
      union all
      select 'raw_fivb_results', max(ingested_at), count(*)::bigint from raw.raw_fivb_results
      union all
      select 'raw_fivb_rounds', max(ingested_at), count(*)::bigint from raw.raw_fivb_rounds
      union all
      select 'raw_fivb_team_rankings', max(ingested_at), count(*)::bigint from raw.raw_fivb_team_rankings
      union all
      select 'raw_fivb_teams', max(ingested_at), count(*)::bigint from raw.raw_fivb_teams
      union all
      select 'raw_fivb_tournaments', max(ingested_at), count(*)::bigint from raw.raw_fivb_tournaments
      order by table_name
      `
    );

    const nullRates = await runQuery(
      client,
      `
      with players as (
        select
          count(*)::numeric as total_rows,
          count(*) filter (where player_id is null)::numeric as null_player_id,
          count(*) filter (where full_name is null or btrim(full_name) = '')::numeric as null_full_name,
          count(*) filter (where country_code is null or btrim(country_code) = '')::numeric as null_country_code,
          count(*) filter (where gender is null)::numeric as null_gender,
          count(*) filter (where ingested_at is null)::numeric as null_ingested_at
        from raw.raw_fivb_players
      ),
      matches as (
        select
          count(*)::numeric as total_rows,
          count(*) filter (where match_id is null)::numeric as null_match_id,
          count(*) filter (where tournament_id is null)::numeric as null_tournament_id,
          count(*) filter (where played_at is null)::numeric as null_played_at,
          count(*) filter (where winner_team_id is null)::numeric as null_winner_team_id,
          count(*) filter (where score_sets is null or btrim(score_sets) = '')::numeric as null_score_sets,
          count(*) filter (where ingested_at is null)::numeric as null_ingested_at
        from raw.raw_fivb_matches
      )
      select
        'raw.raw_fivb_players' as table_name,
        total_rows::bigint as total_rows,
        jsonb_build_object(
          'player_id', round(100 * null_player_id / nullif(total_rows, 0), 2),
          'full_name', round(100 * null_full_name / nullif(total_rows, 0), 2),
          'country_code', round(100 * null_country_code / nullif(total_rows, 0), 2),
          'gender', round(100 * null_gender / nullif(total_rows, 0), 2),
          'ingested_at', round(100 * null_ingested_at / nullif(total_rows, 0), 2)
        ) as null_pct
      from players
      union all
      select
        'raw.raw_fivb_matches' as table_name,
        total_rows::bigint as total_rows,
        jsonb_build_object(
          'match_id', round(100 * null_match_id / nullif(total_rows, 0), 2),
          'tournament_id', round(100 * null_tournament_id / nullif(total_rows, 0), 2),
          'played_at', round(100 * null_played_at / nullif(total_rows, 0), 2),
          'winner_team_id', round(100 * null_winner_team_id / nullif(total_rows, 0), 2),
          'score_sets', round(100 * null_score_sets / nullif(total_rows, 0), 2),
          'ingested_at', round(100 * null_ingested_at / nullif(total_rows, 0), 2)
        ) as null_pct
      from matches
      `
    );

    const topCountries = await runQuery(
      client,
      `
      select coalesce(country_code, 'UNK') as country_code, count(*)::bigint as players
      from raw.raw_fivb_players
      group by 1
      order by 2 desc
      limit 15
      `
    );

    const eloBuckets = await runQuery(
      client,
      `
      select
        width_bucket(elo_rating::numeric, 1200, 2000, 8) as bucket,
        min(elo_rating::numeric) as min_elo,
        max(elo_rating::numeric) as max_elo,
        count(*)::bigint as rows
      from core.player_elo_history
      where elo_rating is not null
      group by 1
      order by 1
      `
    );

    const matchesByYear = await runQuery(
      client,
      `
      select
        case
          when (payload->>'LocalDate') ~ '^\\d{4}-\\d{2}-\\d{2}$'
            then extract(year from (payload->>'LocalDate')::date)::int
          else -1
        end as year,
        count(*)::bigint as matches
      from raw.raw_fivb_matches
      group by 1
      order by 1
      `
    );

    return {
      ok: true,
      generated_at: new Date().toISOString(),
      freshness,
      null_rates: nullRates,
      top_countries: topCountries,
      elo_buckets: eloBuckets,
      matches_by_year: matchesByYear,
    };
  } finally {
    try {
      await client.end();
    } catch (_) {
      /* ignore */
    }
  }
}

module.exports = { getFivbProfile };
