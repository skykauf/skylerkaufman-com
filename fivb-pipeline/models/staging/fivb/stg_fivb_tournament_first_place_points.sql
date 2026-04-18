{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
-- One row per tournament: World Tour / VIS points for the champion (finishing_pos = 1).
-- Sourced from GetBeachTournamentRanking EarnedPointsTeam on the rank-1 row; null when API omits it.
select
    tournament_id,
    max(points) as first_place_points,
    max(ingested_at) as ingested_at
from {{ ref('stg_fivb_results') }}
where finishing_pos = 1
group by tournament_id
