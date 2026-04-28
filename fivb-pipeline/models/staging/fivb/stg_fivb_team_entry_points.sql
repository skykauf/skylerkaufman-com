{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
/*
  Current team entry/seeding points as of today.
  Per FIVB rule, team points are the sum of the two athletes' entry/seeding points.
*/
with player_points as (
    select
        player_id,
        as_of_date,
        entry_seeding_points,
        technical_points_365d
    from {{ ref('stg_fivb_player_entry_points') }}
),

params as (
    select current_date::date as as_of_date
),

teams as (
    select
        team_id,
        tournament_id,
        player_a_id,
        player_b_id,
        country_code
    from {{ ref('stg_fivb_teams') }}
)

select
    p.as_of_date,
    t.team_id,
    t.tournament_id,
    t.player_a_id,
    t.player_b_id,
    t.country_code,
    coalesce(pa.entry_seeding_points, 0)::numeric as player_a_entry_seeding_points,
    coalesce(pb.entry_seeding_points, 0)::numeric as player_b_entry_seeding_points,
    (
        coalesce(pa.entry_seeding_points, 0)
        + coalesce(pb.entry_seeding_points, 0)
    )::numeric as team_entry_seeding_points,
    coalesce(pa.technical_points_365d, 0)::numeric as player_a_technical_points_365d,
    coalesce(pb.technical_points_365d, 0)::numeric as player_b_technical_points_365d,
    (
        coalesce(pa.technical_points_365d, 0)
        + coalesce(pb.technical_points_365d, 0)
    )::numeric as team_technical_points_365d
from teams as t
cross join params as p
left join player_points as pa
    on pa.player_id = t.player_a_id
left join player_points as pb
    on pb.player_id = t.player_b_id
