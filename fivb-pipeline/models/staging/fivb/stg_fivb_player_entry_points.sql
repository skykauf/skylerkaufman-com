{{
    config(
        materialized='view',
        tags=['staging', 'fivb'],
    )
}}
/*
  Current athlete entry/seeding points as of today, following FIVB "best 3 of last 4"
  within the trailing 365-day window.

  Implementation notes:
  - Base events: Beach Pro Tour Elite/Challenge/Future + Senior World Championships.
  - Other sanctioned events are included only when they improve standing.
  - Technical points tie-breaker = sum of all tournament points in last 365 days.
*/
with params as (
    select current_date::date as as_of_date
),

tournaments as (
    select
        t.tournament_id,
        t.name as tournament_name,
        t.tier as tournament_tier,
        t.start_date,
        t.end_date,
        coalesce(t.end_date, t.start_date) as points_awarded_date,
        nullif(trim(t.payload::jsonb->> 'NoEvent'), '')::bigint as event_id
    from {{ ref('stg_fivb_tournaments') }} as t
),

event_names as (
    select
        e.event_id,
        e.name as event_name
    from {{ ref('stg_fivb_events') }} as e
),

tournament_classification as (
    select
        t.tournament_id,
        t.points_awarded_date,
        t.tournament_name,
        t.tournament_tier,
        en.event_name,
        (
            lower(coalesce(t.tournament_tier, '')) in ('51', '52', '53', 'elite16', 'challenge', 'future')
            or lower(coalesce(t.tournament_name, '')) similar to '%(elite|challenge|future)%'
            or lower(coalesce(t.tournament_tier, '')) similar to '%(elite|challenge|future)%'
        ) as is_bpt_event,
        (
            (
                lower(coalesce(t.tournament_tier, '')) similar to '%world championship%'
                or lower(coalesce(t.tournament_name, '')) similar to '%world championship%'
                or lower(coalesce(en.event_name, '')) similar to '%world championship%'
            )
            and lower(coalesce(t.tournament_name, '')) not similar to '%(u19|u21|junior|youth)%'
            and lower(coalesce(en.event_name, '')) not similar to '%(u19|u21|junior|youth)%'
        ) as is_senior_worlds_event
    from tournaments as t
    left join event_names as en
        on en.event_id = t.event_id
),

player_tournament_points as (
    select
        pl.player_id,
        r.tournament_id,
        tc.points_awarded_date,
        r.points::numeric as ranking_points,
        (tc.is_bpt_event or tc.is_senior_worlds_event) as is_base_event
    from {{ ref('stg_fivb_results') }} as r
    inner join {{ ref('stg_fivb_teams') }} as tm
        on
            tm.tournament_id = r.tournament_id
            and tm.team_id = r.team_id
    cross join lateral (
        values (tm.player_a_id), (tm.player_b_id)
    ) as pl(player_id)
    inner join tournament_classification as tc
        on tc.tournament_id = r.tournament_id
    cross join params as p
    where
        pl.player_id is not null
        and r.points is not null
        and tc.points_awarded_date is not null
        and tc.points_awarded_date <= p.as_of_date
        and tc.points_awarded_date > (p.as_of_date - interval '365 days')
),

scored_with_windows as (
    select
        ptp.*,
        row_number() over (
            partition by ptp.player_id, ptp.is_base_event
            order by ptp.points_awarded_date desc, ptp.tournament_id desc
        ) as recent_rank_by_scope
    from player_tournament_points as ptp
),

base_recent_four_with_dates as (
    select
        s.player_id,
        s.points_awarded_date,
        s.ranking_points
    from scored_with_windows as s
    where
        s.is_base_event
        and s.recent_rank_by_scope <= 4
),

base_recent_four_cutoff as (
    select
        player_id,
        min(points_awarded_date) as min_base_recent_four_date
    from base_recent_four_with_dates
    group by player_id
),

base_last_four as (
    select
        b.player_id,
        b.ranking_points
    from base_recent_four_with_dates as b
),

all_events_eligible_for_comparison as (
    select
        ptp.player_id,
        ptp.tournament_id,
        ptp.points_awarded_date,
        ptp.ranking_points
    from player_tournament_points as ptp
    inner join base_recent_four_cutoff as c
        on c.player_id = ptp.player_id
    where
        ptp.is_base_event
        or ptp.points_awarded_date >= c.min_base_recent_four_date
),

all_last_four as (
    select
        s.player_id,
        s.ranking_points
    from (
        select
            ptp.*,
            row_number() over (
                partition by ptp.player_id
                order by ptp.points_awarded_date desc, ptp.tournament_id desc
            ) as recent_rank_all
        from all_events_eligible_for_comparison as ptp
    ) as s
    where s.recent_rank_all <= 4
),

base_best_three as (
    select
        player_id,
        sum(ranking_points) as base_best_three_points
    from (
        select
            b.*,
            row_number() over (
                partition by b.player_id
                order by b.ranking_points desc, b.player_id
            ) as best_rank
        from base_last_four as b
    ) as ranked
    where best_rank <= 3
    group by player_id
),

all_best_three as (
    select
        player_id,
        sum(ranking_points) as all_events_best_three_points
    from (
        select
            a.*,
            row_number() over (
                partition by a.player_id
                order by a.ranking_points desc, a.player_id
            ) as best_rank
        from all_last_four as a
    ) as ranked
    where best_rank <= 3
    group by player_id
),

technical_points as (
    select
        player_id,
        sum(ranking_points) as technical_points_365d
    from player_tournament_points
    group by player_id
),

base_event_counts as (
    select
        player_id,
        count(*) as base_events_in_365d
    from player_tournament_points
    where is_base_event
    group by player_id
),

all_event_counts as (
    select
        player_id,
        count(*) as all_events_in_365d
    from player_tournament_points
    group by player_id
)

select
    p.as_of_date,
    pl.player_id,
    pl.full_name as player_name,
    pl.country_code,
    pl.gender,
    coalesce(tp.technical_points_365d, 0)::numeric as technical_points_365d,
    coalesce(bb.base_best_three_points, 0)::numeric as base_best_three_points,
    coalesce(ab.all_events_best_three_points, 0)::numeric as all_events_best_three_points,
    greatest(
        coalesce(bb.base_best_three_points, 0),
        coalesce(ab.all_events_best_three_points, 0)
    )::numeric as entry_seeding_points,
    (coalesce(ab.all_events_best_three_points, 0) > coalesce(bb.base_best_three_points, 0)) as includes_other_events_for_improvement,
    coalesce(bc.base_events_in_365d, 0) as base_events_in_365d,
    coalesce(ac.all_events_in_365d, 0) as all_events_in_365d
from {{ ref('stg_fivb_players') }} as pl
cross join params as p
left join technical_points as tp
    on tp.player_id = pl.player_id
left join base_best_three as bb
    on bb.player_id = pl.player_id
left join all_best_three as ab
    on ab.player_id = pl.player_id
left join base_event_counts as bc
    on bc.player_id = pl.player_id
left join all_event_counts as ac
    on ac.player_id = pl.player_id
