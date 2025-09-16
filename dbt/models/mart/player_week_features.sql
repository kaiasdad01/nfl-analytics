{{ config(materialized='view') }}

with base as (
  select
      cast(player_id as string)           as player_id
    , cast(season as int64)               as season
    , cast(week as int64)                 as week
    , cast(season_type as string)         as season_type
    , cast(recent_team as string)         as team
    , cast(position as string)            as position
    , cast(fantasy_points as float64)     as fantasy_points
    , cast(fantasy_points_ppr as float64) as fantasy_points_ppr

    -- passing
    , cast(completions as int64)          as completions
    , cast(attempts as int64)             as attempts
    , cast(passing_yards as int64)        as passing_yards
    , cast(passing_tds as int64)          as passing_tds
    , cast(interceptions as int64)        as interceptions

    -- rushing
    , cast(carries as int64)              as rush_attempts
    , cast(rushing_yards as int64)        as rushing_yards
    , cast(rushing_tds as int64)          as rushing_tds

    -- receiving
    , cast(receptions as int64)           as receptions
    , cast(targets as int64)              as targets
    , cast(receiving_yards as int64)      as receiving_yards
    , cast(receiving_tds as int64)        as receiving_tds
  from {{ ref('stg_player_stats') }}
)
, with_roster as (
  select
      b.player_id
    , coalesce(b.team, r.team)            as team
    , b.season
    , b.week
    , b.season_type
    , coalesce(b.position, r.position)    as position
    , b.fantasy_points
    , b.fantasy_points_ppr
    , b.completions
    , b.attempts
    , b.passing_yards
    , b.passing_tds
    , b.interceptions
    , b.rush_attempts
    , b.rushing_yards
    , b.rushing_tds
    , b.receptions
    , b.targets
    , b.receiving_yards
    , b.receiving_tds
  from base b
  left join {{ ref('stg_rosters') }} r
    on r.player_id = b.player_id
   and r.season    = b.season
)
select * from with_roster