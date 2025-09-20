{{ config(materialized='view') }}

select
  player_id,
  season,
  week,
  recent_team as team,
  position,

  -- Passing stats
  attempts as passing_attempts,
  completions as passing_completions,
  passing_yards,
  passing_tds,
  interceptions,

  -- Rushing stats
  carries as rushing_attempts,
  rushing_yards,
  rushing_tds,

  -- Receiving stats
  targets,
  receptions,
  receiving_yards,
  receiving_tds,

  -- Fantasy stats
  fantasy_points,
  fantasy_points_ppr,

  -- Derived stats
  case
    when attempts > 0 then
      round(cast(completions as float64) / cast(attempts as float64), 3)
    else null
  end as completion_percentage,

  case
    when attempts > 0 then
      round(cast(passing_yards as float64) / cast(attempts as float64), 1)
    else null
  end as yards_per_attempt,

  case
    when carries > 0 then
      round(cast(rushing_yards as float64) / cast(carries as float64), 1)
    else null
  end as yards_per_carry,

  case
    when targets > 0 then
      round(cast(receptions as float64) / cast(targets as float64), 3)
    else null
  end as catch_rate,

  case
    when receptions > 0 then
      round(cast(receiving_yards as float64) / cast(receptions as float64), 1)
    else null
  end as yards_per_reception

from {{ source('nfl_data', 'player_stats') }}
where player_id is not null
order by season desc, week desc, fantasy_points desc