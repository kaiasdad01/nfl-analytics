{{ config(materialized='view') }}

-- Team game-level statistics aggregated from play-by-play data
with base_pbp as (
  select
    game_id,
    season,
    week,
    posteam as team,
    play_type,
    yards_gained,
    touchdown,
    first_down,
    interception,
    fumble_lost,
    penalty,
    epa,
    success,
    down,
    yardline_100
  from {{ ref('stg_pbp') }}
  where posteam is not null
),

game_context as (
  select
    game_id,
    home_team,
    away_team
  from {{ ref('stg_games') }}
),

team_stats_by_game as (
  select
    pbp.game_id,
    pbp.season,
    pbp.week,
    pbp.team,

    -- Determine opponent and home/away status
    case
      when g.home_team = pbp.team then g.away_team
      else g.home_team
    end as opponent,

    case
      when g.home_team = pbp.team then 'home'
      else 'away'
    end as home_away,

    -- Offensive statistics
    sum(pbp.yards_gained) as total_yards,
    sum(case when pbp.play_type = 'pass' then pbp.yards_gained else 0 end) as passing_yards,
    sum(case when pbp.play_type = 'run' then pbp.yards_gained else 0 end) as rushing_yards,
    sum(case when pbp.touchdown = 1 then 6 else 0 end) as points_scored,
    sum(case when pbp.interception = 1 or pbp.fumble_lost = 1 then 1 else 0 end) as turnovers,
    sum(case when pbp.penalty = 1 then 1 else 0 end) as penalties,

    -- Efficiency metrics
    sum(case when pbp.play_type in ('pass', 'run') and pbp.down = 3 and pbp.first_down = 1 then 1 else 0 end) as third_down_conversions,
    sum(case when pbp.play_type in ('pass', 'run') and pbp.down = 3 then 1 else 0 end) as third_down_attempts,
    sum(case when pbp.yardline_100 <= 20 and pbp.touchdown = 1 then 1 else 0 end) as red_zone_scores,
    sum(case when pbp.yardline_100 <= 20 then 1 else 0 end) as red_zone_attempts,

    -- Advanced metrics
    sum(pbp.epa) as total_epa,
    avg(pbp.epa) as avg_epa,
    sum(case when pbp.success = 1 then 1 else 0 end) as successful_plays,
    count(*) as total_plays

  from base_pbp pbp
  join game_context g on pbp.game_id = g.game_id
  where pbp.play_type in ('pass', 'run', 'punt', 'field_goal')
  group by 1, 2, 3, 4, 5, 6
)

select
  game_id,
  season,
  week,
  team,
  opponent,
  home_away,
  total_yards,
  passing_yards,
  rushing_yards,
  points_scored,
  turnovers,
  penalties,
  third_down_conversions,
  third_down_attempts,
  red_zone_scores,
  red_zone_attempts,
  total_epa,
  avg_epa,
  successful_plays,
  total_plays,

  -- Derived efficiency metrics
  case
    when third_down_attempts > 0 then
      round(cast(third_down_conversions as float64) / cast(third_down_attempts as float64), 3)
    else null
  end as third_down_conversion_rate,

  case
    when red_zone_attempts > 0 then
      round(cast(red_zone_scores as float64) / cast(red_zone_attempts as float64), 3)
    else null
  end as red_zone_efficiency,

  case
    when total_plays > 0 then
      round(cast(successful_plays as float64) / cast(total_plays as float64), 3)
    else null
  end as success_rate

from team_stats_by_game
order by season desc, week desc, team