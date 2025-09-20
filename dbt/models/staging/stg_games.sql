{{ config(materialized='view') }}

select
  game_id,
  season,
  week,
  game_type as season_type,
  parse_date('%Y-%m-%d', gameday) as game_date,
  gametime as game_time,
  home_team,
  away_team,
  home_score,
  away_score,
  case
    when home_score > away_score then 1
    when away_score > home_score then 0
    else null
  end as home_win,
  (home_score - away_score) as point_differential,
  (home_score + away_score) as total_points,
  case
    when home_score is not null and away_score is not null then 'completed'
    else 'scheduled'
  end as game_status,
  stadium,
  surface,
  roof,

  -- Additional derived fields
  case when week <= 18 then 'regular' else 'playoff' end as season_phase,
  extract(dayofweek from parse_date('%Y-%m-%d', gameday)) as game_day_of_week,
  case
    when extract(dayofweek from parse_date('%Y-%m-%d', gameday)) = 1 then 'Sunday'
    when extract(dayofweek from parse_date('%Y-%m-%d', gameday)) = 2 then 'Monday'
    when extract(dayofweek from parse_date('%Y-%m-%d', gameday)) = 5 then 'Thursday'
    when extract(dayofweek from parse_date('%Y-%m-%d', gameday)) = 7 then 'Saturday'
    else 'Other'
  end as game_day_name,

  -- Betting relevance
  case
    when parse_date('%Y-%m-%d', gameday) between current_date() and date_add(current_date(), interval 7 day)
    then true else false
  end as upcoming_week

from {{ source('nfl_data', 'games') }}
order by season, week, gameday