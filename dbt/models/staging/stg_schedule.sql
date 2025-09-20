{{ config(materialized='view') }}

-- This model focuses on schedule-specific fields from unified games table
select
  game_id,
  season,
  week,
  game_type as season_type,
  parse_date('%Y-%m-%d', gameday) as game_date,
  gametime as game_time,
  home_team,
  away_team,
  stadium,
  null as network,  -- Can be enhanced when network data available

  case
    when extract(dayofweek from parse_date('%Y-%m-%d', gameday)) in (2, 5) then true  -- Monday/Thursday
    else false
  end as primetime,

  -- Derived fields
  case
    when (home_team = 'BUF' and away_team in ('MIA', 'NE', 'NYJ')) or
         (home_team = 'MIA' and away_team in ('BUF', 'NE', 'NYJ')) or
         (home_team = 'NE' and away_team in ('BUF', 'MIA', 'NYJ')) or
         (home_team = 'NYJ' and away_team in ('BUF', 'MIA', 'NE')) or
         -- Add all other division matchups...
         (left(home_team, 2) = left(away_team, 2)) then true
    else false
  end as divisional_game,

  -- Days until game (for scheduled games)
  case
    when parse_date('%Y-%m-%d', gameday) >= current_date() then date_diff(parse_date('%Y-%m-%d', gameday), current_date(), day)
    else null
  end as days_until_game,

  -- Betting relevance
  case
    when parse_date('%Y-%m-%d', gameday) between current_date() and date_add(current_date(), interval 7 day)
    then true else false
  end as upcoming_week

from {{ source('nfl_data', 'games') }}
where home_score is null  -- Focus on scheduled games
order by season, week, parse_date('%Y-%m-%d', gameday)