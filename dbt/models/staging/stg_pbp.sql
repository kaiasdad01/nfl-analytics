{{ config(materialized='view') }}

select
  play_id,
  game_id,
  season,
  week,
  play_type,
  down,
  ydstogo as yards_to_go,
  yardline_100,
  qtr as quarter,
  time as time_remaining,
  posteam,
  defteam,
  yards_gained,
  touchdown,
  first_down,
  interception,
  fumble,
  fumble_lost,
  penalty,
  epa,
  wpa,
  success,
  passer_player_id as passer_id,
  rusher_player_id as rusher_id,
  receiver_player_id as receiver_id,

  -- Derived fields
  case
    when play_type in ('pass', 'run') then 'offensive'
    when play_type in ('punt', 'field_goal', 'extra_point') then 'special_teams'
    when play_type in ('kickoff') then 'kickoff'
    else 'other'
  end as play_category,

  case
    when yardline_100 <= 20 then 'red_zone'
    when yardline_100 <= 40 then 'scoring_territory'
    else 'field'
  end as field_position,

  case
    when down = 3 and ydstogo <= 3 then 'short_third_down'
    when down = 3 and ydstogo > 7 then 'long_third_down'
    when down = 3 then 'medium_third_down'
    when down = 4 then 'fourth_down'
    else 'early_down'
  end as down_situation,

  case
    when epa > 0 then 'positive'
    when epa < 0 then 'negative'
    else 'neutral'
  end as epa_impact

from {{ source('nfl_data', 'pbp_data') }}
where posteam is not null  -- Filter out plays without a possessing team
order by game_id, play_id