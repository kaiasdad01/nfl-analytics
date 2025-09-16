{{ config(materialized='view') }}
with labels as (
  select
      game_id
    , season
    , week
    , home_team
    , away_team
    , home_score
    , away_score
    , (home_score - away_score) as point_diff
    , (home_score + away_score) as total_points
    , case when home_score > away_score then 1
           when home_score < away_score then 0
           else null
      end as home_win
  from {{ ref('game_labels') }}
  where week > 1  -- drop week 1 to avoid missing pre-game features
)

, home_rolling as (
  select
      season
    , week
    , team
    , avg_yards_l3
    , avg_epa_l3
    , third_down_rate_l3
    , turnovers_l3
    , top_secs_l3
  from {{ ref('team_week_rolling') }}
)

, away_rolling as (
  select
      season
    , week
    , team
    , avg_yards_l3
    , avg_epa_l3
    , third_down_rate_l3
    , turnovers_l3
    , top_secs_l3
  from {{ ref('team_week_rolling') }}
)

, home_recency as (
  select
      season
    , week
    , team
    , total_yards
    , total_epa
    , third_down_conversion_rate
    , total_turnovers
    , time_of_possession_seconds
    , red_zone_first_downs
  from {{ ref('team_week_features') }}
)

, away_recency as (
  select
      season
    , week
    , team
    , total_yards
    , total_epa
    , third_down_conversion_rate
    , total_turnovers
    , time_of_possession_seconds
    , red_zone_first_downs
  from {{ ref('team_week_features') }}
)

select
    l.game_id
  , l.season
  , l.week
  , l.home_team
  , l.away_team

  -- home rolling (l3, pre-game only)
  , hr.avg_yards_l3            as home_avg_yards_l3
  , hr.avg_epa_l3              as home_avg_epa_l3
  , hr.third_down_rate_l3      as home_third_down_rate_l3
  , hr.turnovers_l3            as home_turnovers_l3
  , hr.top_secs_l3             as home_top_secs_l3

  -- away rolling (l3, pre-game only)
  , ar.avg_yards_l3            as away_avg_yards_l3
  , ar.avg_epa_l3              as away_avg_epa_l3
  , ar.third_down_rate_l3      as away_third_down_rate_l3
  , ar.turnovers_l3            as away_turnovers_l3
  , ar.top_secs_l3             as away_top_secs_l3

  -- home week-1 recency snapshot
  , h1.total_yards             as home_total_yards_wm1
  , h1.total_epa               as home_total_epa_wm1
  , h1.third_down_conversion_rate as home_third_down_rate_wm1
  , h1.total_turnovers         as home_turnovers_wm1
  , h1.time_of_possession_seconds as home_top_secs_wm1
  , h1.red_zone_first_downs    as home_rz_first_downs_wm1

  -- away week-1 recency snapshot
  , a1.total_yards             as away_total_yards_wm1
  , a1.total_epa               as away_total_epa_wm1
  , a1.third_down_conversion_rate as away_third_down_rate_wm1
  , a1.total_turnovers         as away_turnovers_wm1
  , a1.time_of_possession_seconds as away_top_secs_wm1
  , a1.red_zone_first_downs    as away_rz_first_downs_wm1

  -- targets
  , l.home_win
  , l.home_score
  , l.away_score
  , l.point_diff
  , l.total_points

from labels l
left join home_rolling hr
  on hr.season = l.season
 and hr.team   = l.home_team
 and hr.week   = l.week - 1
left join away_rolling ar
  on ar.season = l.season
 and ar.team   = l.away_team
 and ar.week   = l.week - 1
left join home_recency h1
  on h1.season = l.season
 and h1.team   = l.home_team
 and h1.week   = l.week - 1
left join away_recency a1
  on a1.season = l.season
 and a1.team   = l.away_team
 and a1.week   = l.week - 1