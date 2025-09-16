{{ config(materialized='view') }}

with base as (
  select
      season
    , week
    , season_type
    , team
    , total_yards
    , total_turnovers
    , third_down_conversion_rate
    , total_epa
    , time_of_possession_seconds
  from {{ ref('team_week_features') }}
)
, rolled as (
  select
      season
    , week
    , season_type
    , team
    , avg(total_yards) over w3                    as avg_yards_l3
    , avg(total_epa)   over w3                    as avg_epa_l3
    , avg(third_down_conversion_rate) over w3     as third_down_rate_l3
    , avg(total_turnovers) over w3                as turnovers_l3
    , avg(time_of_possession_seconds) over w3     as top_secs_l3
  from base
  window w3 as (
    partition by season, team
    order by week
    rows between 3 preceding and 1 preceding
  )
)
select * from rolled