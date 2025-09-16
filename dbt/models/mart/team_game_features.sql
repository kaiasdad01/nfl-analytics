{{ config(materialized='view') }}

with plays as (
  select
      game_id
    , season
    , week
    , season_type
    , offense_team                                   as team
    , yards_gained
    , is_interception
    , is_fumble_lost
    , is_first_down
    , third_down_converted
    , third_down_failed
    , epa
    , drive_time_of_possession_seconds
    , in_red_zone
  from {{ ref('stg_pbp') }}
)

, agg as (
  select
      game_id
    , season
    , week
    , season_type
    , team
    , sum(yards_gained)                                        as total_yards
    , sum(case when is_interception then 1 else 0 end)         as interceptions
    , sum(case when is_fumble_lost then 1 else 0 end)          as fumbles_lost
    , sum(case when is_interception or is_fumble_lost then 1 else 0 end) as total_turnovers
    , sum(case when third_down_converted then 1 else 0 end)    as third_down_conversions
    , sum(case when third_down_failed then 1 else 0 end)       as third_down_failures
    , safe_divide(
        sum(case when third_down_converted then 1 else 0 end),
        nullif(sum(case when third_down_converted or third_down_failed then 1 else 0 end), 0)
      )                                                        as third_down_conversion_rate
    , sum(epa)                                                 as total_epa
    , sum(drive_time_of_possession_seconds)                    as time_of_possession_seconds
    , sum(case when in_red_zone and is_first_down then 1 else 0 end) as red_zone_first_downs
  from plays
  group by 1,2,3,4,5
)

select * from agg