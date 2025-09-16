{{ config(materialized='view') }}

with plays as (
  select
      game_id
    , season
    , week
    , season_type
    , cast(posteam as string) as team
    , yards_gained
    , (coalesce(safe_cast(interception  as int64),0) > 0) as is_interception
    , (coalesce(safe_cast(fumble_lost   as int64),0) > 0) as is_fumble_lost
    , (coalesce(safe_cast(first_down    as int64),0) > 0) as is_first_down
    , (coalesce(safe_cast(third_down_converted as int64),0) > 0) as third_down_converted
    , (coalesce(safe_cast(third_down_failed    as int64),0) > 0) as third_down_failed
    , epa
    , case
        when drive_time_of_possession is null then null
        else coalesce(safe_cast(split(drive_time_of_possession, ':')[safe_offset(0)] as int64), 0) * 60
           +       safe_cast(split(drive_time_of_possession, ':')[safe_offset(1)] as int64)
      end as drive_time_of_possession_seconds
    , (yardline_100 is not null and yardline_100 <= 20) as in_red_zone
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