{{ config(materialized='view') }}

with raw as (
  select
      cast(game_id as string) as game_id
    , cast(season as int64)   as season
    , cast(week as int64)     as week
    , cast(season_type as string) as season_type

    -- team identifiers
    , cast(posteam as string) as offense_team
    , cast(defteam as string) as defense_team

    -- core play fields
    , cast(qtr as int64)          as quarter
    , cast(down as int64)         as down
    , cast(ydstogo as int64)      as yards_to_go
    , cast(yards_gained as int64) as yards_gained
    , cast(play_type as string)   as play_type

    -- outcomes/flags (normalize to booleans)
    , (coalesce(safe_cast(touchdown            as int64), 0) > 0) as is_td
    , (coalesce(safe_cast(pass_touchdown       as int64), 0) > 0) as is_pass_td
    , (coalesce(safe_cast(rush_touchdown       as int64), 0) > 0) as is_rush_td
    , (coalesce(safe_cast(interception         as int64), 0) > 0) as is_interception
    , (coalesce(safe_cast(fumble_lost          as int64), 0) > 0) as is_fumble_lost
    , (coalesce(safe_cast(sack                 as int64), 0) > 0) as is_sack
    , (coalesce(safe_cast(penalty              as int64), 0) > 0) as is_penalty
    , (coalesce(safe_cast(first_down           as int64), 0) > 0) as is_first_down

    -- 3rd/4th down conversion flags (booleans)
    , (coalesce(safe_cast(third_down_converted  as int64), 0) > 0) as third_down_converted
    , (coalesce(safe_cast(third_down_failed     as int64), 0) > 0) as third_down_failed
    , (coalesce(safe_cast(fourth_down_converted as int64), 0) > 0) as fourth_down_converted
    , (coalesce(safe_cast(fourth_down_failed    as int64), 0) > 0) as fourth_down_failed

    -- epa
    , cast(epa as float64) as epa

    -- drive features
    , cast(drive_time_of_possession as string) as drive_time_of_possession_raw -- e.g. "2:31"
    , cast(drive_first_downs as int64)         as drive_first_downs

    -- field position / red zone proxy
    , cast(yardline_100 as int64) as yardline_100

    -- environment
    , cast(stadium as string) as stadium
    , cast(weather as string) as weather
  from {{ source('nfl_data', 'pbp_data') }}
)

select
    *
  , (yardline_100 is not null and yardline_100 <= 20) as in_red_zone
  , case
      when drive_time_of_possession_raw is null then null
      else coalesce(safe_cast(split(drive_time_of_possession_raw, ':')[safe_offset(0)] as int64), 0) * 60
         +       safe_cast(split(drive_time_of_possession_raw, ':')[safe_offset(1)] as int64)
    end as drive_time_of_possession_seconds
from raw