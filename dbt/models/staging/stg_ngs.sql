{{ config(materialized='view') }}

with base as (
  select
      cast(player_id as string) as player_id
    , cast(season as int64)     as season
    , cast(week as int64)       as week
    , cast(stat_type as string) as stat_type

    -- identity
    , cast(player_display_name as string) as player_display_name
    , cast(player_position as string)     as player_position
    , cast(team_abbr as string)           as team

    -- PASSING (nullable for other stat_types)
    , cast(avg_time_to_throw as float64)            as avg_time_to_throw
    , cast(avg_completed_air_yards as float64)      as avg_completed_air_yards
    , cast(avg_intended_air_yards as float64)       as avg_intended_air_yards
    , cast(avg_air_yards_differential as float64)   as avg_air_yards_differential
    , cast(aggressiveness as float64)               as aggressiveness
    , cast(attempts as int64)                       as pass_attempts
    , cast(pass_yards as int64)                     as pass_yards
    , cast(pass_touchdowns as int64)                as pass_tds
    , cast(interceptions as int64)                  as interceptions

    -- RUSHING
    , cast(efficiency as float64)                   as rush_efficiency
    , cast(percent_attempts_gte_eight_defenders as float64) as pct_att_8_in_box
    , cast(avg_time_to_los as float64)              as avg_time_to_los
    , cast(rush_attempts as int64)                  as rush_attempts
    , cast(rush_yards as int64)                     as rush_yards
    , cast(avg_rush_yards as float64)               as avg_rush_yards
    , cast(rush_touchdowns as int64)                as rush_tds
    , cast(expected_rush_yards as float64)          as expected_rush_yards
    , cast(rush_yards_over_expected as float64)     as rush_yoe
    , cast(rush_yards_over_expected_per_att as float64) as rush_yoe_per_att
    , cast(rush_pct_over_expected as float64)       as rush_pct_over_expected

    -- RECEIVING
    , cast(avg_cushion as float64)                  as avg_cushion
    , cast(avg_separation as float64)               as avg_separation
    , cast(percent_share_of_intended_air_yards as float64) as share_intended_air_yards
    , cast(receptions as int64)                     as receptions
    , cast(targets as int64)                        as targets
    , cast(catch_percentage as float64)             as catch_pct
    , cast(yards as int64)                          as rec_yards
    , cast(rec_touchdowns as int64)                 as rec_tds
    , cast(avg_yac as float64)                      as avg_yac
    , cast(avg_expected_yac as float64)             as avg_expected_yac
    , cast(avg_yac_above_expectation as float64)    as avg_yac_oe
  from {{ source('nfl_data', 'ngs_data') }}
)
select * from base