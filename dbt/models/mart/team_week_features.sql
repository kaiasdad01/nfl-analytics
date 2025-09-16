
select
    game_id
  , season
  , week
  , season_type
  , team
  , total_yards
  , interceptions
  , fumbles_lost
  , total_turnovers
  , third_down_conversions
  , third_down_failures
  , third_down_conversion_rate
  , total_epa
  , time_of_possession_seconds
  , red_zone_first_downs
from {{ ref('team_game_features') }}