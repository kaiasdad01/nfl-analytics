
with weekly as (
  select * from {{ ref('team_week_features') }}
)
select
    season
  , season_type
  , team
  , sum(total_yards)                             as season_total_yards
  , sum(interceptions)                           as season_interceptions
  , sum(fumbles_lost)                            as season_fumbles_lost
  , sum(total_turnovers)                         as season_turnovers
  , sum(third_down_conversions)                  as season_third_down_conversions
  , sum(third_down_failures)                     as season_third_down_failures
  , safe_divide(
      sum(third_down_conversions),
      nullif(sum(third_down_conversions + third_down_failures), 0)
    )                                            as season_third_down_conversion_rate
  , sum(total_epa)                               as season_total_epa
  , sum(time_of_possession_seconds)              as season_time_of_possession_seconds
  , sum(red_zone_first_downs)                    as season_red_zone_first_downs
  , count(*)                                     as games_played
from weekly
group by 1,2,3