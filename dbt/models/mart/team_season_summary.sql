{{ config(materialized='view') }}

select
    season
  , team
  , count(*)                          as games_played
  , avg(total_yards)                  as avg_yards
  , avg(total_epa)                    as avg_epa
  , avg(third_down_conversion_rate)   as avg_third_down_rate
  , avg(time_of_possession_seconds)   as avg_top_secs
  , sum(total_turnovers)              as turnovers_sum
from {{ ref('team_week_features') }}
group by 1,2