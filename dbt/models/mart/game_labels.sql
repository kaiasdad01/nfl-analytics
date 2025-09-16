{{ config(materialized='view') }}

with games as (
  select
      cast(game_id as string)        as game_id
    , cast(season as int64)          as season
    , cast(week as int64)            as week
    , cast(home_team as string)      as home_team
    , cast(away_team as string)      as away_team
    , cast(home_score as int64)      as home_score
    , cast(away_score as int64)      as away_score
  from {{ ref('stg_games') }}
)

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
from games