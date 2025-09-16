{{ config(materialized='view') }}

with base as (
  select
      player_id
    , team
    , season
    , week
    , season_type
    , coalesce(cast(fantasy_points_ppr as float64), cast(fantasy_points as float64)) as fantasy_ppr
    , cast(passing_yards as int64)   as passing_yards
    , cast(rushing_yards as int64)   as rushing_yards
    , cast(receiving_yards as int64) as receiving_yards
    , cast(passing_tds as int64)     as passing_tds
    , cast(rushing_tds as int64)     as rushing_tds
    , cast(receiving_tds as int64)   as receiving_tds
  from {{ ref('player_week_features') }}
)
, rolled as (
  select
      player_id
    , team
    , season
    , week
    , season_type
    , avg(fantasy_ppr)    over w3  as fantasy_ppr_l3
    , sum(fantasy_ppr)    over w5  as fantasy_ppr_l5_sum
    , avg(passing_yards)  over w3  as pass_yds_l3
    , avg(rushing_yards)  over w3  as rush_yds_l3
    , avg(receiving_yards) over w3 as rec_yds_l3
    , sum(passing_tds + rushing_tds + receiving_tds) over w5 as tds_l5_sum
  from base
  window w3 as (
    partition by player_id, season
    order by week
    rows between 3 preceding and 1 preceding
  )
  , w5 as (
    partition by player_id, season
    order by week
    rows between 5 preceding and 1 preceding
  )
)
select * from rolled