{{ config(materialized='view') }}

with stats as (
  select
      player_id
    , recent_team                  as team
    , season
    , sum(fantasy_points)          as fantasy_points_season
  from {{ ref('stg_player_stats') }}
  group by 1,2,3
)
select
    s.player_id
  , coalesce(s.team, r.team)       as team
  , s.season
  , r.position
  , r.years_exp
  , s.fantasy_points_season
from stats s
left join {{ ref('stg_rosters') }} r
  on r.player_id = s.player_id
 and r.season    = s.season