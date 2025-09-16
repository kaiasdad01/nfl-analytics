{{ config(materialized='view') }}

with ngs_agg as (
  select
      player_id
    , season
    , stat_type
    , avg(avg_time_to_throw)              as avg_time_to_throw
    , avg(avg_completed_air_yards)        as avg_completed_air_yards
    , avg(avg_separation)                 as avg_sep
    , avg(rush_yards_over_expected_per_att) as rush_yoe_per_att
  from {{ ref('stg_ngs') }}
  group by 1,2,3
)
select
    c.*
  , a_p.avg_time_to_throw
  , a_p.avg_completed_air_yards
  , a_r.rush_yoe_per_att
  , a_rcv.avg_sep
from {{ ref('player_season') }} c
left join ngs_agg a_p
  on a_p.player_id = c.player_id and a_p.season = c.season and a_p.stat_type = 'passing'
left join ngs_agg a_r
  on a_r.player_id = c.player_id and a_r.season = c.season and a_r.stat_type = 'rushing'
left join ngs_agg a_rcv
  on a_rcv.player_id = c.player_id and a_rcv.season = c.season and a_rcv.stat_type = 'receiving'