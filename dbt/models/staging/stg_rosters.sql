{{ config(materialized='view') }}

select
    cast(player_id as string) as player_id
  , cast(team as string)      as team
  , cast(season as int64)     as season
  , cast(position as string)  as position
  , cast(player_name as string) as player_name
  , cast(years_exp as int64)    as years_exp
  , cast(rookie_year as int64)  as rookie_year

  -- common external IDs (nullable)
  , cast(espn_id as string)       as espn_id
  , cast(sportradar_id as string) as sportradar_id
  , cast(yahoo_id as string)      as yahoo_id
  , cast(pfr_id as string)        as pfr_id
  , cast(esb_id as string)        as esb_id
from {{ source('nfl_data', 'rosters') }}