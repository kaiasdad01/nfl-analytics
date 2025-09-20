{{ config(materialized='view') }}

select 
      game_id
    , season 
    , week
    , season_type
    , gameday as game_date 
    , home_team
    , away_team

    -- updoming week y/n
    , case when gameday between current_date() and date_add(current_date(), interval 7 day) then true else false end as upcoming_week

from {{ source('nfl_data', 'schedule_2025') }}

where season = 2025
and season_type = 'REG'

order by gameday, game_id