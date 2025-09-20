{{ config(materialized='incremental', unique_key='game_id')}}

select 
      game_id
    , season 
    , week 
    , season_type 
    , game_date 
    , home_team 
    , away_team 
    , home_score 
    , away_score 
    , case when home_score > away_score then 1 else 0 end as home_win
    , current_timestamp as updated_at


from {{ source('nfl_data', 'player_stats_2025') }}

where season = 2025
    and season_type = 'REG'
    and home_score IS NOT NULL 
    and away_score IS NOT NULL

{% if is_incremental() %}
    and updated_at > (select max(updated_at) from {{ this }})
    {% endif %}