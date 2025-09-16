{{ config(materialized='view') }}
select * from {{ source('nfl_data', 'player_stats') }}