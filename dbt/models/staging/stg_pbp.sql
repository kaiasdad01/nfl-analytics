{{ config(materialized='view') }}
select * from {{ source('nfl_data', 'pbp_data') }}