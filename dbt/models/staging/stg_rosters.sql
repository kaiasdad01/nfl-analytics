{{ config(materialized='view') }}
select * from {{ source('nfl_data', 'rosters') }}