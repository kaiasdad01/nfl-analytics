{{ config(materialized='view') }}
select * from {{ source('nfl_data', 'ngs_data') }}