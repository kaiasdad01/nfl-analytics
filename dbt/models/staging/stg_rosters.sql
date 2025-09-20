{{ config(materialized='view') }}

select
  player_id,
  season,
  null as week,  -- Rosters are typically season-level
  team,
  position,
  jersey_number,
  depth_chart_position,
  status,
  null as salary,
  null as contract_year,

  -- Derived fields
  case
    when depth_chart_position in ('1', 'Starter', 'Starting') then 'starter'
    when depth_chart_position in ('2', 'Backup', 'Second') then 'backup'
    when depth_chart_position in ('3', 'Third', 'Reserve') then 'reserve'
    else 'unknown'
  end as roster_role,

  case
    when status = 'ACT' then 'active'
    when status = 'IR' then 'injured_reserve'
    when status = 'PS' then 'practice_squad'
    when status = 'SUS' then 'suspended'
    else lower(status)
  end as roster_status_clean,

  -- Team context
  current_timestamp as updated_at

from {{ source('nfl_data', 'rosters') }}
where player_id is not null
order by season desc, team, position, depth_chart_position