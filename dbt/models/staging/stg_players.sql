{{ config(materialized='view') }}

with latest_player_info as (
  select
    player_id,
    player_name,
    position,
    case
      when position in ('QB') then 'QB'
      when position in ('RB', 'FB') then 'RB'
      when position in ('WR') then 'WR'
      when position in ('TE') then 'TE'
      when position in ('T', 'G', 'C', 'OT', 'OG') then 'OL'
      when position in ('DE', 'DT', 'NT') then 'DL'
      when position in ('OLB', 'ILB', 'MLB', 'LB') then 'LB'
      when position in ('CB', 'S', 'SS', 'FS', 'DB') then 'DB'
      when position in ('K') then 'K'
      when position in ('P') then 'P'
      when position in ('LS') then 'LS'
      else 'OTHER'
    end as position_group,
    height,
    weight,
    birth_date,
    college,
    entry_year as draft_year,
    null as draft_round,
    draft_number as draft_pick,
    years_exp as years_pro,
    status,
    row_number() over (partition by player_id order by years_exp desc) as rn
  from {{ source('nfl_data', 'rosters') }}
  where player_id is not null
)

select
  player_id,
  player_name,
  position,
  position_group,
  height,
  weight,
  birth_date,
  college,
  draft_year,
  draft_round,
  draft_pick,
  years_pro,
  status,

  -- Derived fields
  case
    when birth_date is not null then date_diff(current_date(), date(birth_date), year)
    else null
  end as age,

  case
    when draft_year is not null then 2025 - draft_year
    else null
  end as nfl_experience,

  case
    when draft_round = 1 then 'First Round'
    when draft_round in (2, 3) then 'Early Round'
    when draft_round in (4, 5, 6, 7) then 'Late Round'
    when draft_year is null then 'Undrafted'
    else 'Unknown'
  end as draft_pedigree

from latest_player_info
where rn = 1