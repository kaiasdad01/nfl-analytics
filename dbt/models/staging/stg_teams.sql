{{ config(materialized='view') }}

with all_teams as (
  select distinct home_team as team_abbr
  from {{ source('nfl_data', 'games') }}
  where home_team is not null

  union distinct

  select distinct away_team as team_abbr
  from {{ source('nfl_data', 'games') }}
  where away_team is not null
)

select
  team_abbr,
  case
    when team_abbr = 'ARI' then 'Arizona Cardinals'
    when team_abbr = 'ATL' then 'Atlanta Falcons'
    when team_abbr = 'BAL' then 'Baltimore Ravens'
    when team_abbr = 'BUF' then 'Buffalo Bills'
    when team_abbr = 'CAR' then 'Carolina Panthers'
    when team_abbr = 'CHI' then 'Chicago Bears'
    when team_abbr = 'CIN' then 'Cincinnati Bengals'
    when team_abbr = 'CLE' then 'Cleveland Browns'
    when team_abbr = 'DAL' then 'Dallas Cowboys'
    when team_abbr = 'DEN' then 'Denver Broncos'
    when team_abbr = 'DET' then 'Detroit Lions'
    when team_abbr = 'GB' then 'Green Bay Packers'
    when team_abbr = 'HOU' then 'Houston Texans'
    when team_abbr = 'IND' then 'Indianapolis Colts'
    when team_abbr = 'JAX' then 'Jacksonville Jaguars'
    when team_abbr = 'KC' then 'Kansas City Chiefs'
    when team_abbr = 'LV' then 'Las Vegas Raiders'
    when team_abbr = 'LAC' then 'Los Angeles Chargers'
    when team_abbr = 'LA' then 'Los Angeles Rams'
    when team_abbr = 'MIA' then 'Miami Dolphins'
    when team_abbr = 'MIN' then 'Minnesota Vikings'
    when team_abbr = 'NE' then 'New England Patriots'
    when team_abbr = 'NO' then 'New Orleans Saints'
    when team_abbr = 'NYG' then 'New York Giants'
    when team_abbr = 'NYJ' then 'New York Jets'
    when team_abbr = 'PHI' then 'Philadelphia Eagles'
    when team_abbr = 'PIT' then 'Pittsburgh Steelers'
    when team_abbr = 'SF' then 'San Francisco 49ers'
    when team_abbr = 'SEA' then 'Seattle Seahawks'
    when team_abbr = 'TB' then 'Tampa Bay Buccaneers'
    when team_abbr = 'TEN' then 'Tennessee Titans'
    when team_abbr = 'WAS' then 'Washington Commanders'
    else team_abbr
  end as team_name,

  case
    when team_abbr in ('BUF', 'MIA', 'NE', 'NYJ') then 'AFC'
    when team_abbr in ('BAL', 'CIN', 'CLE', 'PIT') then 'AFC'
    when team_abbr in ('HOU', 'IND', 'JAX', 'TEN') then 'AFC'
    when team_abbr in ('DEN', 'KC', 'LV', 'LAC') then 'AFC'
    when team_abbr in ('DAL', 'NYG', 'PHI', 'WAS') then 'NFC'
    when team_abbr in ('CHI', 'DET', 'GB', 'MIN') then 'NFC'
    when team_abbr in ('ATL', 'CAR', 'NO', 'TB') then 'NFC'
    when team_abbr in ('ARI', 'LA', 'SF', 'SEA') then 'NFC'
    else 'Unknown'
  end as conference,

  case
    when team_abbr in ('BUF', 'MIA', 'NE', 'NYJ') then 'AFC East'
    when team_abbr in ('BAL', 'CIN', 'CLE', 'PIT') then 'AFC North'
    when team_abbr in ('HOU', 'IND', 'JAX', 'TEN') then 'AFC South'
    when team_abbr in ('DEN', 'KC', 'LV', 'LAC') then 'AFC West'
    when team_abbr in ('DAL', 'NYG', 'PHI', 'WAS') then 'NFC East'
    when team_abbr in ('CHI', 'DET', 'GB', 'MIN') then 'NFC North'
    when team_abbr in ('ATL', 'CAR', 'NO', 'TB') then 'NFC South'
    when team_abbr in ('ARI', 'LA', 'SF', 'SEA') then 'NFC West'
    else 'Unknown'
  end as division,

  case
    when team_abbr in ('BUF', 'MIA', 'NE', 'NYJ', 'DAL', 'NYG', 'PHI', 'WAS') then 'East'
    when team_abbr in ('BAL', 'CIN', 'CLE', 'PIT', 'CHI', 'DET', 'GB', 'MIN') then 'North'
    when team_abbr in ('HOU', 'IND', 'JAX', 'TEN', 'ATL', 'CAR', 'NO', 'TB') then 'South'
    when team_abbr in ('DEN', 'KC', 'LV', 'LAC', 'ARI', 'LA', 'SF', 'SEA') then 'West'
    else 'Unknown'
  end as division_name,

  -- Stadium info (basic placeholders)
  null as stadium_name,
  null as stadium_capacity,
  null as timezone

from all_teams
order by conference, division, team_abbr