with games as (
    select * from {{ ref('stg_games') }}
),

team_stats as (
    select 
          team 
        , season 
        , count(*) as games_played
        , sum(case when winning_team = team then 1 else 0 end) as wins
        , sum(case when winning_team != team then 1 else 0 end) as losses
        , sum(case when winning_team = 'TIE' then 1 else 0 end) as ties 
        , sum(case when team = home_team then home_score else away_score end) as points_for 
        , sum(case when team = home_team then away_score else home_score end) as points_against 
        , avg(case when team = home_team then home_score else away_score end) as avg_points_for 
        , avg(case when team = home_team then away_score else home_score end) as avg_points_against
    from games 
    cross join unnest([home_team, away_team]) as team 
    group by team, season
)

select 
      team 
    , season 
    , games_played
    , wins 
    , losses 
    , ties 
    , points_for 
    , points_against
    , avg_points_for
    , avg_points_against
    , round(wins / games_played * 100, 2) as win_percentage
    , points_for - points_against as point_differential

from team_stats