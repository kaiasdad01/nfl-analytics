-- Team matchup analytics
-- Used for: Head-to-head analysis, historical performance, matchup trends
with games as (
    select * from {{ ref('stg_games') }}
),

team_matchups as (
    select
        home_team as team1,
        away_team as team2,
        season,
        count(*) as total_games,
        sum(case when winning_team = home_team then 1 else 0 end) as team1_wins,
        sum(case when winning_team = away_team then 1 else 0 end) as team2_wins,
        sum(case when winning_team = 'TIE' then 1 else 0 end) as ties,
        sum(home_score) as team1_points_for,
        sum(away_score) as team1_points_against,
        sum(away_score) as team2_points_for,
        sum(home_score) as team2_points_against,
        avg(home_score) as team1_avg_points_for,
        avg(away_score) as team1_avg_points_against,
        avg(away_score) as team2_avg_points_for,
        avg(home_score) as team2_avg_points_against
    from games
    group by home_team, away_team, season
)

select
    team1,
    team2,
    season,
    total_games,
    team1_wins,
    team2_wins,
    ties,
    team1_points_for,
    team1_points_against,
    team2_points_for,
    team2_points_against,
    team1_avg_points_for,
    team1_avg_points_against,
    team2_avg_points_for,
    team2_avg_points_against,
    round(team1_wins / total_games * 100, 2) as team1_win_percentage,
    round(team2_wins / total_games * 100, 2) as team2_win_percentage,
    team1_points_for - team1_points_against as team1_point_differential,
    team2_points_for - team2_points_against as team2_point_differential
from team_matchups