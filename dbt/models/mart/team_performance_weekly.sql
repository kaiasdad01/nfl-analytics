
with games as (
    select * from {{ ref('stg_games') }}
),

team_weekly_stats as (
    select
        team,
        season,
        week,
        gameday,
        case when team = home_team then 'home' else 'away' end as home_away,
        case when team = home_team then home_score else away_score end as points_for,
        case when team = home_team then away_score else home_score end as points_against,
        case when team = home_team then away_team else home_team end as opponent,
        case when winning_team = team then 1 else 0 end as win,
        case when winning_team = 'TIE' then 1 else 0 end as tie,
        point_differential,
        total_points,
        -- Rolling averages for ML features
        avg(case when team = home_team then home_score else away_score end) over (
            partition by team, season 
            order by gameday, week 
            rows between 2 preceding and current row
        ) as recent_3_game_points_for_avg,
        avg(case when team = home_team then away_score else home_score end) over (
            partition by team, season 
            order by gameday, week 
            rows between 2 preceding and current row
        ) as recent_3_game_points_against_avg,
        -- Season averages
        avg(case when team = home_team then home_score else away_score end) over (
            partition by team, season
        ) as season_points_for_avg,
        avg(case when team = home_team then away_score else home_score end) over (
            partition by team, season
        ) as season_points_against_avg,
        -- Win streak
        sum(case when winning_team = team then 1 else 0 end) over (
            partition by team, season 
            order by gameday, week 
            rows between unbounded preceding and current row
        ) as wins_so_far,
        -- Games played
        row_number() over (
            partition by team, season 
            order by gameday, week
        ) as games_played_this_season,
        -- Recent form (last 3 games)
        sum(case when winning_team = team then 1 else 0 end) over (
            partition by team, season 
            order by gameday, week 
            rows between 2 preceding and current row
        ) as recent_3_game_wins
    from games
    cross join unnest([home_team, away_team]) as team
)

select * from team_weekly_stats