-- Player matchup analysis
-- Used for: Opponent analysis, home/away splits, situational performance
with player_stats as (
    select * from {{ ref('stg_player_stats') }}
),

games as (
    select * from {{ ref('stg_games') }}
),

player_games as (
    select
        ps.*,
        g.gameday,
        g.home_team,
        g.away_team,
        case when ps.recent_team = g.home_team then 'home' else 'away' end as home_away,
        case when ps.recent_team = g.home_team then g.away_team else g.home_team end as opponent
    from player_stats ps
    join games g on ps.recent_team in (g.home_team, g.away_team) 
        and ps.season = g.season 
        and ps.week = g.week
),

matchup_analysis as (
    select
        player_id,
        player_name,
        position,
        recent_team,
        season,
        opponent,
        home_away,
        count(*) as games_vs_opponent,
        avg(fantasy_points) as avg_fantasy_vs_opponent,
        sum(fantasy_points) as total_fantasy_vs_opponent,
        max(fantasy_points) as best_game_vs_opponent,
        min(fantasy_points) as worst_game_vs_opponent,
        -- Home vs away splits
        avg(case when home_away = 'home' then fantasy_points end) as avg_fantasy_home,
        avg(case when home_away = 'away' then fantasy_points end) as avg_fantasy_away,
        -- Early vs late season performance
        avg(case when week <= 8 then fantasy_points end) as avg_fantasy_early_season,
        avg(case when week > 8 then fantasy_points end) as avg_fantasy_late_season
    from player_games
    group by player_id, player_name, position, recent_team, season, opponent, home_away
)

select * from matchup_analysis