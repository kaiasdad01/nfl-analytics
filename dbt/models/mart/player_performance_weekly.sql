-- Player performance analytics - Weekly Level
-- Used for: ML features, trend analysis, matchup analysis
with player_stats as (
    select * from {{ ref('stg_player_stats') }}
),

player_performance as (
    select
        player_id,
        player_name,
        position,
        recent_team,
        season,
        week,
        passing_yards,
        rushing_yards,
        receiving_yards,
        passing_tds,
        rushing_tds,
        receiving_tds,
        fantasy_points,
        -- Rolling averages for ML features
        avg(fantasy_points) over (
            partition by player_id, season 
            order by week 
            rows between 2 preceding and current row
        ) as recent_3_game_avg,
        avg(fantasy_points) over (
            partition by player_id, season 
            order by week 
            rows between 4 preceding and current row
        ) as recent_5_game_avg,
        -- Season averages for comparison
        avg(fantasy_points) over (
            partition by player_id, season
        ) as season_avg,
        -- Week-over-week change
        fantasy_points - lag(fantasy_points, 1) over (
            partition by player_id, season 
            order by week
        ) as week_over_week_change,
        -- Performance vs season average
        fantasy_points - avg(fantasy_points) over (
            partition by player_id, season
        ) as vs_season_avg,
        -- Rolling standard deviation (consistency)
        stddev(fantasy_points) over (
            partition by player_id, season 
            order by week 
            rows between 2 preceding and current row
        ) as recent_3_game_stddev,
        -- Games played this season so far
        row_number() over (
            partition by player_id, season 
            order by week
        ) as games_played_this_season
    from player_stats
)

select * from player_performance