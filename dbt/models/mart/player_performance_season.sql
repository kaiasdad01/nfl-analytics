
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
        count(*) as games_played,
        sum(passing_yards) as total_passing_yards,
        sum(rushing_yards) as total_rushing_yards,
        sum(receiving_yards) as total_receiving_yards,
        sum(passing_tds) as total_passing_tds,
        sum(rushing_tds) as total_rushing_tds,
        sum(receiving_tds) as total_receiving_tds,
        sum(fantasy_points) as total_fantasy_points,
        avg(passing_yards) as avg_passing_yards,
        avg(rushing_yards) as avg_rushing_yards,
        avg(receiving_yards) as avg_receiving_yards,
        avg(fantasy_points) as avg_fantasy_points,
        max(fantasy_points) as best_game,
        min(fantasy_points) as worst_game,
        stddev(fantasy_points) as fantasy_points_stddev,
        -- Consistency metrics
        case 
            when stddev(fantasy_points) = 0 then 0
            else avg(fantasy_points) / stddev(fantasy_points)
        end as consistency_ratio,

        -- Games with double-digit fantasy points
        sum(case when fantasy_points >= 10 then 1 else 0 end) as double_digit_games,
        
        -- Games with 20+ fantasy points
        sum(case when fantasy_points >= 20 then 1 else 0 end) as boom_games
    from player_stats
    group by player_id, player_name, position, recent_team, season
)

select * from player_performance
