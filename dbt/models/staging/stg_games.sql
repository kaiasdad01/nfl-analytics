with source_data as (
    select * from {{ source('nfl_data', 'games') }}
),

cleaned_games as (
    select
        , game_id
        , season 
        , week 
        , game_type 
        , home_team 
        , away_team 
        , home_score
        , away_score
        , game_date

        case 
            when home_score > away_score then home_team
            when home_score < away_score then away_team
            else 'TIE'
        end as winning_team

        abs(home_score - away_score) as point_differential
        home_score + away_score as total_points
    from source_data
)

select * from cleaned_games