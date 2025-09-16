with source_data as (
    select * from {{ source('nfl_data', 'player_stats') }}
),

cleaned_stats as (
    select 
          player_id
        , player_name
        , position
        , recent_team
        , season 
        , week 

        , coalesce(passing_yards, 0) as passing_yards
        , coalesce(rushing_yards, 0) as rushing_yards
        , coalesce(receiving_yards, 0) as receiving_yards
        , coalesce(passing_tds, 0) as passing_tds
        , coalesce(rushing_tds, 0) as rushing_tds
        , coalesce(receiving_tds, 0) as receiving_tds
        
        -- FANTASY POINTS !!!

        , (coalesce(passing_yards, 0) * {{ var('passing_yard_points') }}) +
          (coalesce(rushing_yards, 0) * {{ var('rushing_yard_points') }}) +
          (coalesce(receiving_yards, 0) * {{ var('receiving_yard_points') }}) +
          (coalesce(passing_tds, 0) * {{ var('passing_td_points') }}) +
          (coalesce(rushing_tds, 0) * {{ var('rushing_td_points') }}) +
          (coalesce(receiving_tds, 0) * {{ var('receiving_td_points') }}) as fantasy_points

    from source_data
)

select * from cleaned_stats