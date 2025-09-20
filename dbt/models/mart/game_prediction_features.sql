{{
  config(
    materialized='view',
    description='ML-ready dataset with home/away team features and game outcomes'
  )
}}

{# Feature configuration for easy modification #}
{%- set team_features = [
    'total_epa_l3',
    'total_yards_l3',
    'points_scored_l3',
    'third_down_conversion_rate_l3',
    'red_zone_efficiency_l3',
    'turnovers_l3',
    'success_rate_l3',
    'total_epa_l5',
    'epa_trend_3v8',
    'epa_volatility_l5',
    'yards_per_play',
    'epa_per_play'
] -%}

with game_base as (
  select
    game_id,
    season,
    week,
    season_type,
    game_date,
    home_team,
    away_team,
    home_score,
    away_score,
    home_win,
    point_differential,
    total_points,
    game_status,
    upcoming_week,
    game_day_name
  from {{ ref('stg_games') }}
  where season_type = 'REG'  -- Focus on regular season for consistent patterns
),

home_team_features as (
  select
    g.game_id,

    {# Generate home team features with 'home_' prefix #}
    {% for feature in team_features %}
    t.{{ feature }} as home_{{ feature }},
    {%- endfor %}

    -- Additional home team context
    t.recent_form as home_recent_form,
    t.season_strength_tier as home_strength_tier,
    t.momentum_direction as home_momentum,
    t.games_played_to_date as home_games_played,
    t.epa_rank_season as home_season_rank

  from game_base g
  left join {{ ref('team_game_performance') }} t
    on g.home_team = t.team
    and g.season = t.season
    and g.week = t.week + 1  -- Use previous week's performance for prediction
),

away_team_features as (
  select
    g.game_id,

    {# Generate away team features with 'away_' prefix #}
    {% for feature in team_features %}
    t.{{ feature }} as away_{{ feature }},
    {%- endfor %}

    -- Additional away team context
    t.recent_form as away_recent_form,
    t.season_strength_tier as away_strength_tier,
    t.momentum_direction as away_momentum,
    t.games_played_to_date as away_games_played,
    t.epa_rank_season as away_season_rank

  from game_base g
  left join {{ ref('team_game_performance') }} t
    on g.away_team = t.team
    and g.season = t.season
    and g.week = t.week + 1  -- Use previous week's performance for prediction
),

feature_differentials as (
  select
    g.*,
    h.* except(game_id),
    a.* except(game_id),

    {# Calculate differential features (key for ML performance) #}
    {% for feature in team_features %}
    coalesce(h.home_{{ feature }}, 0) - coalesce(a.away_{{ feature }}, 0) as diff_{{ feature }},
    {%- endfor %}

    -- Contextual differentials
    coalesce(h.home_season_rank, 32) - coalesce(a.away_season_rank, 32) as diff_season_rank,
    coalesce(h.home_games_played, 0) - coalesce(a.away_games_played, 0) as diff_games_played,

    -- Form matchup indicators
    case
      when h.home_recent_form = 'hot' and a.away_recent_form = 'cold' then 'home_hot_away_cold'
      when h.home_recent_form = 'cold' and a.away_recent_form = 'hot' then 'home_cold_away_hot'
      when h.home_recent_form = a.away_recent_form then concat('both_', h.home_recent_form)
      else 'mixed_form'
    end as form_matchup,

    -- Strength tier matchup
    case
      when h.home_strength_tier = 'elite' and a.away_strength_tier in ('below_average', 'poor') then 'elite_vs_weak'
      when a.away_strength_tier = 'elite' and h.home_strength_tier in ('below_average', 'poor') then 'weak_vs_elite'
      when h.home_strength_tier = a.away_strength_tier then concat('both_', h.home_strength_tier)
      else 'mixed_strength'
    end as strength_matchup

  from game_base g
  left join home_team_features h on g.game_id = h.game_id
  left join away_team_features a on g.game_id = a.game_id
),

final_features as (
  select
    *,

    -- ML target variables
    case when home_win = 1 then 1 else 0 end as target_home_win,
    point_differential as target_point_diff,
    total_points as target_total_points,

    -- Feature quality indicators
    case
      when home_games_played >= 3 and away_games_played >= 3 then 'high_quality'
      when home_games_played >= 1 and away_games_played >= 1 then 'medium_quality'
      else 'low_quality'
    end as feature_quality,

    -- Betting relevance flags
    case when upcoming_week and game_status = 'scheduled' then true else false end as betting_relevant,
    case when season >= 2023 then true else false end as recent_season,

    -- Record metadata
    current_timestamp() as updated_at

  from feature_differentials
)

select * from final_features
where (home_games_played >= 1 and away_games_played >= 1)  -- Filter out season opener edge cases
   or betting_relevant = true  -- Always include upcoming games
order by season desc, week desc, game_date desc