{{
  config(
    materialized='view',
    description='Betting-focused analysis of upcoming games with confidence scoring'
  )
}}

{# Betting analysis configuration #}
{%- set confidence_thresholds = {
    'high': 75,
    'medium': 60,
    'low': 45
} -%}

with upcoming_games_base as (
  select
    game_id,
    season,
    week,
    game_date,
    home_team,
    away_team,
    game_day_name,

    -- Feature differentials (key predictors)
    diff_total_epa_l3,
    diff_total_yards_l3,
    diff_points_scored_l3,
    diff_epa_trend_3v8,
    diff_season_rank,

    -- Team context
    home_recent_form,
    away_recent_form,
    home_strength_tier,
    away_strength_tier,
    home_momentum,
    away_momentum,
    form_matchup,
    strength_matchup,

    -- Data quality
    feature_quality

  from {{ ref('game_prediction_features') }}
  where betting_relevant = true  -- Only upcoming games
    and season >= 2024  -- Focus on current season
),

mismatch_analysis as (
  select
    *,

    -- EPA-based strength analysis
    case
      when abs(diff_total_epa_l3) >= 0.15 then 'large_mismatch'
      when abs(diff_total_epa_l3) >= 0.08 then 'moderate_mismatch'
      when abs(diff_total_epa_l3) >= 0.03 then 'slight_mismatch'
      else 'even_matchup'
    end as epa_mismatch_level,

    -- Rank-based strength analysis
    case
      when abs(diff_season_rank) >= 10 then 'large_rank_gap'
      when abs(diff_season_rank) >= 5 then 'moderate_rank_gap'
      else 'close_ranks'
    end as rank_mismatch_level,

    -- Momentum analysis
    case
      when diff_epa_trend_3v8 > 0.08 then 'home_momentum_edge'
      when diff_epa_trend_3v8 < -0.08 then 'away_momentum_edge'
      else 'neutral_momentum'
    end as momentum_edge,

    -- Form advantage detection
    case
      when form_matchup = 'home_hot_away_cold' then 'home_form_edge'
      when form_matchup = 'home_cold_away_hot' then 'away_form_edge'
      when form_matchup like 'both_hot' then 'both_hot'
      when form_matchup like 'both_cold' then 'both_cold'
      else 'neutral_form'
    end as form_edge

  from upcoming_games_base
),

betting_signals as (
  select
    *,

    -- Signal strength scoring
    case epa_mismatch_level
      when 'large_mismatch' then 30
      when 'moderate_mismatch' then 20
      when 'slight_mismatch' then 10
      else 0
    end as epa_signal_strength,

    case rank_mismatch_level
      when 'large_rank_gap' then 15
      when 'moderate_rank_gap' then 8
      else 0
    end as rank_signal_strength,

    case momentum_edge
      when 'home_momentum_edge' then 12
      when 'away_momentum_edge' then 12
      else 0
    end as momentum_signal_strength,

    case form_edge
      when 'home_form_edge' then 15
      when 'away_form_edge' then 15
      when 'both_hot' then -5  -- Tougher to predict
      when 'both_cold' then -5
      else 0
    end as form_signal_strength,

    -- Data quality adjustment
    case feature_quality
      when 'high_quality' then 1.0
      when 'medium_quality' then 0.8
      when 'low_quality' then 0.5
      else 0.3
    end as quality_multiplier

  from mismatch_analysis
),

confidence_scoring as (
  select
    *,

    -- Calculate total confidence score (0-100)
    least(95, greatest(15,
      (epa_signal_strength + rank_signal_strength + momentum_signal_strength + form_signal_strength) * quality_multiplier + 25
    )) as confidence_score,

    -- Determine recommended side
    case
      when diff_total_epa_l3 > 0.05 then home_team
      when diff_total_epa_l3 < -0.05 then away_team
      else 'No Strong Lean'
    end as recommended_side,

    -- Calculate edge strength
    case
      when abs(diff_total_epa_l3) >= 0.15 and epa_mismatch_level = 'large_mismatch' then 'Strong Edge'
      when abs(diff_total_epa_l3) >= 0.08 and form_edge in ('home_form_edge', 'away_form_edge') then 'Moderate Edge'
      when abs(diff_total_epa_l3) >= 0.03 then 'Slight Edge'
      else 'No Clear Edge'
    end as edge_strength

  from betting_signals
),

final_analysis as (
  select
    *,

    -- Betting recommendation tiers
    case
      when confidence_score >= {{ confidence_thresholds.high }} then 'High Confidence'
      when confidence_score >= {{ confidence_thresholds.medium }} then 'Medium Confidence'
      when confidence_score >= {{ confidence_thresholds.low }} then 'Low Confidence'
      else 'Avoid'
    end as bet_confidence_tier,

    -- Key talking points for analysis
    case
      when epa_mismatch_level = 'large_mismatch' and form_edge in ('home_form_edge', 'away_form_edge') then
        'Strong mismatch with form advantage'
      when momentum_edge != 'neutral_momentum' and rank_mismatch_level = 'large_rank_gap' then
        'Momentum play with talent gap'
      when strength_matchup = 'elite_vs_weak' then
        'Elite team vs struggling opponent'
      when form_matchup = 'both_hot' then
        'High-scoring potential shootout'
      else 'Standard analysis'
    end as betting_narrative,

    -- Game priority for research
    case
      when confidence_score >= {{ confidence_thresholds.high }} and edge_strength = 'Strong Edge' then 1
      when confidence_score >= {{ confidence_thresholds.medium }} then 2
      when edge_strength in ('Moderate Edge', 'Slight Edge') then 3
      else 4
    end as research_priority,

    -- Days until game for urgency
    date_diff(game_date, current_date(), day) as days_until_game,

    -- Record timestamp
    current_timestamp() as analysis_timestamp

  from confidence_scoring
)

select
  -- Game identification
  game_id,
  season,
  week,
  game_date,
  home_team,
  away_team,
  game_day_name,
  days_until_game,

  -- Betting analysis
  confidence_score,
  bet_confidence_tier,
  recommended_side,
  edge_strength,
  betting_narrative,
  research_priority,

  -- Key metrics for decision making
  diff_total_epa_l3 as epa_differential,
  diff_season_rank as rank_differential,
  epa_mismatch_level,
  form_edge,
  momentum_edge,

  -- Supporting context
  home_recent_form,
  away_recent_form,
  home_strength_tier,
  away_strength_tier,
  feature_quality,

  -- Metadata
  analysis_timestamp

from final_analysis
where confidence_score >= 35  -- Filter out very low confidence games
order by research_priority, confidence_score desc, days_until_game