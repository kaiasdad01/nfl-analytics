{{
  config(
    materialized='table',
    description='Team performance by game with rolling averages and advanced metrics'
  )
}}

{# Configuration variables for easy editing #}
{%- set rolling_windows = [3, 5, 8] -%}
{%- set base_metrics = [
    'total_yards',
    'total_epa',
    'points_scored',
    'third_down_conversion_rate',
    'red_zone_efficiency',
    'turnovers',
    'success_rate'
] -%}

with base_team_stats as (
  select
    game_id,
    season,
    week,
    team,
    opponent,
    home_away,

    -- Core performance metrics
    {% for metric in base_metrics %}
    {{ metric }},
    {%- endfor %}

    -- Additional context
    total_plays,
    successful_plays

  from {{ ref('stg_team_stats') }}
  where team is not null
),

rolling_averages as (
  select
    *,

    {# Generate rolling averages dynamically #}
    {% for window in rolling_windows %}
      {% for metric in base_metrics %}
    avg({{ metric }}) over (
      partition by season, team
      order by week
      rows between {{ window }} preceding and 1 preceding
    ) as {{ metric }}_l{{ window }},
      {%- endfor %}
    {% endfor %}

    -- Season-to-date context
    avg(total_epa) over (
      partition by season, team
      order by week
      rows unbounded preceding
    ) as total_epa_season_avg,

    count(*) over (
      partition by season, team
      order by week
      rows unbounded preceding
    ) as games_played_to_date

  from base_team_stats
),

trend_analysis as (
  select
    *,

    -- Momentum indicators (3-game vs 8-game trends)
    coalesce(total_epa_l3, 0) - coalesce(total_epa_l8, 0) as epa_trend_3v8,
    coalesce(total_yards_l3, 0) - coalesce(total_yards_l8, 0) as yards_trend_3v8,
    coalesce(points_scored_l3, 0) - coalesce(points_scored_l8, 0) as scoring_trend_3v8,

    -- Consistency metrics
    stddev(total_epa) over (
      partition by season, team
      order by week
      rows between 5 preceding and 1 preceding
    ) as epa_volatility_l5,

    -- Efficiency ratios
    safe_divide(total_yards, nullif(total_plays, 0)) as yards_per_play,
    safe_divide(total_epa, nullif(total_plays, 0)) as epa_per_play,
    safe_divide(points_scored, nullif(total_yards, 0)) * 100 as points_per_100_yards

  from rolling_averages
),

form_indicators as (
  select
    *,

    -- Recent form classification
    case
      when total_epa_l3 > total_epa_season_avg * 1.15 then 'hot'
      when total_epa_l3 < total_epa_season_avg * 0.85 then 'cold'
      else 'average'
    end as recent_form,

    -- Season strength tier
    case
      when total_epa_season_avg > 0.10 then 'elite'
      when total_epa_season_avg > 0.00 then 'above_average'
      when total_epa_season_avg > -0.10 then 'below_average'
      else 'poor'
    end as season_strength_tier,

    -- Momentum classification
    case
      when epa_trend_3v8 > 0.05 then 'trending_up'
      when epa_trend_3v8 < -0.05 then 'trending_down'
      else 'stable'
    end as momentum_direction

  from trend_analysis
),

final_rankings as (
  select
    *,

    -- Weekly performance rankings
    row_number() over (
      partition by season, week
      order by total_epa desc
    ) as epa_rank_weekly,

    row_number() over (
      partition by season, week
      order by total_yards desc
    ) as yards_rank_weekly,

    -- Season rankings
    row_number() over (
      partition by season
      order by total_epa_season_avg desc
    ) as epa_rank_season,

    -- Percentile rankings (0-100 scale)
    round(
      percent_rank() over (
        partition by season, week
        order by total_epa
      ) * 100, 1
    ) as epa_percentile_weekly,

    -- Record updated timestamp for tracking
    current_timestamp() as updated_at

  from form_indicators
)

select * from final_rankings
order by season desc, week desc, epa_rank_weekly