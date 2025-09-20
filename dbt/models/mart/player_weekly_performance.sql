{{
  config(
    materialized='table',
    description='Player weekly performance with usage trends and team context'
  )
}}

{# Configuration for rolling analysis #}
{%- set rolling_weeks = [3, 5] -%}

with player_base as (
  select
    ps.player_id,
    ps.season,
    ps.week,
    ps.team,
    ps.position,

    -- Core performance stats
    ps.passing_attempts,
    ps.passing_completions,
    ps.passing_yards,
    ps.passing_tds,
    ps.interceptions,
    ps.rushing_attempts,
    ps.rushing_yards,
    ps.rushing_tds,
    ps.targets,
    ps.receptions,
    ps.receiving_yards,
    ps.receiving_tds,
    ps.fantasy_points,
    ps.fantasy_points_ppr,

    -- Derived efficiency metrics
    ps.completion_percentage,
    ps.yards_per_attempt,
    ps.yards_per_carry,
    ps.catch_rate,
    ps.yards_per_reception,

    -- Player context
    p.player_name,
    p.position_group,
    p.age,
    p.nfl_experience,
    p.draft_pedigree,

    -- Roster context
    r.roster_role,
    r.roster_status_clean

  from {{ ref('stg_player_stats') }} ps
  join {{ ref('stg_players') }} p on ps.player_id = p.player_id
  left join {{ ref('stg_rosters') }} r
    on ps.player_id = r.player_id
    and ps.season = r.season
  where ps.player_id is not null
),

team_context as (
  select
    pb.*,

    -- Team performance context
    t.season_strength_tier as team_strength,
    t.recent_form as team_form,
    t.total_epa as team_epa,
    t.points_scored as team_points,
    t.total_plays as team_plays,

    -- Usage opportunity context
    safe_divide(pb.targets, nullif(t.total_plays, 0)) as target_share_of_plays,
    safe_divide(pb.rushing_attempts, nullif(t.total_plays, 0)) as rush_share_of_plays

  from player_base pb
  left join {{ ref('team_game_performance') }} t
    on pb.team = t.team
    and pb.season = t.season
    and pb.week = t.week
),

rolling_performance as (
  select
    *,

    {# Generate rolling averages for key metrics #}
    {% for weeks in rolling_weeks %}
    -- Fantasy performance trends ({{ weeks }}-week)
    avg(fantasy_points) over (
      partition by player_id, season
      order by week
      rows between {{ weeks }} preceding and 1 preceding
    ) as fantasy_points_l{{ weeks }},

    avg(fantasy_points_ppr) over (
      partition by player_id, season
      order by week
      rows between {{ weeks }} preceding and 1 preceding
    ) as fantasy_points_ppr_l{{ weeks }},

    -- Usage trends ({{ weeks }}-week)
    avg(targets) over (
      partition by player_id, season
      order by week
      rows between {{ weeks }} preceding and 1 preceding
    ) as targets_l{{ weeks }},

    avg(rushing_attempts) over (
      partition by player_id, season
      order by week
      rows between {{ weeks }} preceding and 1 preceding
    ) as rushing_attempts_l{{ weeks }},

    -- Efficiency trends ({{ weeks }}-week)
    avg(yards_per_attempt) over (
      partition by player_id, season
      order by week
      rows between {{ weeks }} preceding and 1 preceding
    ) as yards_per_attempt_l{{ weeks }},

    avg(catch_rate) over (
      partition by player_id, season
      order by week
      rows between {{ weeks }} preceding and 1 preceding
    ) as catch_rate_l{{ weeks }},
    {%- endfor %}

    -- Season totals for context
    sum(fantasy_points) over (
      partition by player_id, season
      order by week
      rows unbounded preceding
    ) as fantasy_points_season_total,

    count(*) over (
      partition by player_id, season
      order by week
      rows unbounded preceding
    ) as games_played_season

  from team_context
),

trend_analysis as (
  select
    *,

    -- Performance trends
    case
      when fantasy_points > coalesce(fantasy_points_l3, 0) * 1.25 then 'trending_up'
      when fantasy_points < coalesce(fantasy_points_l3, 0) * 0.75 then 'trending_down'
      else 'stable'
    end as performance_trend,

    -- Usage trends
    case
      when targets > coalesce(targets_l3, 0) * 1.2 then 'usage_increasing'
      when targets < coalesce(targets_l3, 0) * 0.8 then 'usage_decreasing'
      else 'usage_stable'
    end as usage_trend,

    -- Opportunity vs production efficiency
    safe_divide(fantasy_points, nullif(targets + rushing_attempts, 0)) as points_per_opportunity,

    -- Consistency metrics
    case
      when games_played_season >= 4 then
        stddev(fantasy_points) over (
          partition by player_id, season
          order by week
          rows between 4 preceding and current row
        )
      else null
    end as fantasy_volatility

  from rolling_performance
),

final_metrics as (
  select
    *,

    -- Performance classifications
    case
      when fantasy_points >= 20 then 'elite_week'
      when fantasy_points >= 15 then 'wr1_week'
      when fantasy_points >= 10 then 'wr2_week'
      when fantasy_points >= 5 then 'flex_week'
      else 'bust_week'
    end as weekly_performance_tier,

    -- Injury risk indicators (basic)
    case
      when roster_status_clean != 'active' then true
      when age >= 30 and position_group in ('RB', 'WR') then true
      else false
    end as injury_risk_flag,

    -- Breakout/bust potential indicators
    case
      when nfl_experience <= 2 and performance_trend = 'trending_up' then 'breakout_candidate'
      when age >= 32 and performance_trend = 'trending_down' then 'decline_candidate'
      when usage_trend = 'usage_increasing' and team_form = 'hot' then 'opportunity_riser'
      else 'stable_player'
    end as player_narrative,

    -- Fantasy relevance score (0-100)
    least(100, greatest(0,
      coalesce(fantasy_points_l3, 0) * 3 +
      case when roster_role = 'starter' then 15 else 5 end +
      case when team_strength in ('elite', 'above_average') then 10 else 0 end +
      case when usage_trend = 'usage_increasing' then 10 else 0 end
    )) as fantasy_relevance_score,

    -- Record metadata
    current_timestamp() as updated_at

  from trend_analysis
)

select * from final_metrics
where games_played_season >= 1  -- Filter out players who haven't played
order by season desc, week desc, fantasy_relevance_score desc