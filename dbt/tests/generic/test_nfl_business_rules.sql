-- Custom tests for NFL-specific business rules

-- Test that EPA values are reasonable for NFL games
{% test reasonable_epa_values(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < -40 or {{ column_name }} > 40)

{% endtest %}

-- Test that team abbreviations are valid NFL teams
{% test valid_nfl_team(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} not in (
  'ARI', 'ATL', 'BAL', 'BUF', 'CAR', 'CHI', 'CIN', 'CLE', 'DAL', 'DEN',
  'DET', 'GB', 'HOU', 'IND', 'JAX', 'KC', 'LV', 'LAC', 'LAR', 'MIA',
  'MIN', 'NE', 'NO', 'NYG', 'NYJ', 'PHI', 'PIT', 'SEA', 'SF', 'TB', 'TEN', 'WAS'
)

{% endtest %}

-- Test that games have reasonable scores (not negative, not impossibly high)
{% test reasonable_nfl_score(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < 0 or {{ column_name }} > 70)

{% endtest %}

-- Test that we don't have teams playing themselves
{% test teams_not_playing_themselves(model, home_team_column, away_team_column) %}

select *
from {{ model }}
where {{ home_team_column }} = {{ away_team_column }}

{% endtest %}

-- Test that rolling averages are within reasonable bounds of actual values
{% test rolling_average_sanity(model, current_column, rolling_column) %}

select *
from {{ model }}
where {{ current_column }} is not null
  and {{ rolling_column }} is not null
  and abs({{ current_column }} - {{ rolling_column }}) > 50  -- Large deviation check

{% endtest %}