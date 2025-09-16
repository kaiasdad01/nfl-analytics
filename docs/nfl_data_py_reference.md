# NFL Data Py Reference Guide

## Available Data Sources

### Core Data Functions

| Function | Description | Key Parameters | Use Case |
|----------|-------------|----------------|----------|
| `import_schedules()` | Game schedules, scores, betting lines | `years` | Game outcomes, team performance |
| `import_weekly_data()` | Player stats by week | `years, columns, downcast` | Player performance, fantasy points |
| `import_ngs_data()` | Next Gen Stats (advanced metrics) | `stat_type, years` | Advanced player analysis |
| `import_pbp_data()` | Play-by-play data | `years, columns, cache` | Detailed game analysis |
| `import_players()` | Player metadata | None | Player information |
| `import_team_desc()` | Team information | None | Team metadata |
| `import_injuries()` | Injury reports | `years` | Health status tracking |
| `import_contracts()` | Player contracts | None | Salary cap analysis |
| `import_draft_picks()` | Draft information | `years` | Draft analysis |

### Key Datasets Overview

#### 1. Schedules (46 columns)
**Key columns for ML:**
- `game_id`, `season`, `week`, `gameday`
- `away_team`, `home_team`, `away_score`, `home_score`
- `spread_line`, `total_line`, `away_moneyline`, `home_moneyline`
- `roof`, `surface`, `temp`, `wind` (weather)
- `stadium`, `referee`

#### 2. Weekly Player Data (53 columns)
**Key columns for ML:**
- `player_id`, `player_name`, `position`, `recent_team`
- `season`, `week`, `opponent_team`
- **Passing:** `completions`, `attempts`, `passing_yards`, `passing_tds`, `interceptions`
- **Rushing:** `carries`, `rushing_yards`, `rushing_tds`
- **Receiving:** `receptions`, `targets`, `receiving_yards`, `receiving_tds`
- **Fantasy:** `fantasy_points`, `fantasy_points_ppr`
- **Advanced:** `passing_epa`, `rushing_epa`, `receiving_epa`

#### 3. Next Gen Stats
**Available types:**
- `passing` (29 columns) - Advanced passing metrics
- `rushing` (22 columns) - Advanced rushing metrics  
- `receiving` (23 columns) - Advanced receiving metrics

## ML Feature Mapping

### Game Outcome Prediction
**Available features:**
- ✅ `home_team`, `away_team`, `home_score`, `away_score`
- ✅ `season`, `week`, `spread_line`, `total_line`
- ✅ `roof`, `surface`, `temp`, `wind`
- ✅ `stadium`, `referee`

### Player Fantasy Points Prediction
**Available features:**
- ✅ `player_id`, `position`, `recent_team`
- ✅ `passing_yards`, `rushing_yards`, `receiving_yards`
- ✅ `passing_tds`, `rushing_tds`, `receiving_tds`
- ✅ `interceptions`, `fumbles`
- ✅ `fantasy_points` (target variable)

### Team Performance Prediction
**Available features:**
- ✅ Team-level aggregations from player stats
- ✅ Game-level team stats from schedules
- ✅ `time_of_possession`, `third_down_conversion` (from play-by-play)
- ✅ `red_zone_efficiency` (derivable from play-by-play)
- ✅ `epa` (Expected Points Added) from play-by-play

## Usage Examples

```python
import nfl_data_py as nfl

# Get recent seasons
seasons = [2022, 2023, 2024]

# Load core datasets
games = nfl.import_schedules(seasons)
player_stats = nfl.import_weekly_data(seasons)

# Load advanced metrics
ngs_passing = nfl.import_ngs_data(stat_type='passing', years=seasons)
ngs_rushing = nfl.import_ngs_data(stat_type='rushing', years=seasons)
ngs_receiving = nfl.import_ngs_data(stat_type='receiving', years=seasons)

# Load additional data
players = nfl.import_players()
teams = nfl.import_team_desc()
rosters = nfl.import_seasonal_rosters(seasons)  # CRITICAL for player-team context
injuries = nfl.import_injuries(seasons)
```

## Data Quality Notes

- **Schedules:** Complete game data with betting lines
- **Weekly Data:** Comprehensive player stats with fantasy points
- **NGS Data:** Advanced metrics for deeper analysis
- **Missing:** Some team-level stats require play-by-play aggregation
- **Updates:** Data typically updated within 24 hours of games

## Next Steps for ML

1. **Start with player fantasy points** - most complete feature set
2. **Add game outcome prediction** - good feature coverage  
3. **Use play-by-play data** for team performance metrics (now available!)
4. **Use seasonal rosters** for player-team context (critical for accuracy)
5. **Use NGS data** for advanced player analysis

## Key Insights

### Player-Team Context is Critical
- **Daniel Jones on NYG (2022-2023)**: Struggled with poor offensive line
- **Daniel Jones on MIN (2024)**: Thriving with better system
- **Always join player stats with seasonal rosters** to get correct team context

### Play-by-Play Data Unlocks Team Performance
- **397 columns** of detailed game data
- **Team-level metrics**: EPA, time of possession, third down conversion
- **Drive-level data**: Red zone efficiency, drive success rates
- **Game situation**: Down and distance, field position, time remaining
