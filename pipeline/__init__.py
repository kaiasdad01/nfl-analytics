"""
NFL pipeline
"""

from dagster import Definitions

from pipeline.assets.nfl_schedule import nfl_current_schedule
from .assets.nfl_games import nfl_games
from .assets.nfl_player_stats import nfl_player_stats
from .assets.nfl_pbp import nfl_pbp
from .assets.nfl_rosters import nfl_rosters
from .assets.nfl_ngs import nfl_ngs
from .assets.nfl_schedule import nfl_current_schedule
from .assets.nfl_current_stats import current_season_stats

defs = Definitions(
    assets=[nfl_games, nfl_player_stats, nfl_pbp, nfl_rosters, nfl_ngs, nfl_current_schedule, 
    current_season_stats])