"""
NFL pipeline
"""

from dagster import Definitions
from .assets.nfl_games import nfl_games
from .assets.nfl_player_stats import nfl_player_stats
from .assets.nfl_pbp import nfl_pbp
from .assets.nfl_rosters import nfl_rosters
from .assets.nfl_ngs import nfl_ngs

defs = Definitions(
    assets=[nfl_games, nfl_player_stats, nfl_pbp, nfl_rosters, nfl_ngs]
)