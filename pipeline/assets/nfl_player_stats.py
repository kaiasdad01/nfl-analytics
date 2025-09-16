from dagster import asset, AssetExecutionContext
import nfl_data_py
import pandas as pd
from ..config.settings import SEASONS
from ..utils.bigquery_client import store_dataframe_in_bigquery

@asset
def nfl_player_stats(context: AssetExecutionContext) -> pd.DataFrame:
    """Load NFL player stats data from API and store in BigQuery"""
    context.log.info(f"Loading NFL player stats for seasons: {SEASONS}")

    player_stats = nfl_data_py.import_weekly_data(SEASONS)
    context.log.info(f"Loaded {len(player_stats)} player stat records")

    store_dataframe_in_bigquery(context, player_stats, "player_stats")
    
    return player_stats