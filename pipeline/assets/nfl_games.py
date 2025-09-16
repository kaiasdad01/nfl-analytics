from dagster import asset, AssetExecutionContext
import nfl_data_py
import pandas as pd
from ..config.settings import SEASONS
from ..utils.bigquery_client import store_dataframe_in_bigquery

@asset
def nfl_games(context: AssetExecutionContext) -> pd.DataFrame:
    """Load NFL games data from API and store in BigQuery"""
    context.log.info(f"Loading NFL games data for seasons: {SEASONS}")

    games = nfl_data_py.import_schedules(SEASONS)
    context.log.info(f"Loaded {len(games)} games")

    store_dataframe_in_bigquery(context, games, "games")

    return games