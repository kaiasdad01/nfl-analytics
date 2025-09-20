from dagster import asset, AssetExecutionContext
import nfl_data_py
import pandas as pd
from ..config.settings import SEASONS
from ..utils.bigquery_client import store_dataframe_in_bigquery

@asset
def nfl_current_schedule(context: AssetExecutionContext) -> pd.DataFrame:
    """Load NFL current schedule data from API and store in BigQuery"""
    context.log.info(f"Loading NFL current schedule data for current season")

    current_schedule = nfl_data_py.import_schedules([2025])
    context.log.info(f"Loaded {len(current_schedule)} schedule records")

    store_dataframe_in_bigquery(context, current_schedule, "current_schedule")

