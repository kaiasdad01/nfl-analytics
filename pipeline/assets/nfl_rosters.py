from dagster import asset, AssetExecutionContext
import nfl_data_py
import pandas as pd
from ..config.settings import SEASONS
from ..utils.bigquery_client import store_dataframe_in_bigquery

@asset
def nfl_rosters(context: AssetExecutionContext) -> pd.DataFrame:
    """Load NFL rosters data from API and store in BigQuery"""
    context.log.info(f"Loading NFL rosters data for seasons: {SEASONS}")

    try: 
        rosters = nfl_data_py.import_seasonal_rosters(SEASONS)
        context.log.info(f"Loaded {len(rosters)} rosters")

        if len(rosters) == 0: 
            raise ValueError("No roster data loaded")
        
        required_columns = ['player_id', 'team', 'season', 'position']
        missing_columns = [col for col in required_columns if col not in rosters.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        context.log.info(f"Data quality metrics:")
        context.log.info(f" - Seasons: {sorted(rosters['season'].unique())}")
        context.log.info(f" - Teams: {len(rosters['team'].unique())} unique teams")
        context.log.info(f" - Players: {len(rosters['player_id'].unique())} unique players")
        context.log.info(f" - Missing values: {rosters.isnull().sum().sum()}")

        store_dataframe_in_bigquery(context, rosters, "rosters")
        context.log.info(f"Stored {len(rosters)} rosters in BigQuery")

        return rosters
        
    except Exception as e: 
        context.log.error(f"Failed to load rosters data: {str(e)}")
        raise