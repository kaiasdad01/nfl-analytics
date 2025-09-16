from dagster import asset, AssetExecutionContext
import nfl_data_py
import pandas as pd
from ..config.settings import SEASONS
from ..utils.bigquery_client import store_dataframe_in_bigquery

@asset
def nfl_pbp(context: AssetExecutionContext) -> pd.DataFrame:
    """Load NFL PBP data from API and store in BigQuery"""
    context.log.info(f"Loading NFL PBP data for seasons: {SEASONS}")

    try:    
        pbp_data = nfl_data_py.import_pbp_data(SEASONS, cache=False)
        context.log.info(f"Loaded {len(pbp_data)} plays with {len(pbp_data.columns)} columns")

        # Data Qual Checks
        if len(pbp_data) == 0: 
            raise ValueError("No PBP data loaded")
        
        required_columns = ['game_id', 'posteam', 'play_type', 'season']
        missing_columns = [col for col in required_columns if col not in pbp_data.columns]
        if missing_columns: 
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Log Qual Metrics
        context.log.info(f"Data quality metrics:")
        context.log.info(f" - Date range: {pbp_data['game_date'].min()} to {pbp_data['game_date'].max()}")
        context.log.info(f" - Seasons: {sorted(pbp_data['season'].unique())}")
        context.log.info(f" - Missing values: {pbp_data.isnull().sum().sum()}")

        store_dataframe_in_bigquery(context, pbp_data, "pbp_data")
        context.log.info(f"Stored {len(pbp_data)} plays in BigQuery")
        
        return pbp_data

    except Exception as e: 
        context.log.error(f"Failed to load pbp data: {str(e)}")
        raise
