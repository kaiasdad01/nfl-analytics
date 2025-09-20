from dagster import asset, AssetExecutionContext
import nfl_data_py
import pandas as pd
from ..config.settings import SEASONS
from ..utils.bigquery_client import store_dataframe_in_bigquery

@asset
def current_season_stats(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Load current NFL stats by week
    """
    context.log.info("checking for current season stats...")

    results = {} 

    try: 
        pbp_2025 = nfl_data_py.import_pbp_data([2025], cache=False)
        if len(pbp_2025) > 0: 
            context.log.info(f"Loaded {len(pbp_2025)} plays in 2025")
            store_dataframe_in_bigquery(context, pbp_2025, "pbp_2025")
            results['pbp_games'] = len(pbp_2025['game_id'].unique())
        else:
            context.log.info("No 2025 PBP data found")
            results['pbp_games'] = 0
        
        try:
            stats_2025 = nfl_data_py.import_weekly_data([2025])
            if len(stats_2025) > 0: 
                context.log.info(f"Loaded {len(stats_2025)} stats for 2025")
                store_dataframe_in_bigquery(context, stats_2025, "player_stats_2025")
                results['stats_week'] = len(stats_2025['week'].unique())
            else:
                results['stats_week'] = 0
        except Exception as e:
            context.log.error(f"Error loading 2025 player stats: {str(e)}")
            context.log.info("No 2025 player stats found")
            results['stats_week'] = 0
        
        # Check for roster changes
        try: 
            rosters_2025 = nfl_data_py.import_seasonal_rosters([2025])
            if len(rosters_2025) > 0: 
                context.log.info(f"Loaded {len(rosters_2025)} rosters in 2025")
                store_dataframe_in_bigquery(context, rosters_2025, "rosters_2025")
                results['roster_players'] = len(rosters_2025['player_id'].unique())
            else: 
                results['roster_players'] = 0
        except Exception as e:
            context.log.error(f"Error loading 2025 roster data: {str(e)}")
            context.log.info("No 2025 roster data")
            results['roster_players'] = 0
        
        context.log.info(f"2025 Season Stats Summary: {results}")
        return results

    except Exception as e:
        context.log.error(f"Error loading 2025 season stats: {str(e)}")
        return {'error': str(e)}