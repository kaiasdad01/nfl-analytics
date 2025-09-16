from dagster import asset, AssetExecutionContext
import nfl_data_py
import pandas as pd
from ..config.settings import SEASONS
from ..utils.bigquery_client import store_dataframe_in_bigquery

@asset
def nfl_ngs(context: AssetExecutionContext) -> pd.DataFrame:
    """Load NFL Next Gen Stats data from API and store in BigQuery"""
    context.log.info(f"Loading NFL Next Gen Stats data for seasons: {SEASONS}")

    try:
        ngs_data = []

        # Load players reference to map identifiers to canonical player_id (GSIS)
        players = nfl_data_py.import_players()
        # Detect canonical player id column name in players reference
        canonical_player_id_col = "player_id" if "player_id" in players.columns else (
            "gsis_id" if "gsis_id" in players.columns else None
        )
        if canonical_player_id_col is None:
            context.log.warning("Players reference missing canonical player id column; available columns: %s" % list(players.columns))
        id_columns = [
            col
            for col in [
                canonical_player_id_col,
                "nfl_id",
                "esb_id",
                "gsis_it_id",
                "pfr_id",
                "espn_id",
                "sportradar_id",
                "yahoo_id",
                "player_display_name",
            ]
            if col is not None and col in players.columns
        ]
        players_ids = players[id_columns].drop_duplicates()

        for stat_type in ['passing', 'rushing', 'receiving']:
            context.log.info(f"Loading {stat_type} NGS data...")
            stat_data = nfl_data_py.import_ngs_data(stat_type=stat_type, years=SEASONS)
            # Log columns we have to decide mapping strategy
            context.log.info(f"NGS cols ({stat_type}): {list(stat_data.columns)}")

            # Ensure we have a player_id by mapping available identifiers
            # Fast-path: NGS often exposes GSIS id as player_gsis_id
            if "player_id" not in stat_data.columns and "player_gsis_id" in stat_data.columns:
                stat_data["player_id"] = stat_data["player_gsis_id"]

            if "player_id" not in stat_data.columns:
                # Try a sequence of merges based on available identifier columns
                merged = stat_data
                mapped = False
                # (1) nfl_id direct
                if not mapped and "nfl_id" in merged.columns and "nfl_id" in players_ids.columns:
                    merged = merged.merge(
                        players_ids[[canonical_player_id_col, "nfl_id"]].dropna().rename(columns={canonical_player_id_col: "player_id"}),
                        on="nfl_id",
                        how="left",
                    )
                    mapped = True
                # (2) esb_id
                if not mapped and "esb_id" in merged.columns and "esb_id" in players_ids.columns:
                    merged = merged.merge(
                        players_ids[[canonical_player_id_col, "esb_id"]].dropna().rename(columns={canonical_player_id_col: "player_id"}),
                        on="esb_id",
                        how="left",
                    )
                    mapped = True
                # (3) gsis_id aliases commonly seen in datasets
                for candidate in ["gsis_id", "player_gsis_id", "gsis_it_id"]:
                    if not mapped and candidate in merged.columns:
                        # players_ids has canonical player_id; some datasets use same value under different column name
                        # Align by renaming candidate to player_id-like then merge on equality via left join keys
                        if canonical_player_id_col in players_ids.columns:
                            tmp = players_ids[[canonical_player_id_col]].rename(columns={canonical_player_id_col: candidate})
                            merged = merged.merge(tmp.dropna().drop_duplicates(), on=candidate, how="left")
                            # After merge, set player_id from candidate since values match canonical
                            if "player_id" not in merged.columns:
                                merged["player_id"] = merged[candidate]
                        mapped = True
                        break
                # (4) fall back to name join if needed (lower quality)
                if not mapped and "player_display_name" in merged.columns and "player_display_name" in players_ids.columns:
                    merged["_name_key"] = merged["player_display_name"].astype(str).str.strip().str.lower()
                    players_ids = players_ids.copy()
                    players_ids["_name_key"] = players_ids["player_display_name"].astype(str).str.strip().str.lower()
                    merged = merged.merge(
                        players_ids[[canonical_player_id_col, "_name_key"]].dropna().drop_duplicates().rename(columns={canonical_player_id_col: "player_id"}),
                        on="_name_key",
                        how="left",
                    ).drop(columns=["_name_key"], errors="ignore")
                    mapped = True

                # Use merged result
                stat_data = merged

        
            if "player_id" in stat_data.columns:
                unmapped = int(stat_data["player_id"].isna().sum())
                total_rows = len(stat_data)
                context.log.info(
                    f"NGS ({stat_type}) mapping coverage: {total_rows - unmapped}/{total_rows} mapped to player_id"
                )
                stat_data = stat_data[stat_data["player_id"].notna()].copy()

            stat_data["stat_type"] = stat_type
            ngs_data.append(stat_data)
            context.log.info(f"Loaded {len(stat_data)} {stat_type} NGS rows after mapping")
        
        combined_ngs = pd.concat(ngs_data, ignore_index=True)

        if len(combined_ngs) == 0:
            raise ValueError("No NGS data loaded")

        required_columns = ['player_id', 'season', 'stat_type']
        missing_columns = [col for col in required_columns if col not in combined_ngs.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        context.log.info(f"Data quality metrics:")
        context.log.info(f"  - Seasons: {sorted(combined_ngs['season'].unique())}")
        context.log.info(f"  - Stat types: {combined_ngs['stat_type'].value_counts().to_dict()}")
        context.log.info(f"  - Players: {len(combined_ngs['player_id'].unique())} unique players")
        context.log.info(f"  - Missing values: {combined_ngs.isnull().sum().sum()}")
        
        store_dataframe_in_bigquery(context, combined_ngs, "ngs_data")
        context.log.info(f"Stored {len(combined_ngs)} Next Gen Stats data in BigQuery")

        return combined_ngs
       
    except Exception as e: 
        context.log.error(f"Failed to load Next Gen Stats data: {str(e)}")
        raise