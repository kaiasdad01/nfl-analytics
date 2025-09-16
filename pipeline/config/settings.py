"""
Config settings for analytics pipeline
"""

# Data Settings
SEASONS = [2020, 2021, 2022, 2023, 2024]

# BigQuery Settings
BIGQUERY_PROJECT = "nfl-analytics-472221"
BIGQUERY_DATASET = "nfl_data"
BIGQUERY_LOCATION = "US"
WRITE_DISPOSITION = "WRITE_TRUNCATE"

# API Settings
NFL_API_TIMEOUT = 30
BATCH_SIZE = 1000


# API Settings