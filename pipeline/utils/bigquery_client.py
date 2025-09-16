from google.cloud import bigquery
from dagster import AssetExecutionContext
from ..config.settings import BIGQUERY_PROJECT, BIGQUERY_DATASET, WRITE_DISPOSITION

def get_bigquery_client() -> bigquery.Client:
    import os 
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
    return bigquery.Client()

def store_dataframe_in_bigquery(
    context: AssetExecutionContext,
    df,
    table_name: str,
    project: str = BIGQUERY_PROJECT,
    dataset: str = BIGQUERY_DATASET
) -> None:
    client = get_bigquery_client()
    table_id = f"{project}.{dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=WRITE_DISPOSITION,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    context.log.info(f"Stored {len(df)} records in {table_id}")