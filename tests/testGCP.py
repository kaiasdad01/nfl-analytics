from google.cloud import bigquery 
import os 

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

client = bigquery.Client()
print("GCP connection successful")
print(f"Project: {client.project}")

dataset_id = "nfl_data"
dataset = client.dataset(dataset_id)
print(f"Dataset {dataset_id} exists!")