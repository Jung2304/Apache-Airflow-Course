from airflow.sdk import dag, task, asset
import os

@asset(
  schedule = "@daily",
  uri = "/opt/airflow/logs/data/data_extract.txt",
  name = "extract_data"
)
def extract_data(self):

  # Ensure the directory exists
  os.makedirs(os.path.dirname(self.uri), exist_ok = True)

  # Simulate data fetching by writing to a file
  with open(self.uri, 'w') as f:
    f.write(f"Data fetched successfully")

  print(f"Data written to {self.uri}")