from airflow.sdk import dag, task, asset
import os
from assets_13 import extract_data

@asset(
  schedule = extract_data,
  uri = "/opt/airflow/logs/data/data_processed.txt",
  name = "process_data"
)
def process_data(self):
  # Read the upstream asset 
  with open(extract_data.uri, "r") as f:
    extracted_data = f.read()

  # Ensure the directory exists
  os.makedirs(os.path.dirname(self.uri), exist_ok = True)

  # Process and write data
  with open(self.uri, 'w') as f:
    f.write(f"Processed {extracted_data}")

  print(f"Data processed is written to {self.uri}")