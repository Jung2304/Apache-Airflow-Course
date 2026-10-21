from airflow.sdk import dag, task
import os

@dag(
  dag_id="first_orchestrator_dag",
  schedule="@daily",
  catchup=False,        # No automatic backfill
)
def first_orchestrator_dag():

  @task.python
  def first_task():
    print("This is the first task")

  @task.python
  def second_task():
    print("This is the second task")

  @task.python
  def third_task(): 
    # Ensure the directory exists
    os.makedirs(os.path.dirname("/opt/airflow/logs/data"), exist_ok = True)

    # Process and write data
    with open("/opt/airflow/logs/data/output_1.txt", 'w') as f:
      f.write(f"Processed successfully")

  first_task() >> second_task() >> third_task()

first_orchestrator_dag()