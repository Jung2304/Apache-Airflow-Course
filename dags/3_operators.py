from airflow.sdk import dag, task

@dag(
  dag_id="bash_dag",
  schedule="@daily",
  catchup=False,        # No automatic backfill
)
def operators_dag():

  @task.bash
  def run_after_loop() -> str:
    return "echo https://airflow.apache.org/"

  @task.python
  def first_task():
    print("First PythonOperator task")

  run_after_loop() >> first_task()

operators_dag()