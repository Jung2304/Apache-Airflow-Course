# DAG versions track changes to the DAG definition
# Modifying the DAG between runs caused Airflow to register v2

from airflow.sdk import dag, task

@dag(
  dag_id="versioned_dag",
  schedule="@daily",
  catchup=False,        # No automatic backfill
)
def versioned_dag():

  @task.python
  def first_task():
    print("This is the first task")

  @task.python
  def second_task():
    print("This is the second task")

  @task.python
  def third_task(): 
    print("This is the third task")

  @task.python
  def version_task():
    print("This is the version task. DAG Version 2.0!")

  @task.python
  def version_task_2():
    print("This is the version task 2. DAG Version 2.0!")

  first_task() >> second_task() >> third_task() >> version_task() >> version_task_2()

versioned_dag()