from airflow.sdk import dag, task
from pendulum import datetime       # use pendulum to avoid timezone conflict 

@dag(
  dag_id = "schedule_dag",
  start_date = datetime(year=2026, month=10, day=1, tz="Asia/Ho_Chi_Minh"),
  schedule = "@daily",
  catchup = False,        # No automatic backfill
  is_paused_upon_creation = False       # new DAG is ready for automatic scheduling without manually enabling it
)

def schedule_dag():

  @task.python
  def first_task():
    print("This is the first task")

  @task.python
  def second_task():
    print("This is the second task")

  @task.python
  def third_task(): 
    print("This is the third task")

  first_task() >> second_task() >> third_task()

schedule_dag()