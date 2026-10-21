from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
  dag_id = "delta_schedule_dag",
  start_date = datetime(year=2026, month=10, day=20, tz="Asia/Ho_Chi_Minh"),
  end_date = datetime(year=2026, month=10, day=25, tz="Asia/Ho_Chi_Minh"),
  schedule = DeltaTriggerTimetable(duration(days=3)),      # run every 3 days   
  catchup = False,                       # No automatic backfill
  is_paused_upon_creation = False,       # new DAG is ready for automatic scheduling without manually enabling it
)

def delta_schedule_dag():

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

delta_schedule_dag()