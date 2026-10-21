from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(
  event_dates = [
    datetime(2026,10,1),
    datetime(2026,10,15),
    datetime(2026,10,30)
  ]
)

@dag(
  dag_id = "special_dates_dag",
  start_date = datetime(2026, 10, 1, tz = "Asia/Ho_Chi_Minh"),
  end_date = datetime(2026, 10, 31, tz = "Asia/Ho_Chi_Minh"),
  schedule = special_dates, 
)

def special_dates_dag():

  @task.python
  def special_event_task(**kwargs):
    execution_date = kwargs['logical_date']
    print(f"Running task for special event on {execution_date}")

  special_event_task()

special_dates_dag()