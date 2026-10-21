from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
  dag_id = "incremental_load",
  start_date = datetime(2026, 10, 16, tz = "Asia/Ho_Chi_Minh"),
  end_date = datetime(2026, 10, 20, tz = "Asia/Ho_Chi_Minh"),
  schedule = CronDataIntervalTimetable("@daily", timezone = "Asia/Ho_Chi_Minh"),
  catchup = True,          # backfill data 
)

def incremental_load():

  @task.python
  def incremental_data_fetch(**kwargs):
    date_interval_start = kwargs['data_interval_start']
    date_interval_end = kwargs['data_interval_end']
    print(f"Fetching data from {date_interval_start} to {date_interval_end}")

  #! Jinja template
  @task.bash
  def incremental_data_process():
    return "echo 'Processing incremental data from {{ data_interval_start }} to {{ data_interval_end }}'"

  fetch_task = incremental_data_fetch()
  process_task = incremental_data_process()

  fetch_task >> process_task

incremental_load()