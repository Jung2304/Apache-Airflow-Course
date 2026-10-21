from airflow.sdk import dag, task

@dag(
  dag_id="xcoms_auto",
  schedule="@daily",
  catchup=False,        # No automatic backfill
)
def xcoms_auto():

  @task.python
  def first_task():
    print("Extracting data...")
    fetched_data = {"data": [1,2,3,4,5]}
    return fetched_data

  @task.python
  def second_task(data: dict):
    fetch_data = data['data']
    transformed_data = [x ** 2 for x in fetch_data]
    return {"trans_data": transformed_data}        # return dictionary to better work with 

  @task.python
  def third_task(data: dict): 
    even_values = [x for x in data['trans_data'] if x % 2 == 0]
    print("Even values are:", even_values)
  
  # Automatically establishes dependencies
  extracted = first_task()
  transformed = second_task(extracted)
  third_task(transformed)

xcoms_auto()