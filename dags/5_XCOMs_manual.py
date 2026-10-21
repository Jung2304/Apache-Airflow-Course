from airflow.sdk import dag, task

@dag(
  dag_id="xcoms_kwargs",
  schedule="@daily",
  catchup=False,        # No automatic backfill
)
def xcoms_kwargs():

  @task.python
  def first_task(**kwargs):

    # Extracting 'ti' from kwargs to push XCOMs manually
    ti = kwargs['ti']
    
    print("Extracting data...")
    fetched_data = {"data": [1,2,3,4,5]}
    ti.xcom_push(key = 'return_result', value = fetched_data)

  @task.python
  def second_task(**kwargs):
    ti = kwargs['ti']

    # Pulling XCOMs pushed by the first task (dictionary)
    fetched_data = ti.xcom_pull(task_ids = 'first_task', key = 'return_result')['data']
    transformed_data = [x ** 2 for x in fetched_data]

    ti.xcom_push(key = "return_result", value = {"trans_data": transformed_data})

  @task.python
  def third_task(**kwargs): 
    ti = kwargs['ti']

    # Pulling XCOMs pushed by the second task
    fetched_data = ti.xcom_pull(task_ids = 'second_task', key = 'return_result')['trans_data']
    even_values = [x for x in fetched_data if x % 2 == 0]
    print("Even values are:", even_values)


  # Not using any params but directly using XCOMs
  first_task() >> second_task() >> third_task()

xcoms_kwargs()