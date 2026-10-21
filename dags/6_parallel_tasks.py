from airflow.sdk import dag, task

@dag(
  dag_id="parallel_dags",
  schedule="@daily",
  catchup=False,        # No automatic backfill
)
def parallel_dags():

  @task.python
  def extract_task(**kwargs):
    ti = kwargs['ti']
    print("Extracting data...")

    extracted_data_dict = {
      "api_extracted_data": [1,2,3],
      "db_extracted_data": [4,5,6],
      "s3_extracted_data": [7,8,9]
    }
    ti.xcom_push(key = "return_value", value = extracted_data_dict)

  @task.python
  def transform_task_api(**kwargs):
    ti = kwargs['ti']
    print("This API task is running in parallel")
    
    api_extracted_data = ti.xcom_pull(task_ids = "extract_task", key = "return_value")["api_extracted_data"]
    transformed_api_data = [x * 10 for x in api_extracted_data]

    ti.xcom_push(key = "return_value", value = transformed_api_data)

  @task.python
  def transform_task_db(**kwargs):
    ti = kwargs['ti']
    print("This DB task is running in parallel")

    db_extracted_data = ti.xcom_pull(task_ids = "extract_task", key = "return_value")["db_extracted_data"]
    transformed_db_data = [x ** 2 for x in db_extracted_data if x % 2 == 0]

    ti.xcom_push(key = "return_value", value = transformed_db_data)

  @task.python
  def transform_task_s3(**kwargs):
    ti = kwargs['ti']
    print("This S3 task is running in parallel")

    s3_extracted_data = ti.xcom_pull(task_ids = "extract_task", key = "return_value")["s3_extracted_data"]
    transformed_s3_data = [x ** 3 for x in s3_extracted_data if x % 2 == 1]

    ti.xcom_push(key = "return_value", value = transformed_s3_data)

  # Automatic XCom Push
  @task.bash
  def load_task(**kwargs):
    print("Loading data to the destination...") 

    # Automatically saves the command's final stdout line to XCom
    api_data = kwargs['ti'].xcom_pull(task_ids = "transform_task_api", key = "return_value")
    db_data = kwargs['ti'].xcom_pull(task_ids = "transform_task_db", key = "return_value")
    s3_data = kwargs['ti'].xcom_pull(task_ids = "transform_task_s3", key = "return_value")

    return f"echo 'Loaded Data: {api_data}, {db_data}, {s3_data}'"

  # Defining task dependencies
  extracted = extract_task()
  trans_api = transform_task_api()
  trans_db = transform_task_db()
  trans_s3 = transform_task_s3()
  loaded = load_task()
  
  extracted >> [trans_api, trans_db, trans_s3] >> loaded

parallel_dags()