from airflow.sdk import dag, task
from dag_orchestrate_1 import first_orchestrator_dag
from dag_orchestrate_2 import second_orchestrator_dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
  dag_id = "dag_orchestrate_parent",
  schedule = None,
  catchup = False
)
def dag_orchestrate_parent():
  
  trigger_first_dag = TriggerDagRunOperator(
    task_id = "trigger_first_orchestrator_dag",     # Identifies the task inside the parent DAG
    trigger_dag_id = "first_orchestrator_dag",      # Identifies the child DAG to trigger
    wait_for_completion = True,
    deferrable = True         # waiting task can release its worker slot while awaiting the child DAG's completion
  )
  
  trigger_second_dag = TriggerDagRunOperator(
    task_id = "trigger_second_orchestrator_dag",
    trigger_dag_id = "second_orchestrator_dag",
    wait_for_completion = True,
    deferrable = True       # Worker is freed for other tasks while waiting.
  )

  trigger_first_dag >> trigger_second_dag

dag_orchestrate_parent()