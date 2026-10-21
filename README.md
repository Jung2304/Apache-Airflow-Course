# Apache Airflow Course

<p align="center">
  <img src="https://img.shields.io/badge/Apache_Airflow-3.1.6-017CEE?logo=apacheairflow&logoColor=white" alt="Apache Airflow 3.1.6" />
  <img src="https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white" alt="Python 3.11+" />
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white" alt="Docker Compose" />
  <img src="https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white" alt="PostgreSQL 16" />
</p>

A hands-on learning repository documenting my progress with **Apache Airflow 3** for **Data Engineering workflow orchestration**. The exercises progress from basic DAGs and TaskFlow tasks to XComs, branching, advanced scheduling, data-aware Assets, and cross-DAG orchestration.

## What I Learned

| Area | Concepts and practical skills |
| --- | --- |
| **Airflow fundamentals** | DAGs, tasks, dependencies, DAG runs, task instances, scheduling, and DAG versioning |
| **Architecture & deployment** | Airflow 3 services, CeleryExecutor, PostgreSQL, Redis, Docker Compose, bind mounts, and the Triggerer |
| **TaskFlow API** | `@dag`, `@task.python`, `@task.bash`, and the `>>` dependency operator |
| **Data sharing** | Automatic TaskFlow XComs, manual `xcom_push()` / `xcom_pull()`, and the `return_value` key |
| **Workflow control** | Sequential execution, parallel transformation branches, and `@task.branch` |
| **Scheduling** | Presets, cron expressions, timedelta-based and event-date schedules, time zones, `start_date`, `end_date`, and `catchup` |
| **Data-aware scheduling** | Asset producers and consumers using `@asset`, `uri`, and asset-triggered execution |
| **Cross-DAG orchestration** | `TriggerDagRunOperator`, `wait_for_completion`, and `deferrable=True` |

## Local Architecture

The project uses **Apache Airflow 3.1.6** with **CeleryExecutor**, deployed through Docker Compose:

```text
            Local DAG files (./dags)
                       |
                 Bind mount
                       |
                DAG Processor
                       |
                    Scheduler ---------------- PostgreSQL
                       |                      (metadata DB)
                  Redis broker
                       |
                  Celery Worker(s)
                       |
                 Task execution

  API Server / Airflow UI            Triggerer
  (localhost:8081)                  (deferred tasks)
```

- **Scheduler:** Determines when DAG runs and tasks are ready.
- **DAG Processor:** Parses Python DAG definitions.
- **Celery Workers:** Execute tasks dispatched through the Redis broker.
- **PostgreSQL:** Stores Airflow metadata and task state.
- **API Server:** Serves the API and web interface.
- **Triggerer:** Handles asynchronous waits for deferrable tasks.

The Compose configuration mounts `dags/`, `logs/`, `config/`, and `plugins/` into Airflow containers, allowing code changes and logs to be shared between the host and containers.

## Repository Structure

```text
Apache-Airflow-Course/
├── dags/                   # Hands-on Airflow DAG exercises
├── src/airflow_course/     # Local Python package scaffold
├── docker-compose.yaml     # Airflow services and volumes
├── pyproject.toml          # Python project configuration
├── uv.lock                 # Locked local Python dependencies
└── .python-version         # Local Python version
```

## Hands-on Exercises

| Exercise | File | Key idea |
| --- | --- | --- |
| **01. Linear DAG** | [`1_linear_dag.py`](dags/1_linear_dag.py) | Three Python tasks in sequential order with `>>`. |
| **02. DAG Versioning** | [`2_dag_versioning.py`](dags/2_dag_versioning.py) | Explore how definition changes create new DAG versions. |
| **03. Operators** | [`3_operators.py`](dags/3_operators.py) | Combine Bash and Python tasks with TaskFlow decorators. |
| **04. Automatic XCom** | [`4_XCOMs_auto.py`](dags/4_XCOMs_auto.py) | Pass Python return values as task arguments; dependencies are inferred. |
| **05. Manual XCom** | [`5_XCOMs_manual.py`](dags/5_XCOMs_manual.py) | Push and pull values through the task-instance context. |
| **06. Parallel Tasks** | [`6_parallel_tasks.py`](dags/6_parallel_tasks.py) | Fan out to API-, database-, and S3-labeled simulation branches, then fan in to a load task. |
| **07. Branching** | [`7_branches.py`](dags/7_branches.py) | Choose a load or no-load path with `@task.branch` and an example flag. |
| **08. Schedule Presets** | [`8_schedule_preset.py`](dags/8_schedule_preset.py) | Daily scheduling, time-zone-aware dates, `catchup=False`, and automatic unpausing. |
| **09. Cron Scheduling** | [`9_schedule_cron.py`](dags/9_schedule_cron.py) | Weekday 17:00 schedule using `CronDataIntervalTimetable`. |
| **10. Delta Scheduling** | [`10_schedule_delta.py`](dags/10_schedule_delta.py) | Three-day intervals using `DeltaTriggerTimetable`. |
| **11. Incremental Windows** | [`11_incremental_load.py`](dags/11_incremental_load.py) | Use `data_interval_start` / `data_interval_end`, `catchup=True`, and Jinja templates. |
| **12. Special Dates** | [`12_special_dates.py`](dags/12_special_dates.py) | Run on explicitly listed dates with `EventsTimetable`. |
| **13. Asset Producer** | [`assets_13.py`](dags/assets_13.py) | Daily `@asset` function writes an example text file. |
| **14. Asset Consumer** | [`14_assets_dependent.py`](dags/14_assets_dependent.py) | Run after an upstream asset update, read the source file, and write a processed file. |
| **15. DAG Orchestration** | [`dag_orchestrate_parent.py`](dags/dag_orchestrate_parent.py) | Trigger two child DAGs in order and wait efficiently for completion. |

The orchestration example also uses [`dag_orchestrate_1.py`](dags/dag_orchestrate_1.py) and [`dag_orchestrate_2.py`](dags/dag_orchestrate_2.py) as child DAGs.

## Workflow Highlights

### 1. Parallel Processing and Branching

[`6_parallel_tasks.py`](dags/6_parallel_tasks.py) demonstrates the **fan-out / fan-in** pattern:

```text
                       +--> Transform API data --+
                       |                          |
Extract simulated data +--> Transform DB data ---+--> Load / print results
                       |                          |
                       +--> Transform S3 data ---+
```

[`7_branches.py`](dags/7_branches.py) builds on this flow by adding a decision task. Based on a simulated `weekend_flag`, Airflow selects either `load_task` or `no_load_task` and skips the unselected branch.

### 2. Automatic and Manual XComs

With the **TaskFlow API**, a task can return a small object and pass the resulting XCom reference directly to the next task:

```python
extracted = first_task()
transformed = second_task(extracted)
third_task(transformed)
```

The manual approach uses `ti.xcom_push(key=..., value=...)` and `ti.xcom_pull(task_ids=..., key=...)`. XCom is useful for **small pieces of task metadata**, not for transferring large datasets.

### 3. Scheduling and Data Intervals

The scheduling exercises compare different ways to control execution:

- **Presets:** `schedule="@daily"` for a regular daily cadence.
- **Cron:** `CronDataIntervalTimetable("0 17 * * 1-5", timezone="Asia/Ho_Chi_Minh")` for 17:00 on weekdays.
- **Time delta:** `DeltaTriggerTimetable(duration(days=3))` for a three-day trigger cadence.
- **Event dates:** `EventsTimetable` for specific calendar dates.
- **Historical runs:** `catchup=True` to schedule eligible past intervals; `catchup=False` to avoid automatic historical catchup.

### 4. Asset-Driven Pipelines

The asset examples show how **data-aware scheduling** differs from scheduling by time:

```text
@asset extract_data                         @asset process_data
Daily producer                              Asset-triggered consumer
        |                                             ^
        v                                             |
/opt/airflow/logs/data/data_extract.txt -- update event
                                                      |
                                                      v
                                 data_processed.txt (reads upstream file)
```

`assets_13.py` emits an update event after successful execution. `14_assets_dependent.py` uses `schedule=extract_data` so the processing asset runs in response. The asset event is a **scheduling signal**; Airflow does not automatically transfer the file contents or watch local file modifications.

### 5. Parent / Child DAG Orchestration

The parent DAG coordinates the two child DAGs using `TriggerDagRunOperator`:

```text
Parent DAG
   |
   +--> Trigger first_orchestrator_dag
   |           |
   |        Wait for success
   |           |
   +--> Trigger second_orchestrator_dag
               |
            Wait for success
               |
          Parent completes
```

`wait_for_completion=True` preserves the order. `deferrable=True` allows a waiting task to release its worker slot while the **Triggerer** watches for completion.

## Lessons and Takeaways

- **Dependencies vs. data:** `>>` controls task order; XCom passes small values; Assets signal data updates across workflows.
- **Scheduling vs. ingestion:** Knowing a time interval does not by itself create an incremental-load pipeline; the extraction must actually filter records and write them safely.
- **Efficient orchestration:** Parallel tasks can shorten independent processing stages, while `deferrable=True` operators avoid holding worker slots during long waits.
- **Local development:** Docker bind mounts, logs, service health checks, and port mappings are fundamental for debugging Airflow.

## References

- [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Asset-Aware Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)
- [Running Airflow in Docker Compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

