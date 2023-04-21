from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow_db_logger.operators import AirflowDBLoggerCleanupOperator

default_args = {"owner": "tester", "start_date": days_ago(0), "retries": 0}

dag = DAG(
    "db-log-cleanup",
    default_args=default_args,
    description="Test base airflow db logger cleanup",
    schedule_interval=None,
    catchup=False,
)

with dag:
    AirflowDBLoggerCleanupOperator(
        task_id="db_log_cleanup",
        up_to=datetime.now(),
        since=None,
    )

if __name__ == "__main__":
    dag.clear()
    dag.schedule_interval = None
    dag.run()
