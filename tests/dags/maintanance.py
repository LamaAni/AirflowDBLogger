from airflow import DAG
from datetime import datetime
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator
from airflow_db_logger.operators import AirflowDBLoggerCleanupOperator

default_args = {"owner": "tester", "start_date": "1/1/2020", "retries": 0}

dag = DAG(
    "db-log-cleanup",
    default_args=default_args,
    description="Test base airflow db logger",
    schedule_interval=None,
    catchup=False,
)

namespace = None

envs = {
    "PASS_ARG": "a test",
}


def print_multi(*args, **kwargs):
    for i in range(100):
        print(i)


with dag:
    AirflowDBLoggerCleanupOperator(
        task_id="db_log_cleanup",
        up_to=datetime.now(),
        include_operations_log=True,
        include_task_logs=True,
    )

if __name__ == "__main__":
    dag.clear()
    dag.schedule_interval = None
    dag.run()
