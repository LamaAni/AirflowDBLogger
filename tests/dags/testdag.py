from time import sleep
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    "db-log-tester",
    default_args={"owner": "tester"},
    description="Test base airflow db logger",
    schedule="@once",
    catchup=False,
    start_date=datetime.now(),
)

namespace = None

envs = {
    "PASS_ARG": "a test",
}

bash_command = """
echo "Message from bash. Sleeping 1"
sleep 1
echo "Done"
"""

with dag:
    BashOperator(task_id="test-bash-success", bash_command=bash_command)
    # BashOperator(task_id="test-bash-fail", bash_command=bash_command + "\nexit 22")

    def run_python(do_success=True):
        print("Message from python. Sleeping 1")
        sleep(1)
        if not do_success:
            raise Exception("Some kinda error")

    PythonOperator(task_id="test-python-success", python_callable=lambda: run_python(True))
    # PythonOperator(task_id="test-python-fail", python_callable=lambda: run_python(False))


if __name__ == "__main__":
    dag.clear()
    dag.run(run_at_least_once=True)
