from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {"owner": "tester", "start_date": "1/1/2020", "retries": 0}

dag = DAG(
    "db-log-tester",
    default_args=default_args,
    description="Test base airflow db logger",
    schedule_interval=None,
    catchup=False,
)

namespace = None

envs = {
    "PASS_ARG": "a test",
}

bash_command = """
echo "This is a bash command. Sleeping 1"
sleep 1
echo "Done"
"""

with dag:
    BashOperator(task_id="test-bash-success", bash_command=bash_command)
    # BashOperator(task_id="test-bash-fail", bash_command=bash_command + "\nexit 22")

    def run_python(do_success=True):
        if not do_success:
            raise Exception("Some kinda error")

    PythonOperator(task_id="test-python-success", python_callable=lambda: run_python(True))
    # PythonOperator(task_id="test-python-fail", python_callable=lambda: run_python(False))


if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
