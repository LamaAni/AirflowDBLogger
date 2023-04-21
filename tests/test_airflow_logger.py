import logging
import pytest

from airflow import DAG  # noqa F403


task_logger = logging.getLogger("airflow.task")
process_logger = logging.getLogger("airflow.processor")
flask_logger = logging.getLogger("flask_appbuilder")


def test_airflow_task_logger():
    task_logger.info("Test")


def test_airflow_process_logger():
    process_logger.info("Test")


def test_airflow_flask_logger():
    flask_logger.info("Test")


if __name__ == "__main__":
    test_airflow_process_logger()
    # pytest.main(["-v", "-x", __file__])
