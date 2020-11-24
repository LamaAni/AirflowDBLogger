import logging
import airflow
from datetime import datetime
from airflow import settings
from airflow.operators.python_operator import PythonOperator
from sqlalchemy.orm import Session, Query

from airflow_db_logger.config import DBLoggerSession
from airflow_db_logger.data import TaskExecutionLogRecord, DagFileProcessingLogRecord, LoggerModelBase


def create_clean_old_logs_task(
    task_id: str,
    before: datetime,
    after: datetime = None,
    dag: airflow.DAG = None,
):
    dag: airflow.DAG = dag or settings.CONTEXT_MANAGER_DAG
    assert isinstance(dag, airflow.DAG), ValueError("dag is none and not using 'with dag as ..' pattern")
    assert task_id and isinstance(task_id, str), ValueError("Task id must be a non empty string")

    after = after or datetime(year=2000, month=1, day=1)

    def create_select_query(session: Session, cls: LoggerModelBase) -> Query:
        query: Query = session.query(cls)
        query = query.filter(cls.timestamp <= before)
        query = query.filter(cls.timestamp > after)
        return query

    def clean_airflow_logs(*args, **kwargs):
        logging.info(f"Cleaning up logs from {after} to {before}")
        session: Session = DBLoggerSession()
        task_logs = create_select_query(session, TaskExecutionLogRecord)
        processor_logs = create_select_query(session, DagFileProcessingLogRecord)

        task_logs.delete()
        processor_logs.delete()

        session.commit()

    return PythonOperator(
        task_id=task_id,
        python_callable=clean_airflow_logs,
    )
