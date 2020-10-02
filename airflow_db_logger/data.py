from sqlalchemy import Column, Integer, String, Text, Index, DateTime, MetaData
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from airflow.utils.sqlalchemy import UtcDateTime
from airflow_db_logger.config import DB_LOGGER_SQL_ALCHEMY_SCHEMA, DB_LOGGER_CREATE_INDEXES

LoggerModelBase = declarative_base(
    metadata=(
        None
        if not DB_LOGGER_SQL_ALCHEMY_SCHEMA or DB_LOGGER_SQL_ALCHEMY_SCHEMA.isspace()
        else MetaData(schema=DB_LOGGER_SQL_ALCHEMY_SCHEMA)
    )
)


def create_table_indexes(*args):
    if not DB_LOGGER_CREATE_INDEXES:
        return []
    return args


class TaskExecutionLogRecord(LoggerModelBase):
    # FIXME: Not the base name for this table,
    # but since log was taken, it will have to do.
    # NOTE: This class is very similar to airflow.models.log,
    # but differs in purpose. Since we want
    # indexing to allow for fast log retrival, airflow.models.log
    # was not used.
    __tablename__ = "log_task_execution"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    dag_id = Column(String)
    task_id = Column(String)
    execution_date = Column(UtcDateTime)
    try_number = Column(Integer)
    text = Column(Text)

    def __init__(
        self,
        dag_id: str,
        task_id: str,
        execution_date: datetime,
        try_number: int,
        text: str,
    ):
        super().__init__()
        self.timestamp = datetime.now()

        self.dag_id = dag_id
        self.task_id = task_id
        self.execution_date = execution_date
        self.try_number = try_number
        self.text = text


class DagFileProcessingLogRecord(LoggerModelBase):
    # NOTE: This class is very similar to airflow.models.log,
    # but differs in purpose. Since we want
    # indexing to allow for fast log retrival, airflow.models.log
    # was not used.
    __tablename__ = "log_dag_processing"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    dag_filename = Column(String)
    text = Column(Text)

    def __init__(
        self,
        dag_filename: str,
        text: str,
    ):
        super().__init__()

        self.timestamp = datetime.now()

        self.dag_filename = dag_filename
        self.text = text


if DB_LOGGER_CREATE_INDEXES:
    Index("task_execution_log_record_timestamp_idx", TaskExecutionLogRecord.timestamp)
    Index("task_execution_log_record_dag_id_idx", TaskExecutionLogRecord.dag_id)
    Index("task_execution_log_record_task_id_idx", TaskExecutionLogRecord.task_id)
    Index("task_execution_log_record_execution_date_idx", TaskExecutionLogRecord.execution_date)
    Index("task_execution_log_record_try_number_idx", TaskExecutionLogRecord.try_number)

    Index("dag_file_processing_log_record_timestamp_idx", DagFileProcessingLogRecord.timestamp)
    Index("dag_file_processing_log_record_dag_filename_idx", DagFileProcessingLogRecord.dag_filename)
