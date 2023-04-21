import enum
from sqlalchemy import Column, Integer, String, Text, Index, DateTime, MetaData, Enum
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from airflow.utils.sqlalchemy import UtcDateTime
from airflow_db_logger.config import DB_LOGGER_SQL_ALCHEMY_SCHEMA, DB_LOGGER_SQL_ALCHEMY_CREATE_INDEXES

LoggerModelBase = declarative_base(
    metadata=(
        None
        if not DB_LOGGER_SQL_ALCHEMY_SCHEMA or DB_LOGGER_SQL_ALCHEMY_SCHEMA.isspace()
        else MetaData(schema=DB_LOGGER_SQL_ALCHEMY_SCHEMA)
    )
)


class DBLoggerLogCategory(enum.Enum):
    task = "task"  # Task execution log
    dag = "dag"  # Processing
    other = "other"  # Any other redirected airflow logs


def create_table_indexes(*args):
    if not DB_LOGGER_SQL_ALCHEMY_CREATE_INDEXES:
        return []
    return args


class DBLoggerLogRecord(LoggerModelBase):
    # FIXME: Not the base name for this table,
    # but since log was taken, it will have to do.
    # NOTE: This class is very similar to airflow.models.log,
    # but differs in purpose. Since we want
    # indexing to allow for fast log retrival, airflow.models.log
    # was not used.
    __tablename__ = "airflow_db_logger_logs"

    id = Column(Integer, primary_key=True)
    category = Column(Enum(DBLoggerLogCategory))
    timestamp = Column(DateTime)
    dag_id = Column(String)
    task_id = Column(String)
    dag_filename = Column(String)
    execution_date = Column(UtcDateTime)
    try_number = Column(Integer)
    text = Column(Text)

    def __init__(
        self,
        category: DBLoggerLogCategory,
        dag_id: str,
        task_id: str,
        dag_filename: str,
        execution_date: datetime,
        try_number: int,
        text: str,
    ):
        super().__init__()
        self.timestamp = datetime.now()

        self.category = category
        self.dag_id = dag_id
        self.dag_filename = dag_filename
        self.task_id = task_id
        self.execution_date = execution_date
        self.try_number = try_number
        self.text = text


def make_index(name: str, column: Column = None, postfix: str = "idx"):
    if column is None:
        column = getattr(DBLoggerLogRecord, name)
    Index("_".join([DBLoggerLogRecord.__tablename__, name, postfix]), column)


if DB_LOGGER_SQL_ALCHEMY_CREATE_INDEXES:
    make_index("category")
    make_index("timestamp")
    make_index("dag_id")
    make_index("task_id")
    make_index("dag_filename")
    make_index("execution_date")
    make_index("try_number")
