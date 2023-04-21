from typing import Dict, List
from sqlalchemy import asc, desc
from sqlalchemy.orm import Session, Query

from airflow.models import TaskInstance

from airflow_db_logger.data import DBLoggerLogRecord, DBLoggerLogCategory
from airflow_db_logger.log import airflow_db_logger_log
from airflow_db_logger.db import db_logger_session
from airflow_db_logger.config import (
    DB_LOGGER_SHOW_REVERSE_ORDER,
    AIRFLOW_MAJOR_VERSION,
    DB_LOGGER_SHOW_LOG_SPLASH,
    DB_LOGGER_LOG_SPLASH,
)


class ExecutingLogContext:
    def __init__(
        self,
        task_instance: TaskInstance = None,
        dag_filename: str = None,
        try_number: int = None,
        hostname: str = None,
    ):
        super().__init__()
        self.category = DBLoggerLogCategory.other
        self.hostname = hostname
        self.dag_id = None
        self.task_id = None
        self.execution_date = None
        self.map_index = None
        self.category = None
        self.try_number = None
        self.next_try_number = None
        self.hostname = None
        self.dag_filename = None

        if task_instance:
            self.category = DBLoggerLogCategory.task
            self.dag_id = str(task_instance.dag_id)
            self.task_id = str(task_instance.task_id)
            self.execution_date = task_instance.execution_date
            self.map_index = 0
            self.try_number = try_number if try_number is not None else task_instance.try_number
            self.next_try_number = task_instance.next_try_number
            self.hostname = str(task_instance.hostname) if task_instance.hostname else None

        if dag_filename:
            self.dag_filename = dag_filename
            self.category = DBLoggerLogCategory.dag

    def get_try_numbers(self):
        # Task instance increments its try number when it starts to run.
        # So the log for a particular task try will only show up when
        # try number gets incremented in DB, i.e logs produced the time
        # after cli run and before try_number + 1 in DB will not be displayed.
        try_numbers = []
        if self.try_number is None:
            try_numbers = list(range(1, self.next_try_number))
        else:
            try_numbers = [self.try_number]
        return try_numbers

    def create_record(self, msg: str):
        return DBLoggerLogRecord(
            category=self.category,
            dag_id=self.dag_id,
            task_id=self.task_id,
            dag_filename=self.dag_filename,
            execution_date=self.execution_date,
            try_number=self.try_number,
            text=msg,
        )

    def __str__(self) -> str:
        parts = [self.category.name]
        if self.category == DBLoggerLogCategory.task:
            parts += [f"dag_id={self.dag_id}", f"task_id={self.task_id}"]
        if self.category == DBLoggerLogCategory.dag:
            parts += [f"{self.dag_filename}"]
        return ", ".join(parts)

    def __repr__(self) -> str:
        return str(self)


def write_log(
    msg: str,
    context: ExecutingLogContext = None,
    db_session: Session = None,
):
    """Write a log line for a either a task, a dag or in general.

    Args:
        msg (str): The message to write.
        context (ExecutingLogContext, optional): The execution context. Defaults to None.
        db_session (DBLoggerSession, optional): The database session. Defaults to None.
    """
    context = context or ExecutingLogContext()
    record = context.create_record(msg)
    session_created = False
    if db_session is None:
        db_session: Session = db_logger_session()
        session_created = True

    try:
        db_session.add(record)
        db_session.commit()
    except Exception as ex:
        try:
            db_session.rollback()
        except Exception:
            pass
        raise ex
    finally:
        if session_created:
            db_session.close()


def read_log_records(
    context: ExecutingLogContext,
    db_session: Session = None,
    limit: int = 10000,
    reverse: bool = DB_LOGGER_SHOW_REVERSE_ORDER,
    try_numbers: List[int] = None,
):
    session_created = False
    if db_session is None:
        db_session: Session = db_logger_session()
        session_created = True

    try:
        log_records_query: Query = db_session.query(DBLoggerLogRecord)
        log_records_query = log_records_query.filter(DBLoggerLogRecord.category == context.category)

        if context.category == DBLoggerLogCategory.task:
            log_records_query = (
                log_records_query.filter(DBLoggerLogRecord.dag_id == context.dag_id)
                .filter(DBLoggerLogRecord.task_id == context.task_id)
                .filter(DBLoggerLogRecord.execution_date == context.execution_date)
                .filter(DBLoggerLogRecord.try_number.in_(context.get_try_numbers()))
            )
        elif context.category == DBLoggerLogCategory.dag:
            log_records_query = log_records_query.filter(DBLoggerLogRecord.dag_filename == context.dag_filename)

        if reverse:
            log_records_query = log_records_query.order_by(desc(DBLoggerLogRecord.timestamp))
        else:
            log_records_query = log_records_query.order_by(asc(DBLoggerLogRecord.timestamp))

        log_records_query = log_records_query.limit(limit)

        # Reading records
        log_records: List[DBLoggerLogRecord] = list(log_records_query.all())
    finally:
        if session_created:
            db_session.close()

    return log_records


def read_airflow_logs(
    context: ExecutingLogContext,
    db_session: Session = None,
    limit: int = 10000,
    reverse: bool = DB_LOGGER_SHOW_REVERSE_ORDER,
    raise_exception: bool = True,
):
    try_numbers = context.get_try_numbers()
    try_numbers.sort()
    logs = []
    metadata_array = []

    airflow_db_logger_log.debug(f"{context}: Reading logs")

    try:
        log_records = read_log_records(
            context=context,
            db_session=db_session,
            limit=limit,
            reverse=reverse,
            try_numbers=try_numbers,
        )

        has_reached_limit = len(log_records) >= limit

        # Mapping records to airflow log
        records_by_try_number: Dict[int, List[DBLoggerLogRecord]] = dict()

        for log_record in log_records:
            try_number = int(log_record.try_number)
            if try_number not in records_by_try_number:
                records_by_try_number[try_number] = []
            records_by_try_number[try_number].append(log_record)

        # parsing the log
        logs_by_try_number: Dict[int, str] = dict()
        for try_number, record_list in records_by_try_number.items():
            lines = []
            if DB_LOGGER_SHOW_LOG_SPLASH:
                lines.append(str(DB_LOGGER_LOG_SPLASH))
            lines += map(lambda v: str(v.text), record_list)
            log_text = "\n".join(lines)

            if has_reached_limit:
                log_text += f"\n\n---- reached record read limit ({limit}) ----"
            logs_by_try_number[try_number] = log_text

        for try_number in try_numbers:
            # logs.appen
            log_text = logs_by_try_number.get(try_number, "[No logs found]")
            if AIRFLOW_MAJOR_VERSION > 1:
                log_text = [(context.hostname or "[Unknown host]", log_text)]
            logs.append(log_text)
            metadata_array.append({"end_of_log": True})

        airflow_db_logger_log.info(f"{context}: Read {len(log_records)} log records")

    except Exception as ex:
        if raise_exception:
            raise ex
        for try_number in try_numbers:
            log_text = """
ERROR WHILE READING LOGS FROM DB!
Error details will appear in airflow service logs.
            """
            if AIRFLOW_MAJOR_VERSION > 1:
                log_text = [(context.hostname or "[Unknown host]", log_text)]
            logs.append(log_text)
            metadata_array.append({"end_of_log": True})

    return logs, metadata_array
