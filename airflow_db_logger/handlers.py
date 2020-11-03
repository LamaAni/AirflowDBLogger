import logging
import traceback
import os
from airflow.utils.helpers import parse_template_string
from airflow.models import TaskInstance
from sqlalchemy import asc, desc
from typing import Dict, List

from airflow_db_logger.data import TaskExecutionLogRecord, DagFileProcessingLogRecord
from airflow_db_logger.config import DBLoggerSession, DAGS_FOLDER, DB_LOGGER_SHOW_REVERSE_ORDER


class ExecutionLogTaskContextInfo:
    def __init__(self, task_instance: TaskInstance):
        super().__init__()
        self.dag_id = task_instance.dag_id
        self.task_id = task_instance.task_id
        self.execution_date = task_instance.execution_date
        self.try_number = task_instance.try_number


class DBTaskLogHandler(logging.Handler):
    """
    DB Task log handler writes and reads task logs from the logging database
    (Defaults to the airflow database, unless otherwise defined)
    """

    _task_context_info: TaskInstance = None
    _db_session: DBLoggerSession = None

    def __init__(self):
        super().__init__()
        self._task_context_info = None
        self._db_session = None

    @property
    def task_context_info(self):
        assert self._task_context_info is not None, "Task instance was not defined while attempting to write task log"
        return self._task_context_info

    @property
    def has_context(self):
        return self._task_context_info is not None

    @property
    def db_session(self) -> DBLoggerSession:
        return self._db_session

    def set_context(self, task_instance):
        """Initialize the db log configuration.

        Arguments:
            task_instance {task instance object} -- The task instace to write for.
        """

        try:
            self._task_context_info = ExecutionLogTaskContextInfo(task_instance)
            self._db_session = DBLoggerSession()
        except Exception as err:
            logging.error(err)

    def emit(self, record):
        """Emits a log record.

        Arguments:
            record {any} -- The logging record.
        """
        if not self.has_context:
            return

        db_record = TaskExecutionLogRecord(
            self.task_context_info.dag_id,
            self.task_context_info.task_id,
            self.task_context_info.execution_date,
            self.task_context_info.try_number,
            self.format(record),
        )

        self.db_session.add(db_record)
        self.db_session.commit()

    def flush(self):
        """Waits for any unwritten logs to write to the db."""
        if not self.has_context:
            return
        if self.db_session is not None:
            self.db_session.flush()

    def close(self):
        if not self.has_context:
            return
        """Stops and finalizes the log writing.
        """
        if self.db_session is not None:
            self.db_session.close()
            self._db_session = None

    def read(
        self,
        task_instance: TaskInstance,
        try_number: int = None,
        metadata: dict = None,
    ):
        """Read logs of given task instance from the database.

        Arguments:
            task_instance {TaskInstance} -- The task instance object

        Keyword Arguments:
            try_number {int} -- The run try number (default: {None})
            metadata {dict} -- Added metadata (default: {None})

        Raises:
            Exception: [description]

        Returns:
            List[str] -- A log array.
        """
        # Task instance increments its try number when it starts to run.
        # So the log for a particular task try will only show up when
        # try number gets incremented in DB, i.e logs produced the time
        # after cli run and before try_number + 1 in DB will not be displayed.
        db_session: DBLoggerSession = None
        try:
            db_session = DBLoggerSession()
            if try_number is None:
                next_try = task_instance.next_try_number
                try_numbers = list(range(1, next_try))
            elif try_number < 1:
                logs = [
                    "Error fetching the logs. Try number {} is invalid.".format(try_number),
                ]
                return logs
            else:
                try_numbers = [try_number]

            logs_by_try_number: Dict[int, List[TaskExecutionLogRecord]] = dict()

            log_records_query = (
                db_session.query(TaskExecutionLogRecord)
                .filter(TaskExecutionLogRecord.dag_id == task_instance.dag_id)
                .filter(TaskExecutionLogRecord.task_id == task_instance.task_id)
                .filter(TaskExecutionLogRecord.execution_date == task_instance.execution_date)
                .filter(TaskExecutionLogRecord.try_number.in_(try_numbers))
            )

            if DB_LOGGER_SHOW_REVERSE_ORDER is True:
                log_records_query = log_records_query.order_by(desc(TaskExecutionLogRecord.timestamp))
            else:
                log_records_query = log_records_query.order_by(asc(TaskExecutionLogRecord.timestamp))

            log_records = log_records_query.all()

            db_session.close()
            db_session = None

            log_record: TaskExecutionLogRecord = None

            # pull the records
            log_records: List[TaskExecutionLogRecord] = [r for r in log_records]
            # log_records.sort(key=lambda r: r.timestamp.timestamp())

            for log_record in log_records:
                try_number = int(log_record.try_number)
                if try_number not in logs_by_try_number:
                    logs_by_try_number[try_number] = []
                logs_by_try_number[try_number].append(str(log_record.text))

            for try_number in logs_by_try_number.keys():
                logs_by_try_number[try_number] = "\n".join(logs_by_try_number[try_number])

            try_numbers.sort()
            logs = []
            metadata_array = []
            for try_number in try_numbers:
                logs.append(logs_by_try_number[try_number])
                metadata_array.append({"end_of_log": True})

            return logs, metadata_array

        except Exception as err:
            logging.error("Failed to read log from db", err)
            raise err
        finally:
            if db_session:
                db_session.close()


class DBProcessLogHandler(logging.Handler):
    """
    FileProcessorHandler is a python log handler that handles
    dag processor logs. It creates and delegates log handling
    to a database
    """

    _dag_filename: str = None
    _db_session: DBLoggerSession = None

    def __init__(self, filename_template):
        """
        :param filename_template: template filename string
        """

        super().__init__()

        self.dag_dir = os.path.expanduser(DAGS_FOLDER)
        self.filename_template, self.filename_jinja_template = parse_template_string(filename_template)

        self._dag_filename = None
        self._db_session = None

    @property
    def has_context(self):
        return self._dag_filename is not None

    @property
    def db_session(self) -> DBLoggerSession:
        return self._db_session

    def _render_filename(self, filename):
        """Renders a display filename for the specific dag.

        Arguments:
            filename {str} -- The original filename

        Returns:
            str -- The display filename.
        """
        filename = os.path.relpath(filename, self.dag_dir)
        ctx = dict()
        ctx["filename"] = filename

        if self.filename_jinja_template:
            return self.filename_jinja_template.render(**ctx)

        return self.filename_template.format(filename=ctx["filename"])

    def set_context(self, dag_filename):
        """Initialize the db log configuration.

        Arguments:
            dag_filename {str} -- The dag filename context
        """

        try:
            self._dag_filename = self._render_filename(dag_filename)
            self._db_session = DBLoggerSession()
        except Exception:
            logging.error("Failed to initialize process logger contexts")
            traceback.print_exc()
            # logging.error(err)

    def emit(self, record):
        """Emits a log record.

        Arguments:
            record {any} -- The logging record.
        """
        if not self.has_context:
            return

        db_record = DagFileProcessingLogRecord(self._dag_filename, self.format(record))

        self.db_session.add(db_record)
        self.db_session.commit()

    def flush(self):
        """Waits for any unwritten logs to write to the db."""
        if not self.has_context:
            return
        if self.db_session is not None:
            self.db_session.flush()

    def close(self):
        if not self.has_context:
            return

        """Stops and finalizes the log writing.
        """
        if self.db_session is not None:
            self.db_session.close()
            self._db_session = None
