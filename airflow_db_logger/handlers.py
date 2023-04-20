import logging
import traceback
import warnings
import os
import sys
from typing import Dict, List, Union
from zthreading.events import EventHandler, Event
from airflow.utils.helpers import parse_template_string
from airflow.models import TaskInstance
from airflow.utils.context import AirflowContextDeprecationWarning
from sqlalchemy import asc, desc

from airflow_db_logger.utils import get_calling_frame_objects_by_type
from airflow_db_logger.data import TaskExecutionLogRecord, DagFileProcessingLogRecord
from airflow_db_logger.config import (
    LOG_LEVEL,
    DBLoggerSession,
    DAGS_FOLDER,
    IS_RUNNING_DEBUG_EXECUTOR,
    DB_LOGGER_SHOW_REVERSE_ORDER,
    TASK_LOG_FILENAME_TEMPLATE,
    PROCESS_LOG_FILENAME_TEMPLATE,
    DB_LOGGER_WRITE_TO_FILES,
    DB_LOGGER_WRITE_TO_GCS_BUCKET,
    DB_LOGGER_WRITE_TO_SHELL,
    AIRFLOW_MAJOR_VERSION,
    airflow_db_logger_log,
)


class ExecutionLogTaskContextInfo:
    def __init__(self, task_instance: TaskInstance):
        super().__init__()
        self.dag_id = task_instance.dag_id
        self.task_id = task_instance.task_id
        self.execution_date = task_instance.execution_date
        self.try_number = task_instance.try_number
        self.map_index = 0


class DBLoggingEventHandler(EventHandler):
    log_event_name: str = "log"
    flush_event_name: str = "flush"
    close_event_name: str = "close"


class StreamHandler(logging.StreamHandler):
    def __init__(self, stream: str = None, level: str = None) -> None:
        stream = stream or "stdout"
        self._use_stderr = "stderr" in stream
        logging.Handler.__init__(self, level=level or LOG_LEVEL)

    @property
    def stream(self):
        if self._use_stderr:
            return sys.__stderr__

        return sys.__stdout__


class DBLogStreamWriter(DBLoggingEventHandler):
    _stream_writers: List["DBLogStreamWriter"] = None

    def __init__(self, on_event=None) -> None:
        super().__init__(on_event=on_event)

    def emit_event(self, event: Event):
        """Override to handle logging events.

        Args:
            event (Event): The event object
        """
        if event.name == self.log_event_name:
            # assert isinstance(event.sender, DBLogHandler)
            self.write(handler=event.sender, record=event.args[0])

    def write(self, handler: "DBLogHandler", record: logging.LogRecord):
        """Abstract method. Writes a new log line.

        Args:
            handler (DBLogHandler): The log handler.
            record (any): The log record.

        Raises:
            NotImplementedError: [description]
        """
        raise NotImplementedError("This method is abstract, and should be overridden")

    @classmethod
    def _initialize_global_stream_writers(cls):
        cls._stream_writers = []
        if DB_LOGGER_WRITE_TO_FILES is not False:
            from airflow_db_logger.writers.local import DBLogFileWriter

            cls._stream_writers.append(DBLogFileWriter())
        if DB_LOGGER_WRITE_TO_GCS_BUCKET is not None:
            from airflow_db_logger.writers.gcs import GCSFileWriter

            cls._stream_writers.append(GCSFileWriter())

        if DB_LOGGER_WRITE_TO_SHELL is True:
            from airflow_db_logger.writers.shell import DBLogShellWriter

            cls._stream_writers.append(DBLogShellWriter())

    @classmethod
    def get_global_stream_writers(cls) -> List["DBLogStreamWriter"]:
        if cls._stream_writers is None:
            cls._initialize_global_stream_writers()
        return cls._stream_writers


class DBLogHandler(logging.Handler, DBLoggingEventHandler):
    def __init__(
        self,
        level: Union[str, int] = None,
        base_log_folder: str = None,
        filename_template: str = None,  # This will be ignored (Deprecated)
        filename_jinja_template: str = "global.log",
    ):
        logging.Handler.__init__(self, level=level or LOG_LEVEL)
        EventHandler.__init__(self)
        self.base_log_folder = base_log_folder
        self.filename_template = filename_template
        self._logfile_subpath = None

        with warnings.catch_warnings():
            warnings.filterwarnings(action="ignore", category=AirflowContextDeprecationWarning)
            _, self.filename_jinja_template = parse_template_string(filename_jinja_template)

        self._db_session: DBLoggerSession = None
        for writer in DBLogStreamWriter.get_global_stream_writers():
            self.pipe(writer)

    @property
    def has_context(self):
        return self._db_session is not None

    @property
    def db_session(self) -> DBLoggerSession:
        return self._db_session

    @property
    def logfile_subpath(self):
        """Returns the task log filename"""
        if self._logfile_subpath is None:
            self._logfile_subpath = self.render_logfile_subpath()
        return self._logfile_subpath

    def render_logfile_subpath(self, **kwargs) -> str:
        try:
            return self.filename_jinja_template.render(**kwargs)
        except Exception as ex:
            airflow_db_logger_log.error(ex)
        return "err-filename-template.log"

    def set_context(self):
        """Initialize the db log configuration.

        Arguments:
            task_instance {task instance object} -- The task instace to write for.

        Extra args will be added to the context.
        """
        if self._db_session is None:
            self._db_session = DBLoggerSession()

    def emit_handler_event(self, name: str, *args, **kwargs):
        """Emits an event. Any arguments sent after name, will
        be passed to the event action.

        Arguments:
            name {str} -- The name of the event to emit.
        """
        EventHandler.emit(self, name, *args, **kwargs)

    def emit(self, record: logging.LogRecord):
        """Emits a log record. Override this method to provide record handling.

        Arguments:
            record {any} -- The logging record.
        """
        self.emit_handler_event(self.log_event_name, record)

    def flush(self):
        """Waits for any unwritten logs to write to the db."""
        self.emit_handler_event(self.flush_event_name)
        if not self.has_context:
            return
        if self.db_session is not None:
            self.db_session.flush()

    def close(self):
        self.emit_handler_event(self.close_event_name)
        if not self.has_context:
            return
        """Stops and finalizes the log writing.
        """
        if self.db_session is not None:
            self.db_session.close()
            self._db_session = None


class DBTaskLogHandler(DBLogHandler):
    """
    DB Task log handler writes and reads task logs from the logging database
    (Defaults to the airflow database, unless otherwise defined)
    """

    subfolder_path: str = "tasks"

    def __init__(
        self,
        base_log_folder: str = None,
        filename_template: str = None,
        level: Union[str, int] = None,
    ):
        super().__init__(
            filename_template=filename_template,
            base_log_folder=base_log_folder,
            level=level,
            filename_jinja_template=TASK_LOG_FILENAME_TEMPLATE,
        )
        self._task_context_info: ExecutionLogTaskContextInfo = None
        self._task_instance: TaskInstance = None
        self._logfile_subpath: str = None

    @property
    def task_context_info(self) -> ExecutionLogTaskContextInfo:
        assert self._task_context_info is not None, "Task instance was not defined while attempting to write task log"
        return self._task_context_info

    @property
    def has_task_context(self):
        return self._task_context_info is not None

    def render_logfile_subpath(self, **kwargs) -> str:
        kwargs.update(
            dict(
                ti=self.task_context_info,
                dag_id=self.task_context_info.dag_id,
                task_id=self.task_context_info.task_id,
                execution_date=self.task_context_info.execution_date.isoformat(),
                try_number=self.task_context_info.try_number,
            )
        )
        return super().render_logfile_subpath(**kwargs)

    def set_context(self, task_instance):
        """Initialize the db log configuration.

        Arguments:
            task_instance {task instance object} -- The task instace to write for.
        """

        try:
            self._task_instance = task_instance
            self._task_context_info = ExecutionLogTaskContextInfo(task_instance)
            self._logfile_subpath = os.path.join(self.subfolder_path, self.logfile_subpath)
            super().set_context()
        except Exception as err:
            airflow_db_logger_log.error(err)

    def emit(self, record: logging.LogRecord):
        """Emits a log record.

        Arguments:
            record {any} -- The logging record.
        """

        # A fix to allow the debug executor to run also to the database.
        if IS_RUNNING_DEBUG_EXECUTOR and not self.has_task_context:
            ti: TaskInstance = get_calling_frame_objects_by_type(TaskInstance, first_only=True)
            if ti is not None:
                self.set_context(task_instance=ti)

        if self.has_task_context and self.has_context:
            try:
                db_record = TaskExecutionLogRecord(
                    self.task_context_info.dag_id,
                    self.task_context_info.task_id,
                    self.task_context_info.execution_date,
                    self.task_context_info.try_number,
                    self.format(record),
                )

                self.db_session.add(db_record)
                self.db_session.commit()
            except Exception:
                try:
                    self.db_session.rollback()
                except Exception:
                    pass
                airflow_db_logger_log.error(traceback.format_exc())

        super().emit(record)

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

            airflow_db_logger_log.info(
                (
                    f"Reading logs: {task_instance.dag_id}/{task_instance.task_id} {try_numbers}"
                    f" {{{task_instance.execution_date}}}"
                )
            )

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

            for log_record in log_records:
                try_number = int(log_record.try_number)
                if try_number not in logs_by_try_number:
                    logs_by_try_number[try_number] = []
                logs_by_try_number[try_number].append(str(log_record.text))

            for try_number in logs_by_try_number.keys():
                logs_by_try_number[try_number] = str("\n".join(logs_by_try_number[try_number]))

            try_numbers.sort()
            logs = []
            metadata_array = []
            for try_number in try_numbers:
                # logs.appen
                log = logs_by_try_number.get(try_number, "[No logs found]")
                if AIRFLOW_MAJOR_VERSION > 1:
                    log = [(task_instance.hostname, log)]
                logs.append(log)
                metadata_array.append({"end_of_log": True})

            # airflow_db_logger_log.info(traceback.format_stack().j)
            # traceback.print_stack()
            airflow_db_logger_log.info(f"Read {len(logs)} logs")

            return logs, metadata_array

        except Exception:
            if db_session:
                try:
                    db_session.rollback()
                except Exception:
                    pass
            airflow_db_logger_log.error(traceback.format_exc())
            return [f"An error occurred while connecting to the database:\n{traceback.format_exc()}"], [
                {"end_of_log": True}
            ]
        finally:
            if db_session:
                db_session.close()


class DBProcessLogHandler(DBLogHandler):
    """
    FileProcessorHandler is a python log handler that handles
    dag processor logs. It creates and delegates log handling
    to a database
    """

    dags_subfolder_path: str = "dags"
    global_log_file: str = "global.log"
    subfolder_path: str = "process"

    def __init__(
        self,
        base_log_folder: str = None,
        filename_template: str = None,
        level: Union[str, int] = None,
    ):
        super().__init__(
            filename_template=filename_template,
            base_log_folder=base_log_folder,
            level=level,
            filename_jinja_template=PROCESS_LOG_FILENAME_TEMPLATE,
        )
        self.dag_dir: str = os.path.expanduser(DAGS_FOLDER)

    @property
    def dag_relative_filepath(self) -> str:
        """Returns the relative path of the dag filename, to the dags directory."""
        return self._log_file_subpath

    def _render_relative_dag_filepath(self, filename: str):
        """Renders a dag log file relative to the dag filepath.

        Arguments:
            filename {str} -- The original filename

        Returns:
            str -- The display filename.
        """
        filename = os.path.relpath(filename, self.dag_dir)
        return f"{filename}.log"

    def set_context(self, filepath=None):
        """Initialize the dag log configuration.

        Arguments:
            dag_filepath {str} -- The path to the dag file.
        """

        try:
            if filepath:
                self._log_file_subpath = os.path.join(
                    self.subfolder_path,
                    self.dags_subfolder_path,
                    self._render_relative_dag_filepath(filepath),
                )
            self._db_session = DBLoggerSession()
        except Exception:
            airflow_db_logger_log.error("Failed to initialize process logger contexts")
            airflow_db_logger_log.error(traceback.format_exc())

    def emit(self, record: logging.LogRecord):
        """Emits a log record.

        Arguments:
            record {any} -- The logging record.
        """
        if not self.has_context:
            self.set_context()

        db_record_message = self.format(record)
        try:
            db_record = DagFileProcessingLogRecord(self._log_file_subpath, db_record_message)
            self.db_session.add(db_record)
            self.db_session.commit()
        except Exception:
            try:
                self.db_session.rollback()
            except Exception:
                pass
            airflow_db_logger_log.error(
                f"Error while attempting to log ({self._log_file_subpath}): {db_record_message}"
            )
            airflow_db_logger_log.error(traceback.format_exc())

        super().emit(record)
