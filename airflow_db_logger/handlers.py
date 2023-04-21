import logging
from typing import Union
from airflow.models import TaskInstance

from airflow_db_logger.utils import get_calling_frame_objects_by_type
from airflow_db_logger.storage import read_airflow_logs, write_log, ExecutingLogContext
from airflow_db_logger.log import airflow_db_logger_log
from airflow_db_logger.db import db_logger_session
from airflow_db_logger.config import (
    LOG_LEVEL,
    IS_RUNNING_DEBUG_EXECUTOR,
)


class DBLogHandler(logging.Handler):
    error_filename: str = "err-filename-template.log"

    def __init__(
        self,
        level: Union[str, int] = None,
        base_log_folder: str = None,
        filename_template: str = None,  # This will be ignored (Deprecated)
    ):
        logging.Handler.__init__(self, level=level or LOG_LEVEL)
        self.base_log_folder = base_log_folder

        self.__context: ExecutingLogContext = None
        self._db_session: db_logger_session = None

    @property
    def context(self):
        return self.__context

    @property
    def has_context(self) -> bool:
        return self.context is not None

    @property
    def has_session(self):
        return self._db_session is not None

    @property
    def db_session(self) -> db_logger_session:
        return self._db_session

    def set_context(self, context_item: Union[TaskInstance, str] = None):
        """Set the active log context

        Args:
            context_item (Union[TaskInstance, str], optional): The context object, if string its a dag
                processing context, if None the general. Defaults to None.
        """
        if not self.has_session:
            self._db_session = db_logger_session()

        filepath: str = context_item if isinstance(context_item, str) else None
        ti: TaskInstance = context_item if isinstance(context_item, TaskInstance) else None
        self.__context = ExecutingLogContext(task_instance=ti, dag_filename=filepath)

    def emit(self, record: logging.LogRecord):
        """Emits a log record. Override this method to provide record handling.

        Arguments:
            record {any} -- The logging record.
        """

        # Debug executor will not map through the context, and therefore we may set the context
        # by reference
        if IS_RUNNING_DEBUG_EXECUTOR and not self.has_context:
            ti: TaskInstance = get_calling_frame_objects_by_type(TaskInstance, first_only=True)
            if ti is not None:
                self.set_context(ti)

        if self.has_context:
            msg: str = None
            try:
                msg = self.format(record=record)
            except Exception as ex:
                airflow_db_logger_log.error("Error while formatting log", exc_info=ex)
            try:
                write_log(msg, context=self.context, db_session=self.db_session)
            except Exception as ex:
                airflow_db_logger_log.error("Error while writing log", exc_info=ex)

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

        context = ExecutingLogContext(task_instance=task_instance, try_number=try_number)
        return read_airflow_logs(context=context, db_session=self.db_session, raise_exception=False)

    def flush(self):
        """Waits for any unwritten logs to write to the db."""
        if not self.has_session:
            return
        if self.db_session is not None:
            self.db_session.flush()

    def close(self):
        if not self.has_session:
            return
        """Stops and finalizes the log writing.
        """
        if self.db_session is not None:
            self.db_session.close()
            self._db_session = None
