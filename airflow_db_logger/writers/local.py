import os
import logging
from typing import Dict
from weakref import WeakValueDictionary
from zthreading.decorators import collect_delayed_calls_async
from airflow_db_logger.exceptions import DBLoggerException
from airflow_db_logger.config import BASE_LOG_FOLDER
from airflow_db_logger.handlers import DBLogStreamWriter, DBLogHandler, airflow_db_logger_log


class DBLogFileCollectionWriter:
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.pending_records = []

    def write(self, record):
        self.pending_records.append(record)
        self._write_async()

    @collect_delayed_calls_async(interval=0.05, on_error="write_async_error")
    def _write_async(self):
        if len(self.pending_records) == 0:
            return
        try:
            records = self.pending_records
            self.pending_records = []
            records = [str(r) for r in records]
            dirname = os.path.dirname(self.filename)
            if not os.path.isdir(dirname):
                os.makedirs(dirname)
            with open(self.filename, "a") as logfile:
                logfile.write("\n".join(records))
                logfile.write("\n")

        except Exception as err:
            airflow_db_logger_log.error("Failed to flash to file @ " + self.filename)
            airflow_db_logger_log.error(err)

    def write_async_error(self, err: Exception):
        airflow_db_logger_log.error(err)


class DBLogFileWriter(DBLogStreamWriter):
    def __init__(self) -> None:
        super().__init__()
        self._pending_loggers: Dict[str, DBLogFileCollectionWriter] = WeakValueDictionary()

    def get_file_collection_writer(self, filename: str):

        if filename not in self._pending_loggers:
            logger = DBLogFileCollectionWriter(filename)
            self._pending_loggers[filename] = logger
            return logger
        else:
            return self._pending_loggers[filename]

    def write(self, handler: DBLogHandler, record: logging.LogRecord):
        # Only applies when using context execution.
        if not handler.has_context:
            return

        logfile = handler.get_logfile_subpath()
        assert isinstance(logfile, str), DBLoggerException(
            f"Invalid logfile when writing log from {type(handler)}: {logfile}"
        )
        filename = os.path.join(BASE_LOG_FOLDER, handler.get_logfile_subpath())
        self.get_file_collection_writer(filename=filename).write(handler.format(record))
