import os
import sys
import logging
from typing import Dict
from weakref import WeakValueDictionary
from zthreading.decorators import collect_delayed_calls_async
from airflow_db_logger.exceptions import DBLoggerException
from airflow_db_logger.config import BASE_LOG_FOLDER
from airflow_db_logger.handlers import DBLogStreamWriter, DBLogHandler, airflow_db_logger_log


class DBLogShellWriter(DBLogStreamWriter):
    def write(self, handler: "DBLogHandler", record: logging.LogRecord):
        msg = handler.format(record) + "\n"
        if record.levelno < logging.WARNING:
            sys.__stdout__.write(msg)
        else:
            sys.__stderr__.write(msg)
