import sys
import logging
from airflow_db_logger.handlers import DBLogStreamWriter, DBLogHandler


class DBLogShellWriter(DBLogStreamWriter):
    def write(self, handler: "DBLogHandler", record: logging.LogRecord):
        msg = handler.format(record) + "\n"
        if record.levelno < logging.WARNING:
            sys.__stdout__.write(msg)
        else:
            sys.__stderr__.write(msg)
