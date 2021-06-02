import re
import os
import logging
from datetime import datetime
from weakref import WeakValueDictionary
from typing import Dict
from airflow_db_logger.handlers import DBLogStreamWriter, DBLogHandler, airflow_db_logger_log
from airflow_db_logger.exceptions import DBLoggerException
from airflow_db_logger.config import (
    DB_LOGGER_WRITE_TO_GCS_BUCKET,
    DB_LOGGER_WRITE_TO_GCS_PROJECT_ID,
    DB_LOGGER_WRITE_TO_GCS_MULTI_FILE_LOG,
)
from zthreading.decorators import collect_delayed_calls_async
from google.cloud.storage import Client

GOOGLE_STORAGE_RUI_REGEX = r"^(gs:\/\/|)([^\/]+)(\/(.*)|)$"


def to_storage_parts(gs_path):
    parts = re.findall(GOOGLE_STORAGE_RUI_REGEX, gs_path)
    bucket_name = parts[0][1]
    bucket_inner_path = parts[0][3] if len(parts[0]) > 2 else None
    return bucket_name, bucket_inner_path


class GCSFileLogProcessor:
    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.pending_records = []
        self.bucket_name, self.bucket_inner_path = to_storage_parts(DB_LOGGER_WRITE_TO_GCS_BUCKET)

    def write(self, record):
        self.pending_records.append(record)
        self._write_async()

    def _compose_progressing_log_file_name(self, filename):
        filename, ext = os.path.splitext(filename)
        return f"{filename}.{datetime.now().strftime('%Y%m%d.%H%M%S.%f')}{ext}"

    @collect_delayed_calls_async(interval=0.1, on_error="write_async_error", use_daemon_thread=False)
    def _write_async(self):
        if len(self.pending_records) == 0:
            return
        try:
            client = Client(project=DB_LOGGER_WRITE_TO_GCS_PROJECT_ID)
            bucket_path = f"{self.bucket_inner_path}/{self.filename}"
            if DB_LOGGER_WRITE_TO_GCS_MULTI_FILE_LOG:
                bucket_path = self._compose_progressing_log_file_name(bucket_path)

            bucket = client.bucket(bucket_name=self.bucket_name)
            blob = bucket.blob(blob_name=bucket_path)

            records = self.pending_records
            self.pending_records = []

            if not DB_LOGGER_WRITE_TO_GCS_MULTI_FILE_LOG and blob.exists():
                current_log = blob.download_as_string().decode(encoding="utf-8").strip()
                if current_log:
                    records.insert(0, current_log)
                # Reset the blob
                blob = bucket.blob(blob_name=bucket_path)

            blob.upload_from_string("\n".join(records))

        except Exception as err:
            airflow_db_logger_log.error(
                f"Failed to flash to bucket @ {self.bucket_name}/{self.bucket_inner_path}/{self.filename}"
            )
            airflow_db_logger_log.error(err)

    def write_async_error(self, err: Exception):
        airflow_db_logger_log.error(err)


class GCSFileWriter(DBLogStreamWriter):
    def __init__(self, on_event=None) -> None:
        super().__init__(on_event=on_event)
        self._pending_loggers: Dict[str, GCSFileLogProcessor] = WeakValueDictionary()

    def get_file_collection_writer(self, filename: str):
        if filename not in self._pending_loggers:
            logger = GCSFileLogProcessor(filename)
            self._pending_loggers[filename] = logger
            return logger
        else:
            return self._pending_loggers[filename]

    def write(self, handler: DBLogHandler, record: logging.LogRecord):
        # Only applies when using context execution.
        if not handler.has_context:
            return

        filename = handler.get_logfile_subpath()
        assert isinstance(filename, str), DBLoggerException(
            f"Invalid filename when writing log @ {type(handler)}: {DB_LOGGER_WRITE_TO_GCS_BUCKET}/{filename}"
        )
        self.get_file_collection_writer(filename=filename).write(handler.format(record))
