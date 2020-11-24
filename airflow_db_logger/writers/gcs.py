from airflow_db_logger.handlers import DBLogStreamWriter, DBLogHandler


class GCSFileWriter(DBLogStreamWriter):
    def write(self, handler: DBLogHandler, record):
        # Only applies when using context execution
        if not handler.has_context:
            return

        pass
