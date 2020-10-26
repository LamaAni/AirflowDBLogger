from airflow_db_logger.handlers import DBLogStreamWriter, DBLogHandler


class GCSFileWriter(DBLogStreamWriter):
    def write(self, handler: DBLogHandler, record):
        pass
