from datetime import datetime
from typing import Any
from sqlalchemy.orm import Session, Query

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import BaseOperator
from airflow_db_logger.config import AIRFLOW_MAJOR_VERSION
from airflow_db_logger.exceptions import DBLoggerException
from airflow_db_logger.data import DagFileProcessingLogRecord, TaskExecutionLogRecord, LoggerModelBase

if AIRFLOW_MAJOR_VERSION > 1:
    from airflow.utils.session import provide_session
else:
    from airflow.utils.db import provide_session


class AirflowDBLoggerCleanupOperator(BaseOperator, LoggingMixin):
    def __init__(
        self,
        task_id: str,
        up_to: datetime,
        since: datetime = None,
        include_task_logs=True,
        include_operations_log=True,
        **kwargs,
    ) -> None:
        assert isinstance(up_to, datetime), ValueError("up_to must be of type datetime")

        super().__init__(task_id=task_id, **kwargs)

        assert include_operations_log or include_task_logs, ValueError(
            "Either include_operations_log or include_task_logs must be true"
        )

        self.upto = up_to
        self.since = since
        self.include_task_logs = include_task_logs
        self.include_operations_log = include_operations_log

    @provide_session
    def execute(self, session: Session, context: Any):
        """
        Executes the cleanup command.
        """

        since = self.since if self.since is not None else "Beginning of time"
        upto = self.upto

        if self.include_operations_log:
            self.log.info(f"operations logs: Deleting from {since} -> {upto}")
            query: Query = session.query(DagFileProcessingLogRecord)
            query = query.filter(DagFileProcessingLogRecord.timestamp < self.upto)
            if self.since:
                query = query.filter(DagFileProcessingLogRecord.timestamp >= self.since)

            delete_count = query.delete()
            query.session.commit()
            self.log.info(f"operations logs: Deleted {delete_count} records")

        if self.include_task_logs:
            self.log.info(f"Task logs: Deleting from {since} -> {upto}")
            query: Query = session.query(TaskExecutionLogRecord)
            query = query.filter(TaskExecutionLogRecord.timestamp < self.upto)
            if self.since:
                query = query.filter(TaskExecutionLogRecord.timestamp >= self.since)

            delete_count = query.delete()
            query.session.commit()
            self.log.info(f"Task logs: Deleted {delete_count} records")

        self.log.info("Cleanup complete")
