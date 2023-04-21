from datetime import datetime
from typing import Any, List
from sqlalchemy.orm import Session, Query

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import BaseOperator
from airflow_db_logger.config import AIRFLOW_MAJOR_VERSION
from airflow_db_logger.data import DBLoggerLogRecord, DBLoggerLogCategory

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
        categories: List[DBLoggerLogCategory] = None,
        **kwargs,
    ) -> None:
        """Cleanup db_logger database logs

        Args:
            task_id (str): The id of the task.
            up_to (datetime): Any log before this date will be deleted.
            since (datetime, optional): Start from this date. Defaults to None.
            categories (DBLoggerLogCategory, optional): Limit the cleanup to the specified categories.
                If None then all.
        """
        assert isinstance(up_to, datetime), ValueError("up_to must be of type datetime")

        super().__init__(task_id=task_id, **kwargs)

        assert categories is None or len(categories) > 0, ValueError(
            "You must provide at least one category (None for all)"
        )

        self.upto = up_to
        self.since = since
        self.categories = categories

    @provide_session
    def execute(self, session: Session, context: Any):
        """
        Executes the cleanup command.
        """

        since = self.since if self.since is not None else "Beginning of time"
        upto = self.upto

        self.log.info(f"Deleting logs {since} -> {upto}")
        query: Query = session.query(DBLoggerLogRecord)
        query = query.filter(DBLoggerLogRecord.timestamp < self.upto)
        if self.since:
            query = query.filter(DBLoggerLogRecord.timestamp >= self.since)

        if self.categories:
            self.log.info(f"Limited to categories: {self.categories}")
            query = query.filter(DBLoggerLogRecord.category.in_(self.categories))

        delete_count = query.delete()
        query.session.commit()

        self.log.info(f"Deleted {delete_count} records")
