#!/usr/bin/env python
from airflow_db_logger.logging_config import init_logging_config
from airflow_db_logger.log import configure_logging
from airflow_db_logger.db import check_cli_for_init_db

LOGGING_CONFIG: dict = {}
"""airflow_db_logger logging configuration"""

# Override logging config
from airflow_db_logger.logging_config import LOGGING_CONFIG as LOGGING_CONFIG_AS_PROPERTY  # noqa E402


LOGGING_CONFIG = LOGGING_CONFIG_AS_PROPERTY

# call to initialize logging configuration
init_logging_config()

# check for database init
check_cli_for_init_db()

# Initialize the logging
configure_logging()


if __name__ == "__main__":
    import json

    print(json.dumps(LOGGING_CONFIG, indent=2))
