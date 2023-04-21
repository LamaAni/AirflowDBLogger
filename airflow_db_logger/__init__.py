#!/usr/bin/env python
from airflow_db_logger.logging_config import init_logging_config

LOGGING_CONFIG: dict = {}
"""airflow_db_logger logging configuration"""

# Override logging config
from airflow_db_logger.logging_config import LOGGING_CONFIG as LOGGING_CONFIG_AS_PROPERTY  # noqa E402


LOGGING_CONFIG = LOGGING_CONFIG_AS_PROPERTY

# call to initialize logging configuration
init_logging_config()

if __name__ == "__main__":
    import json

    print(json.dumps(LOGGING_CONFIG, indent=2))
