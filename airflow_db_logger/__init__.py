#!/usr/bin/env python

from copy import deepcopy

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow_db_logger.config import (  # noqa
    check_cli_for_init_db,  # noqa: E402
    DB_LOGGER_TASK_FORMATTER,  # noqa: E402
    DB_LOGGER_CONSOLE_FORMATTER,  # noqa: E402
    DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB,  # noqa: E402
    DB_LOGGER_PROCESSER_LOG_LEVEL,
)


def create_logging_config():
    config = deepcopy(DEFAULT_LOGGING_CONFIG)
    processor_handler_config = {
        "class": "airflow_db_logger.handlers.StreamHandler",
        "formatter": DB_LOGGER_CONSOLE_FORMATTER,
        "level": DB_LOGGER_PROCESSER_LOG_LEVEL,
    }

    if DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB:
        processor_handler_config = {
            "class": "airflow_db_logger.handlers.DBProcessLogHandler",
            "formatter": DB_LOGGER_CONSOLE_FORMATTER,
            "level": DB_LOGGER_PROCESSER_LOG_LEVEL,
        }

    config["handlers"] = {
        "console": {
            "class": "airflow_db_logger.handlers.StreamHandler",
            "formatter": DB_LOGGER_CONSOLE_FORMATTER,
        },
        "task": {
            "class": "airflow_db_logger.handlers.DBTaskLogHandler",
            "formatter": DB_LOGGER_TASK_FORMATTER,
        },
        "processor": processor_handler_config,
    }
    return config


LOGGING_CONFIG = create_logging_config()

# Checking for database initialization
check_cli_for_init_db()


if __name__ == "__main__":
    import json

    print(json.dumps(LOGGING_CONFIG, indent=2))
