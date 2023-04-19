#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = "airflow_db_logger"
__author__ = "Zav Shotan"

from copy import deepcopy
import airflow_db_logger.consts as consts


LOGGING_CONFIG = deepcopy(consts.DB_LOGGER_LOGGING_CONFIG)
AIRFLOW_DEFAULT_LOGGING_CONFIG = consts.get_default_loggin_config()

from airflow_db_logger.config import (  # noqa
    check_cli_for_init_db,  # noqa: E402
    DB_LOGGER_TASK_FORMATTER,  # noqa: E402
    DB_LOGGER_CONSOLE_FORMATTER,  # noqa: E402
    DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB,  # noqa: E402
    DB_LOGGER_PROCESSER_LOG_LEVEL,
    airflow_db_logger_log,
)


def update_config_from_defaults():
    consts.IS_DB_LOGGER_LOADING_CONFIG

    if consts.IS_DB_LOGGER_LOADING_CONFIG is True:
        return

    # Remove any other loads.
    try:
        consts.IS_DB_LOGGER_LOADING_CONFIG = True

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

        LOGGING_CONFIG.update(AIRFLOW_DEFAULT_LOGGING_CONFIG)
        LOGGING_CONFIG["handlers"] = {
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

        # Checking for database initialization
        check_cli_for_init_db()

    finally:
        consts.IS_DB_LOGGER_LOADING_CONFIG = False


update_config_from_defaults()

if __name__ == "__main__":
    import json

    print(json.dumps(LOGGING_CONFIG))
