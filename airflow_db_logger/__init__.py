#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = "airflow_db_logger"
__author__ = "Zav Shotan"

from copy import deepcopy
import airflow_db_logger.consts as consts

# THIS IS UGLY

LOGGING_CONFIG = consts.DB_LOGGER_LOGGING_CONFIG

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG  # noqa
from airflow_db_logger.config import (  # noqa
    check_cli_for_init_db,  # noqa: E402
    DB_LOGGER_TASK_FORMATTER,  # noqa: E402
    DB_LOGGER_CONSOLE_FORMATTER,  # noqa: E402
    DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB,  # noqa: E402
)


def update_config_from_defaults():
    consts.IS_LOADING_CONFIG

    if consts.IS_LOADING_CONFIG is True:
        return

    # Remove any other loads.
    consts.IS_LOADING_CONFIG = True

    processor_handler_config = {
        "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
        "formatter": DB_LOGGER_CONSOLE_FORMATTER,
        "stream": "sys.stdout",
    }

    if DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB:
        processor_handler_config = {
            "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
            "formatter": DB_LOGGER_CONSOLE_FORMATTER,
        }

    LOGGING_CONFIG.update(deepcopy(DEFAULT_LOGGING_CONFIG))
    LOGGING_CONFIG["handlers"] = {
        "console": {
            "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
            "formatter": DB_LOGGER_CONSOLE_FORMATTER,
            "stream": "sys.stdout",
        },
        "task": {
            "class": "airflow_db_logger.handlers.DBTaskLogHandler",
            "formatter": DB_LOGGER_TASK_FORMATTER,
        },
        "processor": processor_handler_config,
    }

    # Checking for database initialization
    check_cli_for_init_db()


update_config_from_defaults()
