#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = "airflow_db_logger"
__author__ = "Zav Shotan"

from copy import deepcopy
import airflow_db_logger.consts as consts

# THIS IS UGLY

LOGGING_CONFIG = consts.DB_LOGGER_LOGGING_CONFIG

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG  # noqa: E402
from airflow_db_logger.config import check_cli_for_init_db, FILENAME_TEMPLATE  # noqa: E402


def update_config_from_defaults():
    consts.IS_LOADING_CONFIG

    if consts.IS_LOADING_CONFIG is True:
        return

    # Remove any other loads.
    consts.IS_LOADING_CONFIG = True

    LOGGING_CONFIG.update(deepcopy(DEFAULT_LOGGING_CONFIG))
    LOGGING_CONFIG["handlers"] = {
        "console": {
            "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
            "formatter": "airflow_coloured",
            "stream": "sys.stdout",
        },
        "task": {
            "class": "airflow_db_logger.handlers.DBTaskLogHandler",
            "formatter": "airflow",
        },
        "processor": {
            "class": "airflow_db_logger.handlers.DBProcessLogHandler",
            "formatter": "airflow_coloured",
            "filename_template": FILENAME_TEMPLATE.replace("{{", "{").replace("}}", "}"),
        },
    }

    # Checking for database initialization
    check_cli_for_init_db()


update_config_from_defaults()
