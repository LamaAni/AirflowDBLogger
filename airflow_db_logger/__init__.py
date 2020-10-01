#!/usr/bin/env python
# -*- coding: utf-8 -*-
__title__ = "airflow_db_logger"
__author__ = "Zav Shotan"

from copy import deepcopy
from airflow_db_logger.shell_logging_config import LOGGING_CONFIG

# As a dummy.
LOGGING_CONFIG = deepcopy(LOGGING_CONFIG)

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.logging_config import configure_logging
from airflow_db_logger.config import check_cli_for_init_db

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
        "filename_template": "{{ filename }}.log",
    },
}

# Checking for database initialization
check_cli_for_init_db()

# Re-configure the logging
configure_logging()
