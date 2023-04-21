import sys
import logging
from copy import deepcopy
from airflow_db_logger.shell_logging_config import create_shell_logging_config

LOGGING_CONFIG: dict = None


def init_logging_config(self, force: bool = False):
    pass


def create_logging_config():
    pass


class AirflowDBLoggerLoggingConfig:
    def __init__(self) -> None:
        self._logging_config = create_shell_logging_config(logging.ERROR)
        self._was_initialized = False

    @property
    def LOGGING_CONFIG(self) -> dict:
        return self._logging_config

    def init_logging_config(self, force: bool = False):
        if self._was_initialized and not force:
            return

        self._was_initialized = True
        self._logging_config.update(self.create_logging_config())

    def create_logging_config(self):
        from airflow_db_logger.utils import deep_merge_dicts
        from airflow.config_templates.airflow_local_settings import (
            DEFAULT_LOGGING_CONFIG as AIRFLOW_DEFAULT_LOGGING_CONFIG,
        )

        from airflow_db_logger.config import (
            LOG_LEVEL,
            DB_LOGGER_PROCESSOR_FORMATTER,
            DB_LOGGER_WRITE_TO_SHELL,
            DB_LOGGER_PROCESSOR_LOG_LEVEL,
            DB_LOGGER_TASK_FORMATTER,
            DB_LOGGER_CONSOLE_FORMATTER,
            DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB,
            DB_LOGGER_ADD_TASK_DEFAULT_LOG_HANDLER,
            DB_LOGGER_ADD_PROCESSOR_DEFAULT_LOG_HANDLER,
            DB_LOGGER_OVERRIDE_DEFAULT_CONSOLE_HANDLER,
        )

        config = deepcopy(AIRFLOW_DEFAULT_LOGGING_CONFIG)
        processor_handler_config_to_shell = {
            "class": "airflow_db_logger.shell_logging_config.StreamHandler",
            "formatter": DB_LOGGER_CONSOLE_FORMATTER,
            "level": DB_LOGGER_PROCESSOR_LOG_LEVEL,
        }

        processor_handler_config_to_db = {
            "class": "airflow_db_logger.handlers.DBLogHandler",
            "formatter": DB_LOGGER_PROCESSOR_FORMATTER,
            "level": DB_LOGGER_PROCESSOR_LOG_LEVEL,
        }

        processor_handler_config = (
            processor_handler_config_to_db
            if DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB
            else processor_handler_config_to_shell
        )

        # Processor and task handlers are overwritten since
        # airflow uses these handlers in other locations.

        handlers = {
            "task": {
                "class": "airflow_db_logger.handlers.DBLogHandler",
                "formatter": DB_LOGGER_TASK_FORMATTER,
                "level": LOG_LEVEL,
            },
            "airflow.task": config.get("handlers").get("task"),
            "processor": processor_handler_config,
            "airflow.processor": config.get("handlers").get("processor"),
        }

        if DB_LOGGER_OVERRIDE_DEFAULT_CONSOLE_HANDLER:
            handlers["console"] = {
                "class": "airflow_db_logger.shell_logging_config.StreamHandler",
                "formatter": DB_LOGGER_CONSOLE_FORMATTER,
            }

        task_handlers = ["task"]
        if DB_LOGGER_ADD_TASK_DEFAULT_LOG_HANDLER:
            task_handlers.append("airflow.task")

        processor_handlers = []

        if DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB:
            processor_handlers.append("processor")

        if not DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB or DB_LOGGER_ADD_PROCESSOR_DEFAULT_LOG_HANDLER:
            processor_handlers.append("airflow.processor")

        deep_merge_dicts(
            config,
            {
                "handlers": handlers,
                "loggers": {
                    "airflow.processor": {
                        "handlers": processor_handlers,
                        "level": LOG_LEVEL,
                        # Set to true here (and reset via set_context)
                        # so that if no file is configured we still get logs!
                        "propagate": DB_LOGGER_WRITE_TO_SHELL,
                    },
                    "airflow.task": {
                        "handlers": task_handlers,
                        "level": LOG_LEVEL,
                        # Set to true here (and reset via set_context)
                        # so that if no file is configured we still get logs!
                        "propagate": DB_LOGGER_WRITE_TO_SHELL,
                        "filters": ["mask_secrets"],
                    },
                },
            },
        )
        return config


# We want to handle logging config as a setter and therefore we are loading the configuration
# module as a class
sys.modules[__name__] = AirflowDBLoggerLoggingConfig()
