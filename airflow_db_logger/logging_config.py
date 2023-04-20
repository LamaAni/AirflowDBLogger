import sys
from copy import deepcopy
from airflow_db_logger.logging_config_loopback_defaults import DEFAULT_LOOPBACK_LOGGING_CONFIG


class AirflowDBLoggerLoggingConfig:
    def __init__(self) -> None:
        self._logging_config = DEFAULT_LOOPBACK_LOGGING_CONFIG
        self._was_initialized = False

    @property
    def LOGGING_CONFIG(self) -> dict:
        return self._logging_config

    def init_logging_config(self, force: bool = False):
        if self._was_initialized and not force:
            return

        self._was_initialized = True
        self._logging_config.update(self.create_logging_config())

        from airflow_db_logger.config import check_cli_for_init_db

        # Checking for database initialization
        check_cli_for_init_db()

    def create_logging_config(self):
        from airflow_db_logger.utils import deep_merge_dicts

        from airflow.config_templates.airflow_local_settings import (
            DEFAULT_LOGGING_CONFIG as AIRFLOW_DEFAULT_LOGGING_CONFIG,
        )

        from airflow_db_logger.config import (  # noqa
            DB_LOGGER_TASK_FORMATTER,  # noqa: E402
            DB_LOGGER_CONSOLE_FORMATTER,  # noqa: E402
            DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB,  # noqa: E402
            DB_LOGGER_PROCESSER_LOG_LEVEL,
        )

        config = deepcopy(AIRFLOW_DEFAULT_LOGGING_CONFIG)
        processor_handler_config_to_shell = {
            "class": "airflow_db_logger.handlers.StreamHandler",
            "formatter": DB_LOGGER_CONSOLE_FORMATTER,
            "level": DB_LOGGER_PROCESSER_LOG_LEVEL,
        }

        processor_handler_config_to_db = {
            "class": "airflow_db_logger.handlers.DBProcessLogHandler",
            "formatter": DB_LOGGER_CONSOLE_FORMATTER,
            "level": DB_LOGGER_PROCESSER_LOG_LEVEL,
        }

        if DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB:
            processor_handler_config_to_shell = {
                "class": "airflow_db_logger.handlers.DBProcessLogHandler",
                "formatter": DB_LOGGER_CONSOLE_FORMATTER,
                "level": DB_LOGGER_PROCESSER_LOG_LEVEL,
            }
        config["handlers"] = deep_merge_dicts(
            config["handlers"] if config["handlers"] is not None else {},
            {
                "console": {
                    "class": "airflow_db_logger.handlers.StreamHandler",
                    "formatter": DB_LOGGER_CONSOLE_FORMATTER,
                },
                "task": {
                    "class": "airflow_db_logger.handlers.DBTaskLogHandler",
                    "formatter": DB_LOGGER_TASK_FORMATTER,
                },
                "processor": processor_handler_config_to_shell
                if DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB
                else processor_handler_config_to_db,
            },
        )

        return config


# We want to handle logging config as a setter and therefore we are loading the configuration
# module as a class
sys.modules[__name__] = AirflowDBLoggerLoggingConfig()
