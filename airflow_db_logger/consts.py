import os
from copy import deepcopy
from airflow_db_logger.shell_logging_config import create_shell_logging_config

global IS_DB_LOGGER_LOADING_CONFIG
IS_DB_LOGGER_LOADING_CONFIG = False

global DB_LOGGER_LOGGING_CONFIG
DB_LOGGER_LOGGING_CONFIG = create_shell_logging_config()


def get_default_loggin_config():
    """Returns the airflow default logging config from the settings.

    Start the ariflow system. settings.initialize should be called if the logging configuration is to be reset?
    """
    action_logging_config_env = os.environ.get("AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS", None) or os.environ.get(
        "AIRFLOW__CORE__LOGGING_CONFIG_CLASS", None
    )
    os.environ["AIRFLOW__CORE__LOGGING_CONFIG_CLASS"] = "airflow_db_logger.shell_logging_config.SIMPLE_LOGGING_CONFIG"
    os.environ[
        "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS"
    ] = "airflow_db_logger.shell_logging_config.SIMPLE_LOGGING_CONFIG"

    from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG  # noqa

    os.environ["AIRFLOW__CORE__LOGGING_CONFIG_CLASS"] = action_logging_config_env
    os.environ["AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS"] = action_logging_config_env

    return DEFAULT_LOGGING_CONFIG
