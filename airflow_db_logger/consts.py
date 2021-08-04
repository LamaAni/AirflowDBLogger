import os
from copy import deepcopy
from airflow_db_logger.shell_logging_config import create_shell_logging_config

global IS_DB_LOGGER_LOADING_CONFIG
IS_DB_LOGGER_LOADING_CONFIG = False

global DB_LOGGER_LOGGING_CONFIG
DB_LOGGER_LOGGING_CONFIG = create_shell_logging_config()


def get_default_loggin_config():
    from airflow.version import version as AIRFLOW_VERSION

    AIRFLOW_VERSION_PARTS = AIRFLOW_VERSION.split(".")
    AIRFLOW_VERSION_PARTS = [int(v) for v in AIRFLOW_VERSION_PARTS]

    AIRFLOW_MAJOR_VERSION = AIRFLOW_VERSION_PARTS[0]

    """Returns the airflow default logging config from the settings.

    Start the ariflow system. settings.initialize should be called if the logging configuration is to be reset?
    """

    config_env_name = (
        "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS" if AIRFLOW_MAJOR_VERSION > 1 else "AIRFLOW__CORE__LOGGING_CONFIG_CLASS"
    )

    action_logging_config_env = os.environ.get(config_env_name, None)

    os.environ[config_env_name] = "airflow_db_logger.shell_logging_config.SIMPLE_LOGGING_CONFIG"

    from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG  # noqa

    if action_logging_config_env is not None:
        os.environ[config_env_name] = action_logging_config_env

    return DEFAULT_LOGGING_CONFIG
