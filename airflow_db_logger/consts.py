from copy import deepcopy
from airflow_db_logger.shell_logging_config import LOGGING_CONFIG

global IS_LOADING_CONFIG
IS_LOADING_CONFIG = False

global DB_LOGGER_LOGGING_CONFIG
DB_LOGGER_LOGGING_CONFIG = deepcopy(LOGGING_CONFIG)
