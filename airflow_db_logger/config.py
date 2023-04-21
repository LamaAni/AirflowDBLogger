import logging
from typing import Union, List, Type
from enum import Enum
from airflow.configuration import conf, AirflowConfigException, log
from airflow.version import version as AIRFLOW_VERSION


AIRFLOW_CONFIG_SECTION_NAME = "db_logger"
AIRFLOW_VERSION_PARTS = AIRFLOW_VERSION.split(".")
AIRFLOW_VERSION_PARTS = [int(v) for v in AIRFLOW_VERSION_PARTS]
AIRFLOW_MAJOR_VERSION = AIRFLOW_VERSION_PARTS[0]

DB_LOGGER_DEFAULT_LOG_SPLASH = "[Airflow DBLogger] logs loaded from database\n"


def conf_get_no_warnings_no_errors(*args, **kwargs):
    old_level = log.level
    log.level = logging.ERROR
    try:
        val = conf.get(*args, **kwargs)
    except AirflowConfigException:
        val = None
    log.level = old_level
    return val


def __add_core(*collections):
    if AIRFLOW_MAJOR_VERSION < 1:
        collections = ["core", *collections]
    return collections


def get(
    key: str,
    default=None,
    otype: Type = None,
    allow_empty: bool = False,
    collection: Union[str, List[str]] = None,
):
    collection = collection or AIRFLOW_CONFIG_SECTION_NAME
    if isinstance(collection, str):
        collection = [collection]
    assert all(isinstance(v, str) for v in collection), ValueError("Collection must be a string or a list of strings")
    collection = [v for v in collection if len(v.strip()) > 0]
    assert len(collection) > 0, ValueError("Collection must be a non empty string or list of non empty strings")

    otype = otype or str if default is None else default.__class__
    for col in collection:
        val = conf_get_no_warnings_no_errors(col, key)
        if val is not None:
            break

    if issubclass(otype, Enum):
        allow_empty = False

    value_is_empty = val is None or (isinstance(val, str) and len(val.strip()) == 0)

    if not allow_empty and value_is_empty:
        assert default is not None, AirflowConfigException(
            f"Airflow configuration {collection}.{key} not found, and no default value"
        )
        return default

    if val is None:
        return None
    if otype == bool:
        return val.lower() == "true"
    elif issubclass(otype, Enum):
        val = val.strip()
        return otype(val.strip())
    else:
        return otype(val)


# Loading airflow parameters
LOG_LEVEL = get(collection=__add_core("logging"), key="logging_level").upper()
AIRFLOW_EXECUTOR = get(collection="core", key="executor")
IS_RUNNING_DEBUG_EXECUTOR = AIRFLOW_EXECUTOR == "DebugExecutor"
IS_USING_COLORED_CONSOLE = get(collection=__add_core("logging"), key="colored_console_log", default=False)

# Loading sql parameters
SQL_ALCHEMY_CONN = get(collection=__add_core("database"), key="sql_alchemy_conn", allow_empty=False)
SQL_ALCHEMY_SCHEMA = get(collection=__add_core("database"), key="sql_alchemy_schema", allow_empty=True)

DB_LOGGER_LOG_LEVEL = get(key="logging_level", default="INFO").upper()
DB_LOGGER_PROCESSOR_LOG_LEVEL = get("processor_log_level", default=LOG_LEVEL, allow_empty=True, otype=str)
DB_LOGGER_SHOW_REVERSE_ORDER = get("show_reverse", False)
DB_LOGGER_SHOW_LOG_SPLASH = get("show_log_splash", True)
DB_LOGGER_LOG_SPLASH = get("log_splash", DB_LOGGER_DEFAULT_LOG_SPLASH)

DB_LOGGER_SQL_ALCHEMY_SCHEMA = get("sql_alchemy_schema", SQL_ALCHEMY_SCHEMA)
DB_LOGGER_SQL_ALCHEMY_CONNECTION = get("sql_alchemy_conn", SQL_ALCHEMY_CONN)
DB_LOGGER_SQL_ALCHEMY_CONNECTION_ARGS = get("sql_alchemy_conn_args", None, allow_empty=True)
DB_LOGGER_SQL_ALCHEMY_CREATE_INDEXES = get("create_index", True)
DB_LOGGER_SQL_ALCHEMY_POOL_ENABLED = get("sql_alchemy_pool_enabled", False)
DB_LOGGER_SQL_ALCHEMY_POOL_SIZE = get("sql_alchemy_pool_size", 5)
DB_LOGGER_SQL_ALCHEMY_MAX_OVERFLOW = get("sql_alchemy_max_overflow", 1)
DB_LOGGER_SQL_ALCHEMY_POOL_RECYCLE = get("sql_alchemy_pool_recycle", 1800)
DB_LOGGER_SQL_ALCHEMY_POOL_PRE_PING = get("sql_alchemy_pool_pre_ping", True)
DB_LOGGER_SQL_ALCHEMY_ENGINE_ENCODING = get("sql_engine_encoding", None, allow_empty=True)

DB_LOGGER_ADD_TASK_DEFAULT_LOG_HANDLER = get("add_task_default_log_handler", default=True)
DB_LOGGER_ADD_PROCESSOR_DEFAULT_LOG_HANDLER = get("add_processor_default_log_handler", default=False)
DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB = get("write_dag_processing_to_db", False)
DB_LOGGER_WRITE_TO_SHELL = get("write_to_shell", default=IS_RUNNING_DEBUG_EXECUTOR)
DB_LOGGER_OVERRIDE_DEFAULT_CONSOLE_HANDLER = get("override_default_console_handler", default=True)

DB_LOGGER_CONSOLE_FORMATTER = "airflow_coloured" if IS_USING_COLORED_CONSOLE else "airflow"
DB_LOGGER_TASK_FORMATTER = "airflow"
DB_LOGGER_PROCESSOR_FORMATTER = "airflow"

DB_LOGGER_COLORED_LOG_FORMAT = get(
    "colored_log_format",
    "%(blue)s[%(asctime)s]%(purple)s[airflow_db_logger]%(log_color)s[%(levelname)s]%(reset)s %(message)s",
)
DB_LOGGER_LOG_FORMAT = get(
    "log_format",
    "[%(asctime)s][airflow_db_logger][%(levelname)s] %(message)s",
)
