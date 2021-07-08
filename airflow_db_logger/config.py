import sys
import os
import logging
import warnings
import colorlog
from typing import Union, List
from typing import Type
from enum import Enum
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool, QueuePool
from airflow.configuration import conf, AirflowConfigException, log
from airflow.version import version as AIRFLOW_VERSION

AIRFLOW_CONFIG_SECTION_NAME = "db_logger"
AIRFLOW_VERSION_PARTS = AIRFLOW_VERSION.split(".")
AIRFLOW_VERSION_PARTS = [int(v) for v in AIRFLOW_VERSION_PARTS]

AIRFLOW_MAJOR_VERSION = AIRFLOW_VERSION_PARTS[0]


def conf_get_no_warnings_no_errors(*args, **kwargs):
    old_level = log.level
    log.level = logging.ERROR
    try:
        val = conf.get(*args, **kwargs)
    except AirflowConfigException:
        val = None
    log.level = old_level
    return val


def get(
    key: str,
    default=None,
    otype: Type = None,
    allow_empty: bool = False,
    collection: Union[str, List[str]] = None,
):
    collection = collection or "db_logger"
    collection = collection if isinstance(collection, list) else [collection]
    otype = otype or str if default is None else default.__class__
    collection = collection or AIRFLOW_CONFIG_SECTION_NAME
    for col in collection:
        val = conf_get_no_warnings_no_errors(col, key)
        if val is not None:
            break

    assert all([isinstance(v, str) for v in collection]), AirflowConfigException(
        "Collection must be a non empty string or a collection of non empty strings"
    )

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
LOG_LEVEL = get(collection=["logging", "core"], key="logging_level").upper()
FILENAME_TEMPLATE = get(collection=["logging", "core"], key="LOG_FILENAME_TEMPLATE")
AIRFLOW_EXECUTOR = get(collection="core", key="executor")
IS_RUNNING_DEBUG_EXECUTOR = AIRFLOW_EXECUTOR == "DebugExecutor"
IS_USING_COLORED_CONSOLE = get(collection=["logging", "core"], key="colored_console_log").lower() == "true"

# Loading sql parameters
SQL_ALCHEMY_CONN = get(collection="core", key="sql_alchemy_conn", allow_empty=False)
DAGS_FOLDER = os.path.expanduser(get(collection="core", key="dags_folder"))
BASE_LOG_FOLDER = os.path.expanduser(get(collection=["logging", "core"], key="base_log_folder"))
SQL_ALCHEMY_SCHEMA = get(collection="core", key="sql_alchemy_schema", allow_empty=True)

TASK_LOG_FILENAME_TEMPLATE = (
    get(
        collection="core",
        key="LOG_FILENAME_TEMPLATE",
        default="{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log",
    )
    .replace("{{{{", "{{")
    .replace("}}}}", "}}")
)
PROCESS_LOG_FILENAME_TEMPLATE = (
    get(
        collection="core",
        key="log_processor_filename_template",
        default="{{ filename }}.log",
    )
    .replace("{{{{", "{{")
    .replace("}}}}", "}}")
)

DB_LOGGER_LOG_LEVEL = get(key="logging_level", default="INFO").upper()
DB_LOGGER_SQL_ALCHEMY_SCHEMA = get("sql_alchemy_schema", SQL_ALCHEMY_SCHEMA)
DB_LOGGER_SQL_ALCHEMY_CONNECTION = get("sql_alchemy_conn", SQL_ALCHEMY_CONN)
DB_LOGGER_SHOW_REVERSE_ORDER = get("show_reverse", False)
DB_LOGGER_SQL_ALCHEMY_CONNECTION_ARGS = get("sql_alchemy_conn_args", None, allow_empty=True)
DB_LOGGER_CREATE_INDEXES = get("create_index", True)
DB_LOGGER_WRITE_DAG_PROCESSING_TO_DB = get("write_dag_processing_to_db", False)

DB_LOGGER_SQL_ALCHEMY_POOL_ENABLED = get("sql_alchemy_pool_enabled", False)
DB_LOGGER_SQL_ALCHEMY_POOL_SIZE = get("sql_alchemy_pool_size", 5)
DB_LOGGER_SQL_ALCHEMY_MAX_OVERFLOW = get("sql_alchemy_max_overflow", 1)
DB_LOGGER_SQL_ALCHEMY_POOL_RECYCLE = get("sql_alchemy_pool_recycle", 1800)
DB_LOGGER_SQL_ALCHEMY_POOL_PRE_PING = get("sql_alchemy_pool_pre_ping", True)
DB_LOGGER_SQL_ENGINE_ENCODING = get("sql_engine_encoding", "utf-8")

DB_LOGGER_GOOGLE_APP_CREDS_PATH = get("google_application_credentials", default=None, allow_empty=True, otype=str)
# A bucket path, requires google-cloud-storage to be installed.
DB_LOGGER_WRITE_TO_GCS_BUCKET = get("write_to_gcs_bucket", default=None, allow_empty=True, otype=str)
DB_LOGGER_WRITE_TO_GCS_PROJECT_ID = get("write_to_gcs_project_id", default=None, allow_empty=True, otype=str)
DB_LOGGER_WRITE_TO_GCS_MULTI_FILE_LOG = get("write_to_gcs_multi_file_log", default=False)
DB_LOGGER_PROCESSER_LOG_LEVEL = get("processer_log_level", default="WARN", allow_empty=True, otype=str)
# True or path
DB_LOGGER_WRITE_TO_FILES = get("write_to_files", default=False)
# True or path
DB_LOGGER_WRITE_TO_SHELL = get("write_to_shell", default=IS_RUNNING_DEBUG_EXECUTOR)

DB_LOGGER_CONSOLE_FORMATTER = "airflow_coloured" if IS_USING_COLORED_CONSOLE else "airflow"
DB_LOGGER_TASK_FORMATTER = "airflow_coloured" if IS_RUNNING_DEBUG_EXECUTOR and IS_USING_COLORED_CONSOLE else "airflow"


# Setting the default logger log level
logging.basicConfig(level=LOG_LEVEL)

logging.debug(f"DBLogger is connecting to: {DB_LOGGER_SQL_ALCHEMY_CONNECTION}/{DB_LOGGER_SQL_ALCHEMY_SCHEMA}")
logging.debug(f"DBLogger indexes: {DB_LOGGER_CREATE_INDEXES}")


def create_db_logger_sqlalchemy_engine():

    # Configuring the db_logger sql engine.
    engine_args = {}
    if DB_LOGGER_SQL_ALCHEMY_POOL_ENABLED:
        # Copied from airflow main repo.

        # Pool size engine args not supported by sqlite.
        # If no config value is defined for the pool size, select a reasonable value.
        # 0 means no limit, which could lead to exceeding the Database connection limit.
        pool_size = DB_LOGGER_SQL_ALCHEMY_POOL_SIZE

        # The maximum overflow size of the pool.
        # When the number of checked-out connections reaches the size set in pool_size,
        # additional connections will be returned up to this limit.
        # When those additional connections are returned to the pool, they are disconnected and discarded.
        # It follows then that the total number of simultaneous connections
        # the pool will allow is pool_size + max_overflow,
        # and the total number of “sleeping” connections the pool will allow is pool_size.
        # max_overflow can be set to -1 to indicate no overflow limit;
        # no limit will be placed on the total number
        # of concurrent connections. Defaults to 10.
        max_overflow = DB_LOGGER_SQL_ALCHEMY_MAX_OVERFLOW

        # The DB server already has a value for wait_timeout (number of seconds after
        # which an idle sleeping connection should be killed). Since other DBs may
        # co-exist on the same server, SQLAlchemy should set its
        # pool_recycle to an equal or smaller value.
        pool_recycle = DB_LOGGER_SQL_ALCHEMY_POOL_RECYCLE

        # Check connection at the start of each connection pool checkout.
        # Typically, this is a simple statement like “SELECT 1”, but may also make use
        # of some DBAPI-specific method to test the connection for liveness.
        # More information here:
        # https://docs.sqlalchemy.org/en/13/core/pooling.html#disconnect-handling-pessimistic
        pool_pre_ping = DB_LOGGER_SQL_ALCHEMY_POOL_PRE_PING

        engine_args["pool_size"] = pool_size
        engine_args["pool_recycle"] = pool_recycle
        engine_args["pool_pre_ping"] = pool_pre_ping
        engine_args["max_overflow"] = max_overflow
        engine_args["poolclass"] = QueuePool
    else:
        engine_args = {"poolclass": NullPool}

    # Adding extra arguments
    if DB_LOGGER_SQL_ALCHEMY_CONNECTION_ARGS is not None:
        extra_args = DB_LOGGER_SQL_ALCHEMY_CONNECTION_ARGS.split(",")
        for arg in extra_args:
            if "=" not in arg:
                continue

            equal_idx = arg.index("=")
            key = arg[:equal_idx].strip()
            value = arg[equal_idx + 1 :].strip()  # noqa: E203
            if len(value) == 0 or len(key) == 0:
                continue
            engine_args[key] = value

    # Allow the user to specify an encoding for their DB otherwise default
    # to utf-8 so jobs & users with non-latin1 characters can still use
    # us.
    engine_args["encoding"] = DB_LOGGER_SQL_ENGINE_ENCODING
    # For Python2 we get back a newstr and need a str
    engine_args["encoding"] = engine_args["encoding"].__str__()

    return create_engine(DB_LOGGER_SQL_ALCHEMY_CONNECTION, **engine_args)


# The main db session. Used in all logging.
DB_LOGGER_ENGINE = create_db_logger_sqlalchemy_engine()

DBLoggerSession = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=DB_LOGGER_ENGINE, expire_on_commit=False)
)


def init_logger(reset=False):
    """Initialize the db logger database sqlalchemy models.

    Args:
        reset (bool, optional): If true then reset the tables. Defaults to False.
    """
    from airflow_db_logger.data import LoggerModelBase

    log.info(f"AirflowDBLogger is using: {DB_LOGGER_SQL_ALCHEMY_CONNECTION}")
    if reset:
        # NOTE: There is no promp for logs, when you reset, everything will reset always.
        log.info("Resetting db_logger tables...")
        LoggerModelBase.metadata.drop_all(DB_LOGGER_ENGINE)
    else:
        log.info("Initialzing db_logger tables...")
    LoggerModelBase.metadata.create_all(DB_LOGGER_ENGINE)

    log.info("AirflowDBLogger tables initialized.")


def check_cli_for_init_db():
    """Returns true if the cli command is for initializing the database."""
    # Old system
    if "initdb" in sys.argv or "upgradedb" in sys.argv or "resetdb" in sys.argv:
        init_logger("resetdb" in sys.argv)

    # New system
    if "db" in sys.argv and ("reset" in sys.argv or "init" in sys.argv or "upgrade" in sys.argv):
        init_logger("reset" in sys.argv)


airflow_db_logger_log = logging.getLogger(__file__)
airflow_db_logger_log.setLevel(DB_LOGGER_LOG_LEVEL)
airflow_db_logger_log.propagate = False
airflow_db_logger_log.handlers.clear()
stderr_handler = logging.StreamHandler(stream=sys.__stderr__)
if not IS_USING_COLORED_CONSOLE:
    stderr_handler.setFormatter(logging.Formatter(fmt="[%(asctime)s][airflow_db_logger][%(levelname)s] %(message)s"))
else:
    stderr_handler.setFormatter(
        colorlog.ColoredFormatter(
            fmt="%(blue)s[%(asctime)s]%(purple)s[airflow_db_logger]%(log_color)s[%(levelname)s]%(reset)s %(message)s"
        )
    )

airflow_db_logger_log.addHandler(stderr_handler)
airflow_db_logger_log.info("Initialized.")
