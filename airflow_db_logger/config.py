import sys
import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool, QueuePool
from airflow.configuration import conf

# Loading airflow parameters
LOG_LEVEL = conf.get("core", "LOGGING_LEVEL").upper()
FAB_LOG_LEVEL = conf.get("core", "FAB_LOGGING_LEVEL").upper()
LOG_FORMAT = conf.get("core", "log_format")
SQL_ALCHEMY_CONN = conf.get("core", "SQL_ALCHEMY_CONN")
DAGS_FOLDER = os.path.expanduser(conf.get("core", "DAGS_FOLDER"))
SQL_ALCHEMY_SCHEMA = conf.get("core", "SQL_ALCHEMY_SCHEMA")
COLORED_CONSOLE_LOG = conf.getboolean("core", "colored_console_log")

# DB logger configuration
DB_LOGGER_SQL_ALCHEMY_SCHEMA = conf.get("db_logger", "SQL_ALCHEMY_SCHEMA", fallback=SQL_ALCHEMY_SCHEMA)
DB_LOGGER_SQL_ALCHEMY_CONNECTION = conf.get("db_logger", "SQL_ALCHEMY_CONN", fallback=SQL_ALCHEMY_CONN)
DB_LOGGER_SHOW_REVERSE_ORDER = conf.getboolean("db_logger", "show_reverse", fallback=False)

# Setting the default logger log level
logging.basicConfig(level=LOG_LEVEL)


def create_db_logger_sqlalchemy_engine():

    # Configuring the db_logger sql engine.
    engine_args = {}
    pool_enabled = conf.getboolean("db_logger", "SQL_ALCHEMY_POOL_ENABLED", fallback=True)
    if pool_enabled:
        # Copied from airflow main repo.

        # Pool size engine args not supported by sqlite.
        # If no config value is defined for the pool size, select a reasonable value.
        # 0 means no limit, which could lead to exceeding the Database connection limit.
        pool_size = conf.getint("db_logger", "SQL_ALCHEMY_POOL_SIZE", fallback=5)

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
        max_overflow = conf.getint("db_logger", "SQL_ALCHEMY_MAX_OVERFLOW", fallback=1)

        # The DB server already has a value for wait_timeout (number of seconds after
        # which an idle sleeping connection should be killed). Since other DBs may
        # co-exist on the same server, SQLAlchemy should set its
        # pool_recycle to an equal or smaller value.
        pool_recycle = conf.getint("db_logger", "SQL_ALCHEMY_POOL_RECYCLE", fallback=1800)

        # Check connection at the start of each connection pool checkout.
        # Typically, this is a simple statement like “SELECT 1”, but may also make use
        # of some DBAPI-specific method to test the connection for liveness.
        # More information here:
        # https://docs.sqlalchemy.org/en/13/core/pooling.html#disconnect-handling-pessimistic
        pool_pre_ping = conf.getboolean("db_logger", "SQL_ALCHEMY_POOL_PRE_PING", fallback=True)

        engine_args["pool_size"] = pool_size
        engine_args["pool_recycle"] = pool_recycle
        engine_args["pool_pre_ping"] = pool_pre_ping
        engine_args["max_overflow"] = max_overflow
        engine_args["poolclass"] = QueuePool
    else:
        engine_args = {"poolclass": NullPool}

    # Allow the user to specify an encoding for their DB otherwise default
    # to utf-8 so jobs & users with non-latin1 characters can still use
    # us.
    engine_args["encoding"] = conf.get("db_logger", "SQL_ENGINE_ENCODING", fallback="utf-8")
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

    logging.info(f"Using {DB_LOGGER_SQL_ALCHEMY_CONNECTION}")
    if reset:
        # NOTE: There is no promp for logs, when you reset, everything will reset always.
        logging.info("Resetting db_logger tables...")
        LoggerModelBase.metadata.drop_all(DB_LOGGER_ENGINE)
    else:
        logging.info("Initialzing db_logger tables...")
    LoggerModelBase.metadata.create_all(DB_LOGGER_ENGINE)

    logging.info("db_logger tables initialized.")


def check_cli_for_init_db():
    """Returns true if the cli command is for initializing the database."""
    if "initdb" in sys.argv or "upgradedb" in sys.argv or "resetdb" in sys.argv:
        init_logger("resetdb" in sys.argv)
