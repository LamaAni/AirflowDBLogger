import sys
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, scoped_session, Session
from sqlalchemy.pool import NullPool, QueuePool

from airflow_db_logger.log import airflow_db_logger_log

DB_LOGGER_ENGINE: Engine = None
db_logger_session: Session = None


def create_db_logger_sqlalchemy_engine():
    from airflow_db_logger.config import (
        DB_LOGGER_SQL_ALCHEMY_POOL_ENABLED,
        DB_LOGGER_SQL_ALCHEMY_POOL_SIZE,
        DB_LOGGER_SQL_ALCHEMY_MAX_OVERFLOW,
        DB_LOGGER_SQL_ALCHEMY_POOL_RECYCLE,
        DB_LOGGER_SQL_ALCHEMY_POOL_PRE_PING,
        DB_LOGGER_SQL_ALCHEMY_CONNECTION_ARGS,
        DB_LOGGER_SQL_ALCHEMY_ENGINE_ENCODING,
        DB_LOGGER_SQL_ALCHEMY_CONNECTION,
    )

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
    if DB_LOGGER_SQL_ALCHEMY_ENGINE_ENCODING:
        engine_args["encoding"] = DB_LOGGER_SQL_ALCHEMY_ENGINE_ENCODING

    # DEPRECATED:
    # # For Python2 we get back a newstr and need a str
    # engine_args["encoding"] = engine_args["encoding"].__str__()

    return create_engine(DB_LOGGER_SQL_ALCHEMY_CONNECTION, **engine_args)


def init_db(reset=False, engine: Engine = None):
    """Initialize the db logger database sqlalchemy models.

    Args:
        reset (bool, optional): If true then reset the tables. Defaults to False.
    """
    from airflow_db_logger.data import LoggerModelBase
    from airflow_db_logger.config import DB_LOGGER_SQL_ALCHEMY_CONNECTION

    airflow_db_logger_log.info(f"AirflowDBLogger is using: {DB_LOGGER_SQL_ALCHEMY_CONNECTION}")
    if reset:
        # NOTE: There is no promp for logs, when you reset, everything will reset always.
        airflow_db_logger_log.info("Resetting db_logger tables...")
        LoggerModelBase.metadata.drop_all(engine)
    else:
        airflow_db_logger_log.info("Initialzing db_logger tables...")

    LoggerModelBase.metadata.create_all(engine)

    airflow_db_logger_log.info("AirflowDBLogger tables initialized.")


def check_cli_for_init_db(reset: bool = None, engine: Engine = None):
    """Returns true if the cli command is for initializing the database."""

    if reset is None:
        reset = False
        # Old system
        if "initdb" in sys.argv or "upgradedb" in sys.argv or "resetdb" in sys.argv:
            reset = "resetdb" in sys.argv

        # New system
        if "db" in sys.argv and ("reset" in sys.argv or "init" in sys.argv or "upgrade" in sys.argv):
            reset = "reset" in sys.argv

    init_db(reset=reset, engine=engine)


def create_db_logger_sqlalchemy_session() -> Session:
    pass


# Property override
class __Module:
    def __init__(self) -> None:
        self._engine: Engine = None
        self._session: Session = None

    @property
    def DB_LOGGER_ENGINE(self) -> object:
        if self._engine is None:
            self._engine = self.create_db_logger_sqlalchemy_engine()
        return self._engine

    @property
    def db_logger_session(self) -> Session:
        if self._session is None:
            self._session = self.create_db_logger_sqlalchemy_session()
        return self._session

    def create_db_logger_sqlalchemy_session(self) -> Session:
        return scoped_session(
            sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.DB_LOGGER_ENGINE,
                expire_on_commit=False,
            )
        )

    def create_db_logger_sqlalchemy_engine(self):
        return create_db_logger_sqlalchemy_engine()

    def init_db(self, reset=False, engine: Engine = None):
        init_db(reset, engine or self.DB_LOGGER_ENGINE)

    def check_cli_for_init_db(self, reset=False, engine: Engine = None):
        check_cli_for_init_db(reset, engine or self.DB_LOGGER_ENGINE)


# We want to handle logging config as a setter and therefore we are loading the configuration
# module as a class
sys.modules[__name__] = __Module()
