import sys
import logging
import colorlog


def configure_logging():
    from airflow_db_logger.config import (
        DB_LOGGER_SQL_ALCHEMY_CONNECTION,
        DB_LOGGER_SQL_ALCHEMY_CREATE_INDEXES,
        DB_LOGGER_SQL_ALCHEMY_SCHEMA,
        IS_USING_COLORED_CONSOLE,
        DB_LOGGER_COLORED_LOG_FORMAT,
        DB_LOGGER_LOG_FORMAT,
        DB_LOGGER_LOG_LEVEL,
        LOG_LEVEL,
    )

    # Setting the default logger log level
    logging.basicConfig(level=LOG_LEVEL)

    logging.debug(f"DBLogger is connecting to: {DB_LOGGER_SQL_ALCHEMY_CONNECTION}/{DB_LOGGER_SQL_ALCHEMY_SCHEMA}")
    logging.debug(f"DBLogger indexes: {DB_LOGGER_SQL_ALCHEMY_CREATE_INDEXES}")

    airflow_db_logger_log.setLevel(DB_LOGGER_LOG_LEVEL)
    airflow_db_logger_log.propagate = False
    airflow_db_logger_log.handlers.clear()
    stderr_handler = logging.StreamHandler(stream=sys.__stderr__)
    if not IS_USING_COLORED_CONSOLE:
        stderr_handler.setFormatter(logging.Formatter(fmt=DB_LOGGER_LOG_FORMAT))
    else:
        stderr_handler.setFormatter(colorlog.ColoredFormatter(fmt=DB_LOGGER_COLORED_LOG_FORMAT))

    airflow_db_logger_log.addHandler(stderr_handler)
    airflow_db_logger_log.info("Initialized.")


airflow_db_logger_log = logging.getLogger("airflow_db_logger")
airflow_db_logger_log.setLevel(logging.INFO)
