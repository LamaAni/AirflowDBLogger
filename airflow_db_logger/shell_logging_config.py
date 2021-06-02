import sys
import logging

LOG_LEVEL = logging.INFO
LOG_FORMAT_HEADER = "[%(asctime)s][%(levelname)7s]"
LOG_FORMAT = LOG_FORMAT_HEADER + " %(message)s"


def create_shell_logging_config(level=logging.INFO):
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "shell": {"format": LOG_FORMAT},
        },
        "handlers": {
            "console": {
                "class": "airflow_db_logger.handlers.StreamHandler",
                "formatter": "shell",
                "level": logging.INFO,
            },
            "task": {
                "class": "airflow_db_logger.handlers.StreamHandler",
                "formatter": "shell",
                "level": logging.INFO,
            },
            "processor": {
                "class": "airflow_db_logger.handlers.StreamHandler",
                "formatter": "shell",
                "level": logging.INFO,
            },
        },
        "loggers": {
            "airflow.processor": {
                "handlers": ["processor"],
                "level": LOG_LEVEL,
                "propagate": False,
            },
            "airflow.task": {
                "handlers": ["task"],
                "level": LOG_LEVEL,
                "propagate": False,
            },
            "flask_appbuilder": {
                "handler": ["console"],
                "level": LOG_LEVEL,
                "propagate": True,
            },
        },
        "root": {
            "handlers": ["console"],
            "level": LOG_LEVEL,
        },
    }


LOGGING_CONFIG = create_shell_logging_config(logging.INFO)
