import sys

LOG_LEVEL = "INFO"
LOG_FORMAT_HEADER = "[%(asctime)s][%(levelname)7s]"
LOG_FORMAT = LOG_FORMAT_HEADER + " %(message)s"
LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "shell": {"format": LOG_FORMAT},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "shell",
            "stream": sys.__stdout__,
        },
        "task": {
            "class": "logging.StreamHandler",
            "formatter": "shell",
            "stream": sys.__stdout__,
        },
        "processor": {
            "class": "logging.StreamHandler",
            "formatter": "shell",
            "stream": sys.__stdout__,
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
