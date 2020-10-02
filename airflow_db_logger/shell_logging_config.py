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
            "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
            "formatter": "shell",
            "stream": "sys.stdout",
        },
        "task": {
            "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
            "formatter": "shell",
            "stream": "sys.stdout",
        },
        "processor": {
            "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
            "formatter": "shell",
            "stream": "sys.stdout",
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
