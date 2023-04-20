import os

DEFAULT_LOOPBACK_LOG_LEVEL = "ERROR"
DEFAULT_LOOPBACK_TASK_LOG_PATH = os.path.join(os.path.dirname(__file__), ".local", "tmp", "airflow_logs")
DEFAULT_LOOPBACK_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "[%(asctime)s][{%(filename)s:%(lineno)d}][%(levelname)s] %(message)s",
            "class": "airflow.utils.log.timezone_aware.TimezoneAware",
        },
    },
    "filters": {
        "mask_secrets": {
            "()": "airflow.utils.log.secrets_masker.SecretsMasker",
        }
    },
    "handlers": {
        "console": {
            "class": "airflow.utils.log.logging_mixin.RedirectStdHandler",
            "formatter": "simple",
            "stream": "sys.stdout",
            "filters": ["mask_secrets"],
        },
        "task": {
            "class": "airflow.utils.log.file_task_handler.FileTaskHandler",
            "formatter": "simple",
            "base_log_folder": f"{DEFAULT_LOOPBACK_TASK_LOG_PATH}",
            "filters": ["mask_secrets"],
        },
    },
    "loggers": {
        "airflow.processor": {
            "handlers": ["console"],
            "level": DEFAULT_LOOPBACK_LOG_LEVEL,
            "propagate": True,
        },
        "airflow.task": {
            "handlers": ["task"],
            "level": DEFAULT_LOOPBACK_LOG_LEVEL,
            "propagate": True,
            "filters": ["mask_secrets"],
        },
        "flask_appbuilder": {
            "handlers": ["console"],
            "level": "WARNING",
            "propagate": True,
        },
    },
    "root": {
        "handlers": ["console"],
        "level": DEFAULT_LOOPBACK_LOG_LEVEL,
        "filters": ["mask_secrets"],
    },
}
