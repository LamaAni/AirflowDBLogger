import logging

LOG_FORMAT_HEADER = "[%(asctime)s][%(levelname)7s]"
LOG_FORMAT = LOG_FORMAT_HEADER + " %(message)s"


def create_shell_logging_config(
    level=logging.INFO,
    format: str = LOG_FORMAT,
    handler_class: str = "airflow_db_logger.handlers.StreamHandler",
):
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "shell": {"format": format},
        },
        "handlers": {
            "console": {
                "class": handler_class,
                "formatter": "shell",
            },
            "task": {
                "class": handler_class,
                "formatter": "shell",
            },
            "processor": {
                "class": handler_class,
                "formatter": "shell",
            },
        },
        "loggers": {
            "airflow.processor": {
                "handlers": ["processor"],
                "level": level,
                "propagate": False,
            },
            "airflow.task": {
                "handlers": ["task"],
                "level": level,
                "propagate": False,
                "filters": ["mask_secrets"],
            },
            "flask_appbuilder": {
                "handler": ["console"],
                "level": level,
                "propagate": True,
            },
        },
        "root": {
            "handlers": ["console"],
            "level": level,
            "filters": ["mask_secrets"],
        },
        "filters": {
            "mask_secrets": {
                "()": "airflow.utils.log.secrets_masker.SecretsMasker",
            }
        },
    }

    return config


SIMPLE_LOGGING_CONFIG = create_shell_logging_config(logging.INFO, handler_class="logging.StreamHandler")
LOGGING_CONFIG = create_shell_logging_config(logging.INFO)
