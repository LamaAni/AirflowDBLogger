import sys
import logging

LOG_FORMAT_HEADER = "[%(asctime)s][%(levelname)7s]"
LOG_FORMAT = LOG_FORMAT_HEADER + " %(message)s"


class StreamHandler(logging.StreamHandler):
    def __init__(
        self,
        stream: str = None,
        level: str = None,
        **kwargs,  # should actually accept anything since this is the override handler
    ) -> None:
        """General logging stream handler

        Args:
            stream (str, optional): The stream to write to. Defaults to None.
            level (str, optional): The log level. Defaults to None.
        """
        stream = stream or "stdout"
        self._use_stderr = "stderr" in stream
        logging.Handler.__init__(self, level=level or logging.INFO)

    @property
    def stream(self):
        if self._use_stderr:
            return sys.__stderr__

        return sys.__stdout__


def create_shell_logging_config(
    level=logging.INFO,
    format: str = LOG_FORMAT,
    handler_class: str = "logging.StreamHandler",
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
