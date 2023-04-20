#!/usr/bin/env python
from airflow_db_logger.logging_config import LOGGING_CONFIG, init_logging_config

# call to initialize logging configuration
init_logging_config()

if __name__ == "__main__":
    import json

    print(json.dumps(LOGGING_CONFIG, indent=2))
