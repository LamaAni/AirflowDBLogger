# AirflowDBLogger (python package)

##### Remember: If you like it \* it, so other people would also use it.

An airflow logger that stores its results in a database given an SQLAlchemy connection.

Supports:
1. Airflow 1.10.x (for AirflowDBLogger <= 2.0.3)
2. Airflow 2.5.3 (for AirflowDBLogger >= 2.0.5)
3. Write and read logs to db.
4. Logs cleanup operator


### Deprecation WARNING

Versions before 2.0.5 do not support the current logger config and classes. Class structure has changed
and configuration parameters have changed.

# Install

To install using pip @ https://pypi.org/project/airflow-db-logger/,

```shell
pip install airflow_db_logger
```

To install from master branch,

```shell
pip install git+https://github.com/LamaAni/AirflowDBLogger.git@master
```

To install from a release (tag)

```shell
pip install git+https://github.com/LamaAni/AirflowDBLogger.git@[tag]
```

# TL;DR

Add to airflow.cfg,

```ini
[core or logging(airflow 2)]
logging_config_class = airflow_db_logger.LOGGING_CONFIG

[db_logger]
# Defaults loaded from airflow config

log_level = INFO
processor_log_level = 

# Log flags
show_reverse_order = false 
show_log_splash = true

# Write control
write_dag_processing_to_db = false
# Auto defaults to true in DebugExecutor
write_to_shell = false 
# Internal default
log_splash =

# SQL Alchemy config
sql_alchemy_schema = 
sql_alchemy_connection = 
sql_alchemy_connection_args = 
sql_alchemy_create_indexes = 
sql_alchemy_pool_enabled = true
sql_alchemy_pool_size = 5
sql_alchemy_max_overflow = 1
sql_alchemy_pool_recycle = 1800
sql_alchemy_pool_pre_ping = true
sql_alchemy_engine_encoding = utf-8

# Logging handlers
# Add airflow default handlers (will write to files)
add_task_default_log_handler = false
add_processor_default_log_handler = false
# Override the default console handler
override_default_console_handler = true

# Override the default formatters
console_formatter = airflow_coloured
task_formatter = airflow
processor_formatter = aiflow

# Internal logger (logging status)
# Default: %%(blue)s[%%(asctime)s]%%(purple)s[airflow_db_logger]%%(log_color)s[%%(levelname)s]%%(reset)s %%(message)s
colored_log_format=
# Default: [%%(asctime)s][airflow_db_logger][%%(levelname)s] %%(message)s
log_format=
```

Or use the airflow builtin envs,

```shell
export AIRFLOW__DB_LOGGER_[config value name]="my_value"
```

# Maintenance

You can use the built in airflow operator to do cleanup. for example cleanup of logs older than 30 days,

```python

from airflow_db_logger.operators import AirflowDBLoggerCleanupOperator

...

with dag:
    AirflowDBLoggerCleanupOperator(
        task_id="db_log_cleanup",
        up_to=days_ago(30),
        since=None,
        include_operations_log=True,
        include_task_logs=True,
    )
```

This operator will delete old logs in the database, and reduce the DB size.

# Contributions

Are welcome, please post issues or PR's if needed.

## Implementations still missing

Add an issue (or better submit PR) if you need these.

1. Examples (other than TL;DR)

# License

Copyright ©
`Zav Shotan` and other [contributors](https://github.com/LamaAni/AirflowDBLogger/graphs/contributors).
It is free software, released under the MIT licence, and may be redistributed under the terms specified in [LICENSE](docs/LICENSE).
