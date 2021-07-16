# AirflowDBLogger (python package)

##### Remember: If you like it \* it, so other people would also use it.

An airflow logger that stores its results in a database given an SQLAlchemy connection.

Supports:
1. Airflow 1.10.x
2. Airflow 2.1.1
3. Write and read logs to db.
4. Logs cleanup operator

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
SQL_ALCHEMY_CONN=
SQL_ALCHEMY_SCHEMA=
SQL_ALCHEMY_POOL_ENABLED=True
SQL_ALCHEMY_POOL_SIZE=5
SQL_ALCHEMY_MAX_OVERFLOW=1
SQL_ALCHEMY_POOL_RECYCLE=1800
SQL_ALCHEMY_POOL_PRE_PING=True
SQL_ENGINE_ENCODING=utf-8
```

Or use the airflow builtin envs,

```shell
export AIRFLOW__DB_LOGGER_[config value name]="my_value"
```

# Config

Uses the airflow config, under the section `db_logger`. You can add a section to the airflow
configuration of apply these values using envs, like so,

| section                                  | description                                               | type/values | default                     |
| ---------------------------------------- | --------------------------------------------------------- | ----------- | --------------------------- |
| [db_logger].`SQL_ALCHEMY_CONN`           | The sqlalchemy connection string                          | `string`    | [core].`SQL_ALCHEMY_CONN`   |
| [db_logger].`SQL_ALCHEMY_CONN_ARGS`      | The sqlalchemy connection args                            | `string`    | None                        |
| [db_logger].`SQL_ALCHEMY_SCHEMA`         | The schema where to put the logging tables.               | `string`    | [core].`SQL_ALCHEMY_SCHEMA` |
| [db_logger].`SQL_ALCHEMY_POOL_ENABLED`   | If true enable sql alchemy pool                           | `boolean`   | True                        |
| [db_logger].`SQL_ALCHEMY_POOL_SIZE`      | The size of the sqlalchemy pool.                          | `int`       | 5                           |
| [db_logger].`SQL_ALCHEMY_MAX_OVERFLOW`   | The max overflow for sqlalchemy                           | `int`       | 1                           |
| [db_logger].`SQL_ALCHEMY_POOL_RECYCLE`   | The pool recycle time                                     | `int`       | 1800                        |
| [db_logger].`SQL_ALCHEMY_POOL_PRE_PING`  | If true, do a ping at the connection start.               | `boolean`   | true                        |
| [db_logger].`SQL_ENGINE_ENCODING`        | THe encoding for the sql engine                           | `string`    | utf-8                       |
|                                          |                                                           |             |
| [db_logger].`SHOW_REVERSE_ORDER`         | Show logs in reverse order in airflow log ui              | bool        | false                       |
| [db_logger].`CREATE_INDEXES`             | If true create db indexis                                 | bool        | false                       |
| [db_logger].`GOOGLE_APP_CREDS_PATH`      | The credentials file path for google bucket writing (gcs) | `string`    | None                        |
| [db_logger].`WRITE_TO_GCS_BUCKET`        | The gcs bucket to write to                                | `string`    | None                        |
| [db_logger].`WRITE_TO_GCS_PROJECT_ID`    | The gcs project to write to                               | `string`    | None                        |
| [db_logger].`WRITE_TO_FILES`             | If true, writes the log also to files                     | false       | None                        |
| [db_logger].`WRITE_TO_SHELL`             | Output the logs to shell as well                          | false       | None                        |
| [db_logger].`WRITE_DAG_PROCESSING_TO_DB` | Write all dag processing to database (a lot)              | `string`    | utf-8                       |
| [db_logger].`CONSOLE_FORMATTER`          | the formatter to use for teh console                      | `string`    | airflow_coloured            |
| [db_logger].`TASK_FORMATTER`             | the formatter to use for the task                         | `string`    | airflow                     |
| [db_logger].`PROCESSER_LOG_LEVEL`        | The log level to use for dag processing                   | `string`    | "WARN"                      |

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
