# airflow_db_logger

An airflow logger that stores its results in a database given an SQLAlchemy connection.

## Configuration

Uses the airflow config, under the section `db_logger`. You can add a section to the airflow
configuration of apply these values using envs, like so,
```shell
export AIRFLOW__DB_LOGGER_[config value name]="my_value"
```

### Airflow config values

section | description |  type/values | default
---|---|---|---
[db_logger].`SQL_ALCHEMY_CONN` | The sqlalchemy connection string | `string` | [core].`SQL_ALCHEMY_CONN`
[db_logger].`SQL_ALCHEMY_SCHEMA` | The schema where to put the logging tables. | `string` | [core].`SQL_ALCHEMY_SCHEMA`
[db_logger].`SQL_ALCHEMY_POOL_ENABLED` | If true enable sql alchemy pool | `boolean` | True
[db_logger].`SQL_ALCHEMY_POOL_SIZE` | The size of the sqlalchemy pool. | `int` | 5
[db_logger].`SQL_ALCHEMY_MAX_OVERFLOW` | The max overflow for sqlalchemy | `int` | 1
[db_logger].`SQL_ALCHEMY_POOL_RECYCLE` | The pool recycle time | `int` | 1800
[db_logger].`SQL_ALCHEMY_POOL_PRE_PING` | If true, do a ping at the connection start. | `boolean` | true
[db_logger].`SQL_ENGINE_ENCODING` | THe encoding for the sql engine | `string` | utf-8
