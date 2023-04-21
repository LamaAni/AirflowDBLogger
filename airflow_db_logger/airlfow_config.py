"""Overrides the airflow config getter to allow non circular loading
"""
import logging
from enum import Enum
from typing import Union, List, Type
from airflow.version import version as AIRFLOW_VERSION
from airflow.configuration import conf, AirflowConfigException, log

AIRFLOW_CONFIG_SECTION_NAME = "db_logger"
AIRFLOW_VERSION_PARTS = AIRFLOW_VERSION.split(".")
AIRFLOW_VERSION_PARTS = [int(v) for v in AIRFLOW_VERSION_PARTS]

AIRFLOW_MAJOR_VERSION = AIRFLOW_VERSION_PARTS[0]


def conf_get_no_warnings_no_errors(*args, **kwargs):
    old_level = log.level
    log.level = logging.ERROR
    try:
        val = conf.get(*args, **kwargs)
    except AirflowConfigException:
        val = None
    log.level = old_level
    return val


def __add_core(*collections):
    if AIRFLOW_MAJOR_VERSION < 1:
        collections = ["core", *collections]
    return collections


def get(
    key: str,
    default=None,
    otype: Type = None,
    allow_empty: bool = False,
    collection: Union[str, List[str]] = None,
):
    collection = collection or AIRFLOW_CONFIG_SECTION_NAME
    if isinstance(collection, str):
        collection = [collection]
    assert all(isinstance(v, str) for v in collection), ValueError("Collection must be a string or a list of strings")
    collection = [v for v in collection if len(v.strip()) > 0]
    assert len(collection) > 0, ValueError("Collection must be a non empty string or list of non empty strings")

    otype = otype or str if default is None else default.__class__
    for col in collection:
        val = conf_get_no_warnings_no_errors(col, key)
        if val is not None:
            break

    if issubclass(otype, Enum):
        allow_empty = False

    value_is_empty = val is None or (isinstance(val, str) and len(val.strip()) == 0)

    if not allow_empty and value_is_empty:
        assert default is not None, AirflowConfigException(
            f"Airflow configuration {collection}.{key} not found, and no default value"
        )
        return default

    if val is None:
        return None
    if otype == bool:
        return val.lower() == "true"
    elif issubclass(otype, Enum):
        val = val.strip()
        return otype(val.strip())
    else:
        return otype(val)
