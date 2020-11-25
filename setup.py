#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))


def get_version():
    version_file_path = os.path.join(here, "package_version.txt")
    if not os.path.isfile(version_file_path):
        return "debug"
    version = None
    with open(version_file_path, "r") as raw:
        version = raw.read()

    return version


setup(
    name="airflow_db_logger",
    version=get_version(),
    description="An airflow logger that stores its results in a database given an SQLAlchemy connection.",
    long_description="Please see readme.md @ https://github.com/LamaAni/AirflowDBLogger",
    classifiers=[],
    author="Zav Shotan",
    author_email="",
    url="https://github.com/LamaAni/AirflowDBLogger",
    packages=["airflow_db_logger", "airflow_db_logger/writers"],
    platforms="any",
    license="docs/LICENSE",
    install_requires=[
        "sqlalchemy>=0.23.1",
        "zthreading>=0.1.15",
    ],
    python_requires=">=3.6",
    include_package_data=True,
)
