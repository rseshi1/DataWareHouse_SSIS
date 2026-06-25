"""Pytest fixtures: a Delta-enabled local Spark session and a temp warehouse."""
import os
import shutil
import sys
import tempfile

import pytest

# Make the project root importable (common/, jobs/).
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    warehouse = tempfile.mkdtemp(prefix="spark_wh_")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ssis_migration_tests")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    from delta import configure_spark_with_delta_pip

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture()
def db(spark):
    """A fresh schema per test, with the warehouse DDL applied."""
    name = "testdw"
    spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE")
    spark.sql(f"CREATE DATABASE {name}")
    yield name
    spark.sql(f"DROP DATABASE IF EXISTS {name} CASCADE")
