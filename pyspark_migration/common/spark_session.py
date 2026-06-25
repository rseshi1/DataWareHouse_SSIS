"""Spark session helper.

On Databricks the active session is reused.  Locally (e.g. unit tests) a
Delta-enabled local session is created via ``delta-spark``'s configurator.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "ssis_migration") -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    try:
        from delta import configure_spark_with_delta_pip

        return configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception:
        # delta-spark not installed (e.g. already on a Delta-enabled cluster).
        return builder.getOrCreate()
