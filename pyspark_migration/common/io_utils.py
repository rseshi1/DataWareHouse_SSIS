"""I/O helpers: JDBC read/write, REST/JSON ingestion and Delta utilities.

These replace the SSIS OLE DB Source/Destination components and the ZappySys
JSON Source.  Keeping them isolated makes the per-package jobs declarative and
easy to unit-test (the source/sink can be swapped for in-memory DataFrames).
"""
from __future__ import annotations

import json
import urllib.request
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from .config import JdbcConfig, JsonSourceConfig


# --------------------------------------------------------------------------- #
# SQL Server (OLE DB Source / Destination replacement)
# --------------------------------------------------------------------------- #
def read_jdbc_query(spark: SparkSession, cfg: JdbcConfig, query: str) -> DataFrame:
    """Run a SQL query against SQL Server and return a DataFrame.

    Equivalent to an OLE DB Source in 'SQL command' access mode.
    """
    return (
        spark.read.format("jdbc")
        .options(**cfg.reader_options())
        .option("query", query)
        .load()
    )


def write_jdbc_table(
    df: DataFrame, cfg: JdbcConfig, table: str, mode: str = "append"
) -> None:
    """Write a DataFrame to a SQL Server table (OLE DB Destination)."""
    (
        df.write.format("jdbc")
        .options(**cfg.reader_options())
        .option("dbtable", table)
        .mode(mode)
        .save()
    )


# --------------------------------------------------------------------------- #
# REST / JSON (ZappySys JSON Source replacement)
# --------------------------------------------------------------------------- #
def fetch_json(cfg: JsonSourceConfig) -> list[dict]:
    """Fetch a JSON document over HTTP and return the array of row objects.

    Reproduces the ZappySys JSON Source:
      * HttpRequestMethod  -> cfg.http_method
      * Filter $.<array>[*] -> cfg.array_path
      * MaxRows            -> cfg.max_rows (client-side slice)
    """
    req = urllib.request.Request(cfg.url, method=cfg.http_method)
    for key, value in cfg.headers.items():
        req.add_header(key, value)
    with urllib.request.urlopen(req, timeout=cfg.request_timeout_seconds) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    rows = payload[cfg.array_path] if cfg.array_path else payload
    if not isinstance(rows, list):
        raise ValueError(
            f"JSON path '{cfg.array_path}' did not resolve to an array"
        )
    if cfg.max_rows and cfg.max_rows > 0:
        rows = rows[: cfg.max_rows]
    return rows


def json_rows_to_df(
    spark: SparkSession, rows: list[dict], schema: str
) -> DataFrame:
    """Build a typed DataFrame from raw JSON dicts using an explicit schema.

    An explicit schema is used (rather than inference) so the output column
    types exactly match the SSIS metadata / warehouse DDL.
    """
    # Re-serialise so Spark performs the typed JSON parse with our schema.
    json_strings = [json.dumps(r) for r in rows]
    rdd = spark.sparkContext.parallelize(json_strings)
    return spark.read.schema(schema).json(rdd)


# --------------------------------------------------------------------------- #
# Delta helpers
# --------------------------------------------------------------------------- #
def table_exists(spark: SparkSession, name: str) -> bool:
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


def read_delta(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.format("delta").table(name)


def append_delta(df: DataFrame, name: str, path: Optional[str] = None) -> int:
    writer = df.write.format("delta").mode("append")
    if path:
        writer = writer.option("path", path)
    writer.saveAsTable(name)
    return df.count()


def current_max_key(spark: SparkSession, table: str, key_col: str) -> int:
    """Return the current MAX(key) for a table (0 if missing/empty)."""
    if not table_exists(spark, table):
        return 0
    row = read_delta(spark, table).agg(F.max(key_col).alias("m")).first()
    return int(row["m"]) if row and row["m"] is not None else 0


def assign_surrogate_keys(
    df: DataFrame, start_after: int, key_col: str = "Emp_Key"
) -> DataFrame:
    """Assign sequential surrogate keys starting at ``start_after + 1``.

    Reproduces SQL Server ``IDENTITY(1,1)`` behaviour for engines/tests where
    Delta identity columns are unavailable.
    """
    win = Window.orderBy(F.monotonically_increasing_id())
    return df.withColumn(
        key_col, (F.row_number().over(win) + F.lit(start_after)).cast("int")
    )
