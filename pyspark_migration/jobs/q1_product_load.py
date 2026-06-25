"""Q1 — JSON / REST API extraction and load to the warehouse.

SSIS package: ``Q1.dtsx``
  Data Flow Task
    [ZappySys JSON Source]  https://dummyjson.com/products/  (GET, MaxRows=25,
                            JsonFormat=Array, Filter=$.products[*])
        -> [OLE DB Destination]  ASS3DB_DW.dbo.Product_Q1

There is no Control Flow logic and no transformation between source and
destination — the eight selected JSON attributes are loaded as-is.  Business
logic preserved here:
  * only the first 25 records are kept (ZappySys MaxRows = 25);
  * exactly the eight mapped columns are written, with the warehouse data
    types (id/price/stock = BIGINT, discountPercentage/rating = DOUBLE).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.audit import AuditLogger
from common.config import JobConfig
from common.io_utils import append_delta, fetch_json, json_rows_to_df
from common.spark_session import get_spark

# Schema used to parse the raw JSON.  Decimal source values (e.g. price 9.99)
# are read as DOUBLE first, then converted to the BIGINT warehouse type in
# `transform` so we can apply SQL-Server-style rounding explicitly.
_JSON_PARSE_SCHEMA = (
    "id long, title string, price double, discountPercentage double, "
    "stock long, brand string, category string, rating double"
)

# Eight output columns in warehouse order/type (matches Product_Q1 DDL).
_OUTPUT_COLUMNS = [
    "id",
    "title",
    "price",
    "discountPercentage",
    "stock",
    "brand",
    "category",
    "rating",
]


def transform(df: DataFrame) -> DataFrame:
    """Project the eight mapped columns with warehouse data types.

    SQL Server CONVERT(bigint, <decimal>) rounds half-up; ``F.round`` + cast
    reproduces that so ``price`` matches what the OLE DB destination stored.
    """
    return df.select(
        F.col("id").cast("bigint").alias("id"),
        F.col("title").cast("string").alias("title"),
        F.round(F.col("price")).cast("bigint").alias("price"),
        F.col("discountPercentage").cast("double").alias("discountPercentage"),
        F.col("stock").cast("bigint").alias("stock"),
        F.col("brand").cast("string").alias("brand"),
        F.col("category").cast("string").alias("category"),
        F.col("rating").cast("double").alias("rating"),
    )


def run(
    spark: SparkSession,
    config: JobConfig,
    load_mode: str = "append",
) -> int:
    """Execute the Q1 load. Returns the number of rows written.

    ``load_mode``: ``append`` mirrors the SSIS OLE DB destination (insert only).
    Use ``overwrite`` to reproduce the operational ``TRUNCATE TABLE Product_Q1``
    documented in ``Query.sql`` before a full reload.
    """
    audit = AuditLogger(spark, package="Q1", audit_table=config.audit_table)
    target = config.product_q1

    with audit.step("extract_json") as h:
        rows = fetch_json(config.json_source)
        raw = json_rows_to_df(spark, rows, _JSON_PARSE_SCHEMA)
        h.set_row_count(len(rows))

    with audit.step("transform") as h:
        out = transform(raw)
        h.set_row_count(out.count())

    with audit.step("load_product_q1") as h:
        if load_mode == "overwrite":
            (
                out.select(*_OUTPUT_COLUMNS)
                .write.format("delta")
                .mode("overwrite")
                .saveAsTable(target.name)
            )
            written = out.count()
        else:
            written = append_delta(
                out.select(*_OUTPUT_COLUMNS), target.name, target.path
            )
        h.set_row_count(written)

    return written


if __name__ == "__main__":
    run(get_spark("Q1_product_load"), JobConfig.from_env())
