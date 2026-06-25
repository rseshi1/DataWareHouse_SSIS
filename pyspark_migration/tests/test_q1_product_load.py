"""Unit tests for Q1 (JSON -> Product_Q1 load)."""
from common.config import DeltaTableConfig, JobConfig
from common.io_utils import json_rows_to_df
from jobs import q1_product_load as q1


def test_transform_types_and_rounding(spark):
    rows = [
        {
            "id": 1,
            "title": "iPhone 9",
            "price": 549.99,
            "discountPercentage": 12.96,
            "stock": 94,
            "brand": "Apple",
            "category": "smartphones",
            "rating": 4.69,
        }
    ]
    raw = json_rows_to_df(spark, rows, q1._JSON_PARSE_SCHEMA)
    out = q1.transform(raw).collect()[0]
    # price BIGINT with SQL-Server-style half-up rounding (549.99 -> 550).
    assert out["price"] == 550
    assert out["id"] == 1
    assert out["stock"] == 94
    assert abs(out["rating"] - 4.69) < 1e-9
    assert out["category"] == "smartphones"


def test_max_rows_slice_via_fetch(monkeypatch, spark):
    rows = [{"id": i, "title": f"p{i}", "price": i, "stock": i,
             "discountPercentage": 0.0, "brand": "b", "category": "c",
             "rating": 0.0} for i in range(40)]

    # fetch_json already applies max_rows; emulate that here.
    monkeypatch.setattr(q1, "fetch_json", lambda cfg: rows[: cfg.max_rows])

    cfg = JobConfig(
        source=None,
        warehouse_jdbc=None,
        product_q1=DeltaTableConfig("testdw.product_q1"),
        audit_table=DeltaTableConfig("testdw.audit"),
    )
    spark.sql("DROP DATABASE IF EXISTS testdw CASCADE")
    spark.sql("CREATE DATABASE testdw")
    written = q1.run(spark, cfg, load_mode="append")
    assert written == 25
    assert spark.table("testdw.product_q1").count() == 25
    spark.sql("DROP DATABASE IF EXISTS testdw CASCADE")
