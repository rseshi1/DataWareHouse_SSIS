"""Unit + integration tests for Q3 (EMPLOYEE_Q3 Type 6 versioned SCD)."""
import datetime as dt

from pyspark.sql import Row

from common.config import DeltaTableConfig, JobConfig
from jobs import q3_employee_type6 as q3

ACTIVE_SCHEMA = (
    "Emp_Key int, ID int, Name string, City string, Email string, "
    "Schedule_Date date, Active_Flag int, Version_No int"
)
SRC_SCHEMA = "ID int, Name string, City string, Email string, Schedule_Date date"


def _active(spark, rows):
    return spark.createDataFrame(rows, schema=ACTIVE_SCHEMA)


def _src(spark, rows):
    return spark.createDataFrame(rows, schema=SRC_SCHEMA)


def _by_id(df):
    return {r["ID"]: r.asDict() for r in df.collect()}


def test_new_member_version_1(spark):
    src = _src(spark, [Row(5, "Eve", "Aswan", "e@x", dt.date(2024, 2, 1))])
    inserts, expire = q3.build_changes(src, _active(spark, []))
    assert expire.count() == 0
    r = _by_id(inserts)[5]
    assert r["Version_No"] == 1 and r["Active_Flag"] == 1


def test_date_changed_resets_version_to_1(spark):
    active = _active(spark, [Row(20, 1, "Al", "Cairo", "a@x",
                                 dt.date(2024, 1, 1), 1, 7)])
    src = _src(spark, [Row(1, "Al", "Cairo", "a@x", dt.date(2024, 3, 1))])
    inserts, expire = q3.build_changes(src, active)
    assert [r["Emp_Key"] for r in expire.collect()] == [20]
    r = _by_id(inserts)[1]
    # Preserved-as-is quirk: a changed date RESETS Version_No to 1.
    assert r["Version_No"] == 1
    assert r["Schedule_Date"] == dt.date(2024, 3, 1)


def test_date_unchanged_increments_version(spark):
    active = _active(spark, [Row(21, 1, "Al", "Cairo", "a@x",
                                 dt.date(2024, 1, 1), 1, 7)])
    src = _src(spark, [Row(1, "Al", "Cairo", "a@x", dt.date(2024, 1, 1))])
    inserts, expire = q3.build_changes(src, active)
    assert [r["Emp_Key"] for r in expire.collect()] == [21]
    r = _by_id(inserts)[1]
    # Preserved-as-is quirk: an UNCHANGED date INCREMENTS Version_No (7 -> 8).
    assert r["Version_No"] == 8


def test_end_to_end_run(spark, monkeypatch):
    spark.sql("DROP DATABASE IF EXISTS testdw CASCADE")
    spark.sql("CREATE DATABASE testdw")
    spark.sql(f"CREATE TABLE testdw.employee_q3 ({ACTIVE_SCHEMA}) USING DELTA")
    seed = _active(spark, [Row(1, 1, "Al", "Cairo", "a@x",
                               dt.date(2024, 1, 1), 1, 3)])
    seed.write.format("delta").mode("append").saveAsTable("testdw.employee_q3")

    # ID=1 unchanged date -> increment; ID=2 new member.
    src = _src(spark, [
        Row(1, "Al", "Cairo", "a@x", dt.date(2024, 1, 1)),
        Row(2, "Bob", "Luxor", "b@x", dt.date(2024, 2, 1)),
    ])
    monkeypatch.setattr(q3, "read_jdbc_query", lambda s, c, q: src)

    cfg = JobConfig(
        source=None,
        warehouse_jdbc=None,
        employee_q3=DeltaTableConfig("testdw.employee_q3"),
        audit_table=DeltaTableConfig("testdw.audit"),
        initial_load_watermark="1900-01-01",
    )
    # Force a full load by pretending the active watermark is the floor.
    monkeypatch.setattr(q3, "compute_watermark", lambda s, c: "1900-01-01")

    inserted, expired = q3.run(spark, cfg)
    assert inserted == 2 and expired == 1

    final = spark.table("testdw.employee_q3")
    assert final.filter("Emp_Key = 1").first()["Active_Flag"] == 0
    active_rows = {r["ID"]: r for r in final.filter("Active_Flag = 1").collect()}
    assert set(active_rows) == {1, 2}
    assert active_rows[1]["Version_No"] == 4  # 3 + 1
    assert active_rows[2]["Version_No"] == 1
    spark.sql("DROP DATABASE IF EXISTS testdw CASCADE")
