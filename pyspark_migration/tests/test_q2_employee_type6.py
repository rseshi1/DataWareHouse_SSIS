"""Unit + integration tests for Q2 (EMPLOYEE_Q2 Type 6 SCD)."""
import datetime as dt

from pyspark.sql import Row

from common.config import DeltaTableConfig, JobConfig
from jobs import q2_employee_type6 as q2

ACTIVE_SCHEMA = (
    "Emp_Key int, ID int, Name string, CURRENTCity string, HESTORICALCity string, "
    "CURRENTEmail string, HESTORICALEmail string, STARTDate date, ENDDate date, "
    "Active_Flag int"
)
SRC_SCHEMA = "ID int, Name string, City string, Email string, Update_Date date"
RUN_DATE = dt.date(2024, 1, 15)


def _active(spark, rows):
    return spark.createDataFrame(rows, schema=ACTIVE_SCHEMA)


def _src(spark, rows):
    return spark.createDataFrame(rows, schema=SRC_SCHEMA)


def _by_id(df):
    return {r["ID"]: r.asDict() for r in df.collect()}


def test_new_member_uses_update_date_and_null_history(spark):
    src = _src(spark, [Row(2, "Bob", "Luxor", "b@x", dt.date(2024, 1, 10))])
    active = _active(spark, [])
    inserts, expire = q2.build_changes(src, active, RUN_DATE)
    rows = _by_id(inserts)
    assert expire.count() == 0
    r = rows[2]
    assert r["CURRENTCity"] == "Luxor" and r["HESTORICALCity"] is None
    assert r["CURRENTEmail"] == "b@x" and r["HESTORICALEmail"] is None
    assert r["STARTDate"] == dt.date(2024, 1, 10)  # source Update_Date, not run date
    assert r["Active_Flag"] == 1


def test_city_change(spark):
    active = _active(spark, [Row(10, 1, "Al", "Cairo", "oldc", "a@x", "oldmail",
                                 dt.date(2020, 1, 1), None, 1)])
    src = _src(spark, [Row(1, "Al", "Giza", "a@x", dt.date(2024, 1, 12))])
    inserts, expire = q2.build_changes(src, active, RUN_DATE)
    assert [r["Emp_Key"] for r in expire.collect()] == [10]
    r = _by_id(inserts)[1]
    assert r["CURRENTCity"] == "Giza"
    assert r["HESTORICALCity"] == "Cairo"      # previous current pushed to history
    assert r["CURRENTEmail"] == "a@x"
    assert r["HESTORICALEmail"] == "oldmail"   # carried forward unchanged
    assert r["STARTDate"] == RUN_DATE
    assert r["Active_Flag"] == 1


def test_email_change(spark):
    active = _active(spark, [Row(11, 1, "Al", "Cairo", "oldc", "a@x", "oldmail",
                                 dt.date(2020, 1, 1), None, 1)])
    src = _src(spark, [Row(1, "Al", "Cairo", "new@x", dt.date(2024, 1, 12))])
    inserts, expire = q2.build_changes(src, active, RUN_DATE)
    assert [r["Emp_Key"] for r in expire.collect()] == [11]
    r = _by_id(inserts)[1]
    assert r["CURRENTCity"] == "Cairo"
    assert r["HESTORICALCity"] == "oldc"       # carried forward unchanged
    assert r["CURRENTEmail"] == "new@x"
    assert r["HESTORICALEmail"] == "a@x"        # previous current pushed to history
    assert r["STARTDate"] == RUN_DATE


def test_both_change(spark):
    active = _active(spark, [Row(12, 1, "Al", "Cairo", "oldc", "a@x", "oldmail",
                                 dt.date(2020, 1, 1), None, 1)])
    src = _src(spark, [Row(1, "Al", "Giza", "new@x", dt.date(2024, 1, 12))])
    inserts, expire = q2.build_changes(src, active, RUN_DATE)
    assert [r["Emp_Key"] for r in expire.collect()] == [12]
    r = _by_id(inserts)[1]
    assert r["CURRENTCity"] == "Giza" and r["HESTORICALCity"] == "Cairo"
    assert r["CURRENTEmail"] == "new@x" and r["HESTORICALEmail"] == "a@x"


def test_no_change_is_dropped(spark):
    active = _active(spark, [Row(13, 1, "Al", "Cairo", "oldc", "a@x", "oldmail",
                                 dt.date(2020, 1, 1), None, 1)])
    src = _src(spark, [Row(1, "Al", "Cairo", "a@x", dt.date(2024, 1, 12))])
    inserts, expire = q2.build_changes(src, active, RUN_DATE)
    assert inserts.count() == 0
    assert expire.count() == 0


def test_end_to_end_run(spark, monkeypatch):
    spark.sql("DROP DATABASE IF EXISTS testdw CASCADE")
    spark.sql("CREATE DATABASE testdw")
    spark.sql(
        f"CREATE TABLE testdw.employee_q2 ({ACTIVE_SCHEMA}) USING DELTA"
    )
    # Seed one active member (ID=1, Emp_Key=1).
    seed = _active(spark, [Row(1, 1, "Al", "Cairo", None, "a@x", None,
                               dt.date(2020, 1, 1), None, 1)])
    seed.write.format("delta").mode("append").saveAsTable("testdw.employee_q2")

    # Incoming: ID=1 city change + ID=2 new member.
    src = _src(spark, [
        Row(1, "Al", "Giza", "a@x", dt.date(2024, 1, 12)),
        Row(2, "Bob", "Luxor", "b@x", dt.date(2024, 1, 13)),
    ])
    monkeypatch.setattr(q2, "read_jdbc_query", lambda s, c, q: src)

    cfg = JobConfig(
        source=None,
        warehouse_jdbc=None,
        employee_q2=DeltaTableConfig("testdw.employee_q2"),
        audit_table=DeltaTableConfig("testdw.audit"),
    )
    inserted, expired = q2.run(spark, cfg, run_date="2024-01-15")
    assert inserted == 2 and expired == 1

    final = spark.table("testdw.employee_q2")
    # Original ID=1 row expired.
    old = final.filter("Emp_Key = 1").first()
    assert old["Active_Flag"] == 0 and old["ENDDate"] == dt.date(2024, 1, 15)
    # Exactly one active row per ID afterwards.
    active_ids = {r["ID"]: r for r in final.filter("Active_Flag = 1").collect()}
    assert set(active_ids) == {1, 2}
    assert active_ids[1]["CURRENTCity"] == "Giza"
    assert active_ids[1]["HESTORICALCity"] == "Cairo"
    assert active_ids[1]["Emp_Key"] > 1  # new surrogate key
    spark.sql("DROP DATABASE IF EXISTS testdw CASCADE")
