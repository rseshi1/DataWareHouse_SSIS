"""Q3 — EMPLOYEE_Q3 Type 6 dimension (versioned example).

SSIS package: ``Q3.dtsx``

Data Flow:
    [OLE DB Source EMPLOYEE_Q3]
        select ID,Name,City,Email,Schedule_Date from ASS3DB.dbo.EMPLOYEE_Q3
        WHERE Schedule_Date > ?
      -> [Lookup 'New Record']  reference = DW EMPLOYEE_Q3 where Active_Flag=1, join on ID
          |-- No Match -> [Derived 'Add Meta Data 1': F=1 (Active_Flag), V=1 (Version_No)]
          |              -> [Dest 1]  (insert new member, version 1)
          |-- Match    -> [Derived 'Add Meta Data': NotSameDay = Schedule_Date != ref.Schedule_Date]
                          -> [Conditional Split 'check if date changed ?']
                              |- Date changed (NotSameDay==1)
                              |     -> [OLE DB Cmd 'Set Active Flag 0'] (expire old row)
                              |     -> [Derived 'Add Meta Data 2': vnum=1, activeflag=1]
                              |     -> [Dest 2]
                              |- Date Not Changed (NotSameDay!=1)
                                    -> [OLE DB Cmd 'Set Active Flag 0 1'] (expire old row)
                                    -> [Derived 'Add Meta Data 3': vnum=Version_No+1, activeflag=1]
                                    -> [Dest 3]

Both OLE DB Commands run: UPDATE EMPLOYEE_Q3 SET Active_Flag=0 WHERE Emp_Key=?

NOTE (preserved exactly, flagged as an ambiguity in docs/Q3_migration.md):
  * BOTH match branches expire the current active row and insert a new active
    row — so a matched member is always re-versioned, even when the date did
    not change.
  * The Version_No assignment is counter-intuitive: a *changed* date resets
    Version_No to 1, while an *unchanged* date increments it (old + 1).  This
    mirrors the source package's Derived Column wiring 1:1.
"""
from __future__ import annotations

from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.audit import AuditLogger
from common.config import JobConfig
from common.io_utils import (
    append_delta,
    assign_surrogate_keys,
    current_max_key,
    read_delta,
    read_jdbc_query,
    table_exists,
)
from common.spark_session import get_spark

INSERT_COLUMNS = [
    "ID",
    "Name",
    "City",
    "Email",
    "Schedule_Date",
    "Active_Flag",
    "Version_No",
]


def build_changes(
    source_df: DataFrame,
    active_dim_df: DataFrame,
) -> Tuple[DataFrame, DataFrame]:
    """Compute (rows_to_insert, emp_keys_to_expire) for the Q3 Type 6 load."""
    s = source_df.alias("s")
    d = active_dim_df.alias("d")
    joined = s.join(d, on=F.col("s.ID") == F.col("d.ID"), how="left")

    is_match = F.col("d.ID").isNotNull()
    not_same_day = F.when(
        F.col("s.Schedule_Date") != F.col("d.Schedule_Date"), F.lit(1)
    ).otherwise(F.lit(0))

    enriched = joined.select(
        "s.*",
        F.col("d.Emp_Key").alias("d_Emp_Key"),
        F.col("d.Version_No").alias("d_Version_No"),
        is_match.alias("is_match"),
        not_same_day.alias("not_same_day"),
    )

    base_cols = [
        F.col("ID"),
        F.col("Name"),
        F.col("City"),
        F.col("Email"),
        F.col("Schedule_Date").cast("date").alias("Schedule_Date"),
        F.lit(1).alias("Active_Flag"),
    ]

    # New member (no match): version 1.
    new_rows = enriched.filter(~F.col("is_match")).select(
        *base_cols, F.lit(1).alias("Version_No")
    )

    matched = enriched.filter(F.col("is_match"))

    # Date changed: version reset to 1.
    changed_rows = matched.filter(F.col("not_same_day") == 1).select(
        *base_cols, F.lit(1).alias("Version_No")
    )

    # Date not changed: version incremented.
    unchanged_rows = matched.filter(F.col("not_same_day") != 1).select(
        *base_cols, (F.col("d_Version_No") + F.lit(1)).cast("int").alias("Version_No")
    )

    to_insert = new_rows.unionByName(changed_rows).unionByName(unchanged_rows)

    # Every matched member's current active row is expired by an OLE DB Command.
    to_expire = matched.select(
        F.col("d_Emp_Key").alias("Emp_Key")
    ).distinct()

    return to_insert, to_expire


def compute_watermark(spark: SparkSession, config: JobConfig) -> str:
    """Source filter is ``Schedule_Date > ?``.

    The package binds the parameter to an (undefined) variable, so the intended
    watermark is ambiguous (see docs/Q3_migration.md).  We mirror the Q2
    pattern: MAX(Schedule_Date) of active rows, coalesced to the configured
    initial watermark for first loads.
    """
    table = config.employee_q3.name
    if not table_exists(spark, table):
        return config.initial_load_watermark
    row = (
        read_delta(spark, table)
        .filter(F.col("Active_Flag") == 1)
        .agg(F.max("Schedule_Date").alias("wm"))
        .first()
    )
    if row and row["wm"] is not None:
        return str(row["wm"])
    return config.initial_load_watermark


def run(
    spark: SparkSession,
    config: JobConfig,
    run_date: Optional[str] = None,  # unused; kept for signature parity with Q2
) -> Tuple[int, int]:
    """Execute the Q3 Type 6 load. Returns (rows_inserted, rows_expired)."""
    from delta.tables import DeltaTable

    audit = AuditLogger(spark, package="Q3", audit_table=config.audit_table)
    target = config.employee_q3.name

    with audit.step("compute_watermark") as h:
        watermark = compute_watermark(spark, config)
        h.set_row_count(0)
        audit.log.info("watermark=%s", watermark)

    with audit.step("extract_source") as h:
        query = (
            "select ID, Name, City, Email, Schedule_Date "
            f"from EMPLOYEE_Q3 where Schedule_Date > '{watermark}'"
        )
        source_df = read_jdbc_query(spark, config.source, query)
        h.set_row_count(source_df.count())

    with audit.step("read_active_dim") as h:
        if table_exists(spark, target):
            active = read_delta(spark, target).filter(F.col("Active_Flag") == 1)
        else:
            active = spark.createDataFrame([], schema=_empty_dim_schema())
        h.set_row_count(active.count())

    with audit.step("transform_type6") as h:
        to_insert, to_expire = build_changes(source_df, active)
        # Detach lineage from the Delta table before the expire MERGE mutates it
        # (otherwise a recompute would read the post-expire snapshot).
        to_insert = to_insert.localCheckpoint(eager=True)
        to_expire = to_expire.localCheckpoint(eager=True)
        inserted = to_insert.count()
        expired = to_expire.count()
        h.set_row_count(inserted + expired)

    with audit.step("expire_old_rows") as h:
        if expired > 0 and table_exists(spark, target):
            dt = DeltaTable.forName(spark, target)
            (
                dt.alias("t")
                .merge(
                    to_expire.alias("x"),
                    "t.Emp_Key = x.Emp_Key and t.Active_Flag = 1",
                )
                .whenMatchedUpdate(set={"Active_Flag": F.lit(0)})
                .execute()
            )
        h.set_row_count(expired)

    with audit.step("insert_new_versions") as h:
        if inserted > 0:
            start_after = current_max_key(spark, target, "Emp_Key")
            keyed = assign_surrogate_keys(
                to_insert.select(*INSERT_COLUMNS), start_after, "Emp_Key"
            ).select("Emp_Key", *INSERT_COLUMNS)
            append_delta(keyed, target, config.employee_q3.path)
        h.set_row_count(inserted)

    return inserted, expired


def _empty_dim_schema() -> str:
    return (
        "Emp_Key int, ID int, Name string, City string, Email string, "
        "Schedule_Date date, Active_Flag int, Version_No int"
    )


if __name__ == "__main__":
    run(get_spark("Q3_employee_type6"), JobConfig.from_env())
