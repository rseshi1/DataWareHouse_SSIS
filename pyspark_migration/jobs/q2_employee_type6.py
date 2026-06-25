"""Q2 — EMPLOYEE_Q2 Type 6 slowly changing dimension.

SSIS package: ``Q2.dtsx``

Control Flow:
    [Execute SQL Task]  SELECT MAX(STARTDate) FROM EMPLOYEE_Q2 WHERE Active_Flag=1
        --(precedence)--> [Data Flow Task]

Data Flow:
    [OLE DB Source EMPLOYEE_Q2]  select * from EMPLOYEE_Q2 where Update_Date > ?
        -> [Lookup 'New Record']  reference = DW EMPLOYEE_Q2 where Active_Flag=1, join on ID
            |-- No Match  -> [Derived 'Add meta Data 1']        -> [Dest: no changes]
            |-- Match     -> [Conditional Split 'Check ID': Source.ID == Lookup.ID]
                              -> [Derived 'Add Meta Data': NewCity, NewEmail flags]
                                 -> [Conditional Split 'Check changes']
                                     |- city  : NewCity==1 && NewEmail!=1
                                     |          -> [OLE DB Cmd: expire old row] -> [Dest: change in city]
                                     |- email : NewEmail==1 && NewCity!=1
                                     |          -> [OLE DB Cmd: expire old row] -> [Dest: change in email]
                                     |- both  : NewEmail==1 && NewCity==1
                                                -> [OLE DB Cmd: expire old row] -> [Dest: change in email and city]

The three OLE DB Commands all run:
    UPDATE EMPLOYEE_Q2 SET Active_Flag=0, ENDDate=GETDATE() WHERE Emp_Key=?

Type 6 semantics preserved exactly:
  * a changed dimension member closes its current active row (Active_Flag=0,
    ENDDate=today) and inserts a NEW active row;
  * the new row keeps the CURRENT* value = incoming value and pushes the prior
    value into the matching HESTORICAL* column (only for the attribute(s) that
    changed; the other HESTORICAL* value is carried forward unchanged);
  * brand-new members are inserted with STARTDate = source Update_Date and
    NULL historical columns (faithful to the 'no changes' destination mapping).
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

# Output column order for the EMPLOYEE_Q2 dimension (excluding identity Emp_Key).
INSERT_COLUMNS = [
    "ID",
    "Name",
    "CURRENTCity",
    "HESTORICALCity",
    "CURRENTEmail",
    "HESTORICALEmail",
    "STARTDate",
    "ENDDate",
    "Active_Flag",
]


def _changed(incoming, current) -> "F.Column":
    """SSIS '(a != b) ? 1 : 0' — NULL comparison yields 'no change' (0)."""
    return F.when(incoming != current, F.lit(1)).otherwise(F.lit(0))


def build_changes(
    source_df: DataFrame,
    active_dim_df: DataFrame,
    run_date,
) -> Tuple[DataFrame, DataFrame]:
    """Compute (rows_to_insert, emp_keys_to_expire) for the Type 6 merge.

    ``source_df``      : ID, Name, City, Email, Update_Date
    ``active_dim_df``  : current active dimension rows (Active_Flag = 1)
    ``run_date``       : date literal used for STARTDate/ENDDate on changes
                         (replaces SSIS GETDATE()).
    """
    s = source_df.alias("s")
    d = active_dim_df.alias("d")
    joined = s.join(d, on=F.col("s.ID") == F.col("d.ID"), how="left")

    is_match = F.col("d.ID").isNotNull()
    new_city = _changed(F.col("s.City"), F.col("d.CURRENTCity"))
    new_email = _changed(F.col("s.Email"), F.col("d.CURRENTEmail"))

    enriched = joined.select(
        "s.*",
        F.col("d.Emp_Key").alias("d_Emp_Key"),
        F.col("d.CURRENTCity").alias("d_CURRENTCity"),
        F.col("d.HESTORICALCity").alias("d_HESTORICALCity"),
        F.col("d.CURRENTEmail").alias("d_CURRENTEmail"),
        F.col("d.HESTORICALEmail").alias("d_HESTORICALEmail"),
        is_match.alias("is_match"),
        new_city.alias("new_city"),
        new_email.alias("new_email"),
    )

    run_date_col = F.lit(run_date).cast("date")

    # --- brand new members (Lookup No Match -> 'no changes' destination) ----
    new_rows = enriched.filter(~F.col("is_match")).select(
        F.col("ID"),
        F.col("Name"),
        F.col("City").alias("CURRENTCity"),
        F.lit(None).cast("string").alias("HESTORICALCity"),
        F.col("Email").alias("CURRENTEmail"),
        F.lit(None).cast("string").alias("HESTORICALEmail"),
        F.col("Update_Date").cast("date").alias("STARTDate"),
        F.lit(None).cast("date").alias("ENDDate"),
        F.lit(1).alias("Active_Flag"),
    )

    matched = enriched.filter(F.col("is_match"))

    # --- change in city only -----------------------------------------------
    city_rows = matched.filter(
        (F.col("new_city") == 1) & (F.col("new_email") != 1)
    ).select(
        F.col("ID"),
        F.col("Name"),
        F.col("City").alias("CURRENTCity"),
        F.col("d_CURRENTCity").alias("HESTORICALCity"),
        F.col("Email").alias("CURRENTEmail"),
        F.col("d_HESTORICALEmail").alias("HESTORICALEmail"),
        run_date_col.alias("STARTDate"),
        F.lit(None).cast("date").alias("ENDDate"),
        F.lit(1).alias("Active_Flag"),
    )

    # --- change in email only ----------------------------------------------
    email_rows = matched.filter(
        (F.col("new_email") == 1) & (F.col("new_city") != 1)
    ).select(
        F.col("ID"),
        F.col("Name"),
        F.col("City").alias("CURRENTCity"),
        F.col("d_HESTORICALCity").alias("HESTORICALCity"),
        F.col("Email").alias("CURRENTEmail"),
        F.col("d_CURRENTEmail").alias("HESTORICALEmail"),
        run_date_col.alias("STARTDate"),
        F.lit(None).cast("date").alias("ENDDate"),
        F.lit(1).alias("Active_Flag"),
    )

    # --- change in both -----------------------------------------------------
    both_rows = matched.filter(
        (F.col("new_email") == 1) & (F.col("new_city") == 1)
    ).select(
        F.col("ID"),
        F.col("Name"),
        F.col("City").alias("CURRENTCity"),
        F.col("d_CURRENTCity").alias("HESTORICALCity"),
        F.col("Email").alias("CURRENTEmail"),
        F.col("d_CURRENTEmail").alias("HESTORICALEmail"),
        run_date_col.alias("STARTDate"),
        F.lit(None).cast("date").alias("ENDDate"),
        F.lit(1).alias("Active_Flag"),
    )

    to_insert = new_rows.unionByName(city_rows).unionByName(email_rows).unionByName(
        both_rows
    )

    # Emp_Keys to expire = matched members with any change.
    to_expire = (
        matched.filter((F.col("new_city") == 1) | (F.col("new_email") == 1))
        .select(F.col("d_Emp_Key").alias("Emp_Key"))
        .distinct()
    )

    return to_insert, to_expire


def compute_watermark(spark: SparkSession, config: JobConfig) -> str:
    """SSIS Execute SQL Task: MAX(STARTDate) of active rows.

    NULL (empty active dimension) is coalesced to ``initial_load_watermark`` so
    a first load is possible; the literal SSIS package would load zero rows in
    that situation (``Update_Date > NULL`` is UNKNOWN).
    """
    table = config.employee_q2.name
    if not table_exists(spark, table):
        return config.initial_load_watermark
    row = (
        read_delta(spark, table)
        .filter(F.col("Active_Flag") == 1)
        .agg(F.max("STARTDate").alias("wm"))
        .first()
    )
    if row and row["wm"] is not None:
        return str(row["wm"])
    return config.initial_load_watermark


def run(
    spark: SparkSession,
    config: JobConfig,
    run_date: Optional[str] = None,
) -> Tuple[int, int]:
    """Execute the Q2 Type 6 load. Returns (rows_inserted, rows_expired)."""
    from delta.tables import DeltaTable

    audit = AuditLogger(spark, package="Q2", audit_table=config.audit_table)
    target = config.employee_q2.name
    # Concrete date for STARTDate/ENDDate (SSIS GETDATE()); deterministic per run.
    effective_date = run_date or str(spark.sql("select current_date() d").first()["d"])

    with audit.step("compute_watermark") as h:
        watermark = compute_watermark(spark, config)
        h.set_row_count(0)
        audit.log.info("watermark=%s", watermark)

    with audit.step("extract_source") as h:
        query = (
            "select ID, Name, City, Email, Update_Date "
            f"from EMPLOYEE_Q2 where Update_Date > '{watermark}'"
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
        to_insert, to_expire = build_changes(source_df, active, effective_date)
        # Detach lineage from the Delta table: the expire MERGE below mutates
        # the table and would otherwise force a recompute against the new
        # snapshot (dropping the just-expired rows from the active lookup).
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
                .whenMatchedUpdate(
                    set={
                        "Active_Flag": F.lit(0),
                        "ENDDate": F.lit(effective_date).cast("date"),
                    }
                )
                .execute()
            )
        h.set_row_count(expired)

    with audit.step("insert_new_versions") as h:
        if inserted > 0:
            start_after = current_max_key(spark, target, "Emp_Key")
            keyed = assign_surrogate_keys(
                to_insert.select(*INSERT_COLUMNS), start_after, "Emp_Key"
            ).select("Emp_Key", *INSERT_COLUMNS)
            append_delta(keyed, target, config.employee_q2.path)
        h.set_row_count(inserted)

    return inserted, expired


def _empty_dim_schema() -> str:
    return (
        "Emp_Key int, ID int, Name string, CURRENTCity string, "
        "HESTORICALCity string, CURRENTEmail string, HESTORICALEmail string, "
        "STARTDate date, ENDDate date, Active_Flag int"
    )


if __name__ == "__main__":
    run(get_spark("Q2_employee_type6"), JobConfig.from_env())
