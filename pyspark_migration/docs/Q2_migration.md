# Q2.dtsx — Migration Document (SSIS → PySpark)

> Source package: `Q2.dtsx` · Target: `pyspark_migration/jobs/q2_employee_type6.py`

---

## 1. Executive Summary

`Q2.dtsx` implements a **Type 6 Slowly Changing Dimension** (a.k.a. SCD 1+2+3,
"hybrid") for the `EMPLOYEE_Q2` dimension. It performs an **incremental** extract
from the OLTP source `ASS3DB.dbo.EMPLOYEE_Q2` (only rows whose `Update_Date` is
newer than the last load), compares each incoming record against the **current
active** version in the warehouse `ASS3DB_DW.dbo.EMPLOYEE_Q2`, and routes changes
into one of four destinations depending on *which* attributes changed (City,
Email, both, or it is a brand-new member).

For changed members it applies the classic Type 6 pattern:
- **expire** the current active row (`Active_Flag = 0`, `ENDDate = today`), and
- **insert** a new active row where the changed attribute's previous value is
  preserved in a parallel `HESTORICAL*` column (Type 3 component) while the row
  itself is a new dated version (Type 2 component).

Complexity is **High** (lookup + two conditional splits + three OLE DB Commands +
four destinations + an Execute SQL Task watermark). The PySpark port reproduces
the routing with a single deterministic transformation (`build_changes`) and a
Delta **MERGE** (expire) + append (insert).

---

## 2. Package Flow Diagram

```
Control Flow
  [Execute SQL Task: "get max start date"]
      SELECT MAX(STARTDate) FROM EMPLOYEE_Q2 WHERE Active_Flag = 1   --> @watermark
              │ (precedence: success)
              ▼
  [Data Flow Task]

Data Flow Task
  [OLE DB Source "EMPLOYEE_Q2 Source"]
     select * from EMPLOYEE_Q2 where Update_Date > ?     (? = @watermark)
        │
        ▼
  [Lookup "New Record"]  ref = DW EMPLOYEE_Q2 WHERE Active_Flag=1, join on ID
     ├── No Match Output ──► [Derived "Add meta Data 1": date=GETDATE(), F=1]
     │                          └─► [OLE DB Dest: "...with no changes"]   (INSERT new member)
     │
     └── Match Output ─► [Conditional Split "Check ID": Source.ID == Lookup.ID]
                            └─► [Derived "Add Meta Data": NewCity, NewEmail]
                                  └─► [Conditional Split "Check changes"]
                                        ├── "change in column ciry"  : NewCity==1 && NewEmail!=1
                                        │     └► [OLE DB Cmd: expire] ─► [Derived "Add meta Data 3"] ─► [Dest: change in city]
                                        ├── "change in column email" : NewEmail==1 && NewCity!=1
                                        │     └► [OLE DB Cmd: expire] ─► [Derived "Add meta Data 2"] ─► [Dest: change in email]
                                        └── "change in both"         : NewEmail==1 && NewCity==1
                                              └► [OLE DB Cmd: expire] ─► [Derived "Add meta Data 2 1"] ─► [Dest: change in email and city]
```

All three OLE DB Commands execute:
```sql
update EMPLOYEE_Q2 set Active_Flag = 0, ENDDate = GETDATE() where Emp_Key = ?
```

---

## 3. Control Flow Mapping

| SSIS object | Behaviour | PySpark equivalent |
|---|---|---|
| Execute SQL Task `MAX(STARTDate) … Active_Flag=1` | Computes incremental watermark | `q2.compute_watermark()` |
| Precedence constraint (Success) | Run data flow after watermark | sequential calls in `run()` |
| Data Flow Task | The SCD pipeline | `q2.run()` body |

---

## 4. Data Flow Mapping

| SSIS component | Type | PySpark equivalent |
|---|---|---|
| `EMPLOYEE_Q2 Source` (`Update_Date > ?`) | OLE DB Source | `read_jdbc_query(... where Update_Date > '<wm>')` |
| `New Record` (Lookup, `Active_Flag=1`, on `ID`) | Lookup | `source.join(active_dim, on ID, how="left")` |
| `Check ID` (Conditional Split) | Conditional Split | redundant guard (`ID == ID`), collapsed into the join's match flag |
| `Add Meta Data` (Derived) | Derived Column | `new_city`, `new_email` flags in `build_changes` |
| `Check changes` (Conditional Split) | Conditional Split | `.filter(...)` for city / email / both branches |
| `Add meta Data 1/2/3/2 1` (Derived) | Derived Column | per-branch `select(...)` literals |
| `change in ciry/email/...` OLE DB Commands | OLE DB Command | Delta `MERGE … whenMatchedUpdate(Active_Flag=0, ENDDate)` |
| 4 × `EMPLOYEE_Q2 Destination …` | OLE DB Destination | `append_delta` of the unioned insert set |

---

## 5. Variable Mapping

| SSIS variable / parameter | Purpose | PySpark equivalent |
|---|---|---|
| Execute SQL Task result → source parameter `?` | Incremental watermark (max active `STARTDate`) | return value of `compute_watermark()` → injected into the source SQL |
| `System::*` (package metadata) | SSIS housekeeping | not required |

> **Note:** the `.dtsx` binds the source `?` parameter to a package variable that
> receives the Execute SQL Task result. The result-set binding is implicit in the
> package; we make it explicit and configurable via `JobConfig`.

---

## 6. Connection Mapping

| SSIS Connection Manager | Type | PySpark config |
|---|---|---|
| `DESKTOP-HREK7MN.ASS3DB` | OLE DB (source OLTP) | `JobConfig.source` (`JdbcConfig`, DB `ASS3DB`) |
| `DESKTOP-HREK7MN.ASS3DB_DW` | OLE DB (warehouse) | `JobConfig.employee_q2` (Delta `ass3db_dw.employee_q2`) |

---

## 7. Transformation Mapping

### Source schema (`ASS3DB.dbo.EMPLOYEE_Q2`)
`ID int`, `Name nvarchar(255)`, `City nvarchar(255)`, `Email nvarchar(255)`,
`Update_Date date`.

### Warehouse schema (`ASS3DB_DW.dbo.EMPLOYEE_Q2`)
`Emp_Key int IDENTITY (PK)`, `ID int`, `Name`, `CURRENTCity`, `HESTORICALCity`,
`CURRENTEmail`, `HESTORICALEmail`, `STARTDate date`, `ENDDate date`, `Active_Flag int`.
*(`HESTORICAL*` spelling is taken verbatim from the source warehouse.)*

### Derived column expressions (preserved exactly)
| Derived component | Expression | Output |
|---|---|---|
| `Add Meta Data` | `(City != [New Record].CURRENTCity) ? 1 : 0` | `NewCity` |
| `Add Meta Data` | `(Email != [New Record].CURRENTEmail) ? 1 : 0` | `NewEmail` |
| `Add meta Data 1` | `GETDATE()`, `1` | `date`, `F` |
| `Add meta Data 2 / 2 1 / 3` | `GETDATE()`, `1` | `StartData`, `Flag` |

> NULL handling: in SSIS, `!=` against NULL yields NULL → the row falls through
> to the default (no change). `build_changes._changed` reproduces this
> (`when(a != b, 1).otherwise(0)` ⇒ `0` when either side is NULL).

### Destination column mappings (exact, extracted from `Q2.dtsx`)

**"no changes" (new member, Lookup No-Match):**
| Target | Source |
|---|---|
| ID, Name | Source.ID, Source.Name |
| CURRENTCity | Source.City |
| CURRENTEmail | Source.Email |
| HESTORICALCity, HESTORICALEmail | *(unmapped → NULL)* |
| STARTDate | **Source.Update_Date** |
| ENDDate | *(unmapped → NULL)* |
| Active_Flag | `F` (= 1) |

**"change in city" (`NewCity==1 && NewEmail!=1`):**
| Target | Source |
|---|---|
| CURRENTCity | Source.City *(new)* |
| HESTORICALCity | Lookup.CURRENTCity *(previous current)* |
| CURRENTEmail | Source.Email |
| HESTORICALEmail | Lookup.HESTORICALEmail *(carried forward)* |
| STARTDate | `GETDATE()` (`Add meta Data 3.StartData`) |
| Active_Flag | `1` |

**"change in email" (`NewEmail==1 && NewCity!=1`):**
| Target | Source |
|---|---|
| CURRENTCity | Source.City |
| HESTORICALCity | Lookup.HESTORICALCity *(carried forward)* |
| CURRENTEmail | Source.Email *(new)* |
| HESTORICALEmail | Lookup.CURRENTEmail *(previous current)* |
| STARTDate | `GETDATE()` (`Add meta Data 2.StartData`) |
| Active_Flag | `1` |

**"change in email and city" (`NewEmail==1 && NewCity==1`):**
| Target | Source |
|---|---|
| CURRENTCity | Source.City *(new)* |
| HESTORICALCity | Lookup.CURRENTCity *(previous current)* |
| CURRENTEmail | Source.Email *(new)* |
| HESTORICALEmail | Lookup.CURRENTEmail *(previous current)* |
| STARTDate | `GETDATE()` (`Add meta Data 2 1.StartData`) |
| Active_Flag | `1` |

> **Undocumented transformation captured:** the `Add meta Data 1.date` derived
> column (`GETDATE()`) is computed but **not** used by the destination — the new
> member's `STARTDate` comes from `Source.Update_Date`, not `GETDATE()`. This is
> preserved exactly (`new_rows.STARTDate = Update_Date`).

> **GETDATE() → DATE:** `STARTDate`/`ENDDate` are `DATE`, so SQL Server truncates
> `GETDATE()` to the date. We use `current_date()` (overridable via
> `run(run_date=...)` for deterministic runs/tests).

---

## 8. Error Handling Mapping

| SSIS behaviour | PySpark behaviour |
|---|---|
| OLE DB Command input `errorRowDisposition = FailComponent` | MERGE is atomic; failure aborts the run (logged + re-raised). |
| OLE DB Destination `FailComponent` | append is transactional in Delta; a bad batch fails the job. |
| Lookup No-Match → separate output (not an error) | `left join` + null-key branch (new member). |
| SSIS execution log | `AuditLogger` step rows: `compute_watermark`, `extract_source`, `read_active_dim`, `transform_type6`, `expire_old_rows`, `insert_new_versions`. |

---

## 9. PySpark Implementation

See **`pyspark_migration/jobs/q2_employee_type6.py`**. Core algorithm:

```python
watermark   = compute_watermark(spark, config)          # MAX(STARTDate) active
source      = read_jdbc_query(... f"Update_Date > '{watermark}'")
active      = read_delta(target).filter("Active_Flag = 1")
to_insert, to_expire = build_changes(source, active, run_date)   # pure, testable

# expire current active rows of changed members (the 3 OLE DB Commands)
DeltaTable.forName(target).alias("t").merge(
    to_expire.alias("x"), "t.Emp_Key = x.Emp_Key and t.Active_Flag = 1"
).whenMatchedUpdate(set={"Active_Flag": 0, "ENDDate": run_date}).execute()

# insert new active versions (the 4 destinations, unioned)
append_delta(assign_surrogate_keys(to_insert, max_key), target)
```

`build_changes` is engine-independent and unit-tested in isolation; `run()`
handles I/O, watermarking, surrogate-key assignment, and audit logging.
`to_insert`/`to_expire` are `localCheckpoint`-ed before the MERGE so mutating the
table cannot retroactively change the computed change set.

---

## 10. Unit Test Cases

See **`pyspark_migration/tests/test_q2_employee_type6.py`**:

| Test | Scenario | Asserts |
|---|---|---|
| `test_new_member_…` | Lookup no-match | inserts with `STARTDate = Update_Date`, NULL history, no expiry |
| `test_city_change` | City only | `HESTORICALCity = old CURRENTCity`, email carried, 1 expiry |
| `test_email_change` | Email only | `HESTORICALEmail = old CURRENTEmail`, city carried |
| `test_both_change` | City + Email | both `HESTORICAL*` = old `CURRENT*` |
| `test_no_change_is_dropped` | No change | zero inserts, zero expiries |
| `test_end_to_end_run` | Seeded Delta table | old row expired (`Active_Flag=0`, `ENDDate`); exactly one active row per ID; new surrogate key |

---

## 11. Data Validation Queries

See **`sql/validation_queries.sql`** (section Q2):
- `V2.1` ≤ 1 active row per `ID`
- `V2.2` active ⇒ `ENDDate IS NULL`; inactive ⇒ `ENDDate IS NOT NULL`
- `V2.3` Type 6 history integrity (changed attribute's previous value captured)
- `V2.4` `Emp_Key` uniqueness

---

## 12. Deployment Instructions

1. **DDL:** run `sql/ddl_delta_tables.sql` (creates `employee_q2`, `etl_audit_log`).
2. **Secrets/config:** provide source + warehouse JDBC via env vars
   (`SRC_HOST/DB/USER/PASSWORD`, `DW_*`) or a `JobConfig`. On Databricks pull
   credentials from a secret scope; never inline them.
3. **JDBC driver:** ensure the MS SQL Server JDBC driver is on the cluster
   (`com.microsoft.sqlserver:mssql-jdbc`).
4. **Initial load:** an empty active dimension yields a NULL watermark; the job
   coalesces it to `JobConfig.initial_load_watermark` (`1900-01-01`) so the dim
   can be seeded. (See Ambiguities.)
5. **Schedule:** run as a Databricks Job task `jobs/q2_employee_type6.py`; the
   watermark makes each run incremental and idempotent w.r.t. already-loaded rows.
6. **Validate:** run Q2 validation queries; inspect `etl_audit_log`.

### Assumptions & Ambiguities (Q2)
- **Watermark on empty dim:** literal SSIS would compare `Update_Date > NULL`
  and load **zero** rows. We coalesce to `initial_load_watermark` to enable the
  first load — a deliberate, documented enhancement. Set it to a future date to
  reproduce the strict zero-row behaviour.
- **`Check ID` conditional split** (`Source.ID == Lookup.ID`) is redundant after
  a matched lookup; collapsed into the join match flag.
- **`Add meta Data 1.date`** (`GETDATE()`) is unused by the destination (new
  members get `STARTDate = Update_Date`); preserved exactly.
- Target is Delta Lake; `Emp_Key` assigned in code for portability (swap to
  `GENERATED ALWAYS AS IDENTITY` on Databricks if preferred).
- `GETDATE()` truncated to `DATE` to match the column type.
