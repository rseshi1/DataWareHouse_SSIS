# Q3.dtsx — Migration Document (SSIS → PySpark)

> Source package: `Q3.dtsx` · Target: `pyspark_migration/jobs/q3_employee_type6.py`

---

## 1. Executive Summary

`Q3.dtsx` is a **Type 6 dimension** example for `EMPLOYEE_Q3` that tracks changes
to a single attribute, **`Schedule_Date`**, and maintains a **`Version_No`** per
natural key (`ID`). It incrementally extracts rows from `ASS3DB.dbo.EMPLOYEE_Q3`
(`Schedule_Date > ?`), looks up the current **active** warehouse version
(`Active_Flag = 1`) on `ID`, and:

- **new member** (no match) → insert with `Version_No = 1`, `Active_Flag = 1`;
- **matched member** → **expire** the current active row (`Active_Flag = 0`) and
  **insert** a new active row.

Unlike Q2, Q3 stores the natural attributes directly (`City`, `Email`,
`Schedule_Date`) plus `Active_Flag` + `Version_No` (no `CURRENT*/HESTORICAL*`
pairs, no `ENDDate`). Complexity is **Medium-High**.

> ⚠️ **Two quirks in the source package are preserved verbatim** (see §7/§12):
> (1) **both** match branches expire + re-insert, so a matched member is always
> re-versioned even when nothing changed; (2) a **changed** `Schedule_Date`
> **resets** `Version_No` to `1`, while an **unchanged** date **increments** it
> (`old + 1`). This is the opposite of intuitive versioning and is flagged as a
> likely defect in the original package — but migrated 1:1 per the "preserve
> business logic exactly / never skip undocumented transformations" mandate.

---

## 2. Package Flow Diagram

```
Data Flow Task
  [OLE DB Source "EMPLOYEE_Q3 Source"]
     select ID,Name,City,Email,Schedule_Date
     from [ASS3DB].[dbo].EMPLOYEE_Q3 WHERE Schedule_Date > ?
        │
        ▼
  [Lookup "New Record"]  ref = DW EMPLOYEE_Q3 WHERE Active_Flag=1, join on ID
     ├── No Match ─► [Derived "Add Meta Data 1": F=1 (Active_Flag), V=1 (Version_No)]
     │                  └─► [OLE DB Dest "Destination 1"]   (INSERT new, version 1)
     │
     └── Match ─► [Derived "Add Meta Data": NotSameDay = (Schedule_Date != ref.Schedule_Date)?1:0]
                    └─► [Conditional Split "check if date changed ?"]
                          ├── "Date changed"      (NotSameDay == 1)
                          │     └► [OLE DB Cmd "Set Active Flag 0"]  expire old
                          │        └► [Derived "Add Meta Data 2": vnum=1, activeflag=1]
                          │           └► [OLE DB Dest "Destination 2"]
                          └── "Date Not Changed"  (NotSameDay != 1)
                                └► [OLE DB Cmd "Set Active Flag 0 1"] expire old
                                   └► [Derived "Add Meta Data 3": vnum=Version_No+1, activeflag=1]
                                      └► [OLE DB Dest "Destination 3"]
```

Both OLE DB Commands execute:
```sql
update [ASS3DB_DW].[dbo].EMPLOYEE_Q3 set Active_Flag = 0 WHERE Emp_Key = ?
```

---

## 3. Control Flow Mapping

| SSIS object | Behaviour | PySpark equivalent |
|---|---|---|
| `Data Flow Task` | The SCD pipeline (runs once) | `q3.run()` |

There is **no Execute SQL Task** in Q3's control flow. The source `?` parameter
is bound to a package variable GUID that is **not defined** in the package — so
the intended watermark source is ambiguous (see §5 / §12).

---

## 4. Data Flow Mapping

| SSIS component | Type | PySpark equivalent |
|---|---|---|
| `EMPLOYEE_Q3 Source` (`Schedule_Date > ?`) | OLE DB Source | `read_jdbc_query(... where Schedule_Date > '<wm>')` |
| `New Record` (Lookup, `Active_Flag=1`, on `ID`) | Lookup | `source.join(active_dim, on ID, how="left")` |
| `Add Meta Data` (Derived) | Derived Column | `not_same_day` flag |
| `check if date changed ?` (Conditional Split) | Conditional Split | `.filter(not_same_day == 1 / != 1)` |
| `Add Meta Data 1/2/3` (Derived) | Derived Column | per-branch `Version_No` literal/expr |
| `Set Active Flag 0` / `Set Active Flag 0 1` | OLE DB Command | Delta `MERGE … whenMatchedUpdate(Active_Flag=0)` |
| `Destination 1/2/3` | OLE DB Destination | `append_delta` of the unioned insert set |

---

## 5. Variable Mapping

| SSIS variable / parameter | Purpose | PySpark equivalent |
|---|---|---|
| Source `?` (bound to undefined variable GUID) | Incremental watermark on `Schedule_Date` | `q3.compute_watermark()` — **assumed** `MAX(Schedule_Date)` of active rows |

> **Ambiguity:** the watermark variable is referenced but not defined in the
> `.dtsx`. We mirror the Q2 pattern (max active `Schedule_Date`), coalesced to
> `JobConfig.initial_load_watermark` for first loads. Override `compute_watermark`
> or pass a different config if the real intent differs (e.g. full load).

---

## 6. Connection Mapping

| SSIS Connection Manager | Type | PySpark config |
|---|---|---|
| `DESKTOP-HREK7MN.ASS3DB` | OLE DB (source OLTP) | `JobConfig.source` (DB `ASS3DB`) |
| `DESKTOP-HREK7MN.ASS3DB_DW` | OLE DB (warehouse) | `JobConfig.employee_q3` (Delta `ass3db_dw.employee_q3`) |

---

## 7. Transformation Mapping

### Source schema (`ASS3DB.dbo.EMPLOYEE_Q3`)
`ID int`, `Name nvarchar(255)`, `City nvarchar(255)`, `Email nvarchar(255)`,
`Schedule_Date date`.

### Warehouse schema (`ASS3DB_DW.dbo.EMPLOYEE_Q3`)
`Emp_Key int IDENTITY (PK)`, `ID int`, `Name`, `City`, `Email`,
`Schedule_Date date`, `Active_Flag int`, `Version_No int`.

### Derived column expressions (preserved exactly)
| Derived component | Expression | Output |
|---|---|---|
| `Add Meta Data` | `([EMPLOYEE_Q3 Source].Schedule_Date != [New Record].Schedule_Date) ? 1 : 0` | `NotSameDay` |
| `Add Meta Data 1` | `1`, `1` | `F` (Active_Flag), `V` (Version_No) |
| `Add Meta Data 2` | `1`, `1` | `vnum` (Version_No), `activeflag` |
| `Add Meta Data 3` | `Version_No + 1`, `1` | `vnum`, `activeflag` |

### Destination column mappings (exact, extracted from `Q3.dtsx`)
All three destinations write `ID, Name, City, Email, Schedule_Date` from the
**source** plus `Active_Flag` and `Version_No` from the derived columns:

| Destination | Branch | Version_No source | Active_Flag |
|---|---|---|---|
| `Destination 1` | new member (no match) | `Add Meta Data 1.V` = **1** | 1 |
| `Destination 2` | "Date changed" | `Add Meta Data 2.vnum` = **1** | 1 |
| `Destination 3` | "Date Not Changed" | `Add Meta Data 3.vnum` = **Version_No + 1** | 1 |

> **Quirks preserved (see §1):**
> - Both `Destination 2` and `Destination 3` paths first run an OLE DB Command
>   that expires (`Active_Flag = 0`) the matched member's current row — so a
>   matched member is **always** re-inserted as a new version, even when the date
>   is unchanged.
> - The version logic is inverted relative to intuition (changed → reset to 1;
>   unchanged → increment). Migrated exactly; flagged as a probable source defect.

---

## 8. Error Handling Mapping

| SSIS behaviour | PySpark behaviour |
|---|---|
| OLE DB Command / Destination `FailComponent` | atomic Delta MERGE + append; failure aborts and is logged/re-raised |
| Lookup No-Match output | `left join` + null-key branch (new member) |
| SSIS execution log | `AuditLogger` steps: `compute_watermark`, `extract_source`, `read_active_dim`, `transform_type6`, `expire_old_rows`, `insert_new_versions` |

---

## 9. PySpark Implementation

See **`pyspark_migration/jobs/q3_employee_type6.py`**:

```python
watermark = compute_watermark(spark, config)          # MAX(Schedule_Date) active (assumed)
source    = read_jdbc_query(... f"Schedule_Date > '{watermark}'")
active    = read_delta(target).filter("Active_Flag = 1")
to_insert, to_expire = build_changes(source, active)  # pure, testable

DeltaTable.forName(target).alias("t").merge(           # both OLE DB Commands
    to_expire.alias("x"), "t.Emp_Key = x.Emp_Key and t.Active_Flag = 1"
).whenMatchedUpdate(set={"Active_Flag": 0}).execute()

append_delta(assign_surrogate_keys(to_insert, max_key), target)  # 3 destinations
```

---

## 10. Unit Test Cases

See **`pyspark_migration/tests/test_q3_employee_type6.py`**:

| Test | Scenario | Asserts |
|---|---|---|
| `test_new_member_version_1` | no match | `Version_No = 1`, no expiry |
| `test_date_changed_resets_version_to_1` | date changed | preserved quirk: `Version_No = 1`; 1 expiry |
| `test_date_unchanged_increments_version` | date same | preserved quirk: `Version_No = old + 1`; 1 expiry |
| `test_end_to_end_run` | seeded Delta table | old row expired; one active row per ID; `Version_No` 3→4 for re-versioned member |

---

## 11. Data Validation Queries

See **`sql/validation_queries.sql`** (section Q3):
- `V3.1` ≤ 1 active row per `ID`
- `V3.2` `Version_No ≥ 1`
- `V3.3` `Emp_Key` uniqueness

---

## 12. Deployment Instructions

1. **DDL:** run `sql/ddl_delta_tables.sql` (creates `employee_q3`, `etl_audit_log`).
2. **Config/secrets:** source + warehouse JDBC via env vars / `JobConfig`
   (secret scope on Databricks).
3. **JDBC driver:** MS SQL Server JDBC on the cluster.
4. **Initial load:** empty active dim → watermark coalesced to
   `initial_load_watermark` (`1900-01-01`).
5. **Schedule:** Databricks Job task `jobs/q3_employee_type6.py`.
6. **Validate:** Q3 validation queries + `etl_audit_log`.

### Assumptions & Ambiguities (Q3)
- **Undefined watermark variable:** source `?` is bound to a variable not present
  in the package; we assume `MAX(Schedule_Date)` of active rows. Adjust if the
  real intent is a full load or a different column.
- **Always-re-version on match:** both branches expire + insert, including the
  "Date Not Changed" path — preserved exactly.
- **Inverted version logic** (changed → 1, unchanged → +1) — preserved exactly;
  likely a defect in the source package. **Recommend confirming intended
  behaviour with the data owner**; the fix would be a one-line swap in
  `build_changes` if they want changed→increment / unchanged→no-op.
- Target Delta Lake; `Emp_Key` assigned in code for portability.
