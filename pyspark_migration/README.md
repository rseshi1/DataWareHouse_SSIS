# SSIS → PySpark Migration (`DataWareHouse_SSIS`)

Production-ready PySpark / Delta Lake port of the three SSIS packages in this
repository, with full migration documentation, unit tests and validation SQL.

| SSIS package | Purpose | PySpark job | Doc |
|---|---|---|---|
| `Q1.dtsx` | JSON/REST API → `Product_Q1` load | `jobs/q1_product_load.py` | [docs/Q1_migration.md](docs/Q1_migration.md) |
| `Q2.dtsx` | Type 6 SCD for `EMPLOYEE_Q2` (City/Email) | `jobs/q2_employee_type6.py` | [docs/Q2_migration.md](docs/Q2_migration.md) |
| `Q3.dtsx` | Type 6 SCD for `EMPLOYEE_Q3` (Schedule_Date + version) | `jobs/q3_employee_type6.py` | [docs/Q3_migration.md](docs/Q3_migration.md) |

Each `docs/Q*_migration.md` contains the **12 required deliverables**: Executive
Summary, Package Flow Diagram, Control/Data Flow Mapping, Variable Mapping,
Connection Mapping, Transformation Mapping, Error Handling Mapping, PySpark
Implementation, Unit Test Cases, Data Validation Queries, Deployment Instructions.

## Layout

```
pyspark_migration/
├── common/            # shared infra (config, spark, JDBC/JSON I/O, audit)
│   ├── config.py      # JdbcConfig / DeltaTableConfig / JsonSourceConfig / JobConfig
│   ├── spark_session.py
│   ├── io_utils.py    # OLE DB Source/Dest + ZappySys JSON + Delta helpers
│   └── audit.py       # replaces the SSIS execution log
├── jobs/              # one module per package
│   ├── q1_product_load.py
│   ├── q2_employee_type6.py
│   └── q3_employee_type6.py
├── sql/
│   ├── ddl_delta_tables.sql      # Delta DDL (from bacpac + SSIS metadata)
│   └── validation_queries.sql    # post-load data-quality checks
├── tests/             # pytest (local Delta-enabled Spark session)
├── docs/              # the 12 deliverables per package
└── requirements.txt
```

## Design principles

- **Business logic preserved exactly.** Transformation/derived-column expressions,
  conditional-split predicates and destination column mappings were extracted
  directly from the `.dtsx` XML and reproduced 1:1. Undocumented/quirky behaviour
  (e.g. Q3's version logic) is **kept and flagged**, never silently "fixed".
- **Pure, testable transforms.** Each job exposes a side-effect-free
  `build_changes` / `transform` that the unit tests exercise without I/O.
- **Distributed-friendly.** SCD routing is set-based (joins + a single Delta
  `MERGE` + append) — no row-by-row OLE DB Command loops. Scales horizontally.
- **Auditing & error handling** are explicit (`common/audit.py` → Delta audit
  table + structured logs; fail-fast on bad batches, matching SSIS
  `FailComponent`).
- **Portable.** Source = SQL Server via JDBC; sink = Delta Lake. Swap helpers in
  `io_utils` to retarget (e.g. keep SQL Server sink via `write_jdbc_table`).

## Schemas (source of truth)

Warehouse/source column types were taken from the bundled bacpac
`model.xml` files (`ASS3DB.bacpac`, `ASS3DB_DW.bacpac`) and cross-checked against
the SSIS lookup `ReferenceMetadataXml`. See `sql/ddl_delta_tables.sql`.

## Quickstart (local)

```bash
cd pyspark_migration
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
python -m pytest tests/ -q          # 12 tests, Delta-enabled local Spark
```

## Run on Databricks

1. Apply `sql/ddl_delta_tables.sql`.
2. Provide source/warehouse credentials from a secret scope and build a
   `JobConfig` (or set `SRC_*` / `DW_*` env vars and use `JobConfig.from_env()`).
3. Ensure the MS SQL Server JDBC driver is installed on the cluster.
4. Create one Job task per package (`jobs/q1_product_load.py`, etc.).
5. After each run, execute `sql/validation_queries.sql` and inspect
   `ass3db_dw.etl_audit_log`.

See each package doc's **Deployment Instructions** and **Assumptions &
Ambiguities** sections for specifics (notably the incremental-watermark behaviour
for Q2/Q3 and the preserved quirks in Q3).
