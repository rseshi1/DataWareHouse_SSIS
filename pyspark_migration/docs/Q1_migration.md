# Q1.dtsx — Migration Document (SSIS → PySpark)

> Source package: `Q1.dtsx` · Target: `pyspark_migration/jobs/q1_product_load.py`

---

## 1. Executive Summary

`Q1.dtsx` is a single **Data Flow Task** that ingests product data from a public
**JSON REST API** and lands it into a SQL Server warehouse table. It uses the
third-party **ZappySys "JSON Source (REST API or File)"** component to call
`https://dummyjson.com/products/` (HTTP `GET`), flattens the `$.products[*]`
array, keeps the **first 25 rows** (`MaxRows = 25`) and writes **8 columns** to
`ASS3DB_DW.dbo.Product_Q1` via an OLE DB Destination (fast-load).

There is **no Control Flow logic**, **no variables**, and **no transformation**
between source and destination — it is a straight extract-and-load. Complexity
is **Low**. The only nuances worth preserving are (a) the 25-row cap and (b) the
warehouse data types (`id`/`price`/`stock` are `BIGINT`, so the decimal API
`price` is rounded to an integer).

The PySpark port fetches the JSON, parses it with an **explicit schema**
(no inference, so types match the DDL exactly), applies the same projection and
rounding, and writes to a **Delta** table `ass3db_dw.product_q1`.

---

## 2. Package Flow Diagram

```
Control Flow
└── Data Flow Task "Data Flow Task"

Data Flow
  ┌──────────────────────────────────────────────┐
  │ ZappySys JSON Source                          │
  │   URL   : https://dummyjson.com/products/     │
  │   Method: GET                                 │
  │   Filter: $.products[*]                        │
  │   MaxRows: 25                                  │
  └───────────────────────┬──────────────────────┘
                          │  id,title,price,discountPercentage,
                          │  stock,brand,category,rating
                          ▼
  ┌──────────────────────────────────────────────┐
  │ OLE DB Destination  [dbo].[Product_Q1]        │
  │   FastLoad: TABLOCK, CHECK_CONSTRAINTS         │
  │   Error disposition: FailComponent            │
  └──────────────────────────────────────────────┘
```

---

## 3. Control Flow Mapping

| SSIS Control Flow object | Behaviour | PySpark equivalent |
|---|---|---|
| `Data Flow Task` | Single task, runs once | `q1_product_load.run()` |
| (no precedence constraints) | — | linear function calls |

There are no loops, sequence containers, Execute SQL tasks, or event handlers.

---

## 4. Data Flow Mapping

| SSIS Data Flow component | Type | PySpark equivalent |
|---|---|---|
| `JSON Source` (ZappySys) | Source | `io_utils.fetch_json()` + `io_utils.json_rows_to_df()` |
| `OLE DB Destination` → `Product_Q1` | Destination | `append_delta()` / `mode("overwrite")` |

ZappySys properties preserved:

| ZappySys property | Value | Where in PySpark |
|---|---|---|
| `DirectPath` | `https://dummyjson.com/products/` | `JsonSourceConfig.url` |
| `HttpRequestMethod` | `GET` | `JsonSourceConfig.http_method` |
| `Filter` | `$.products[*]` | `JsonSourceConfig.array_path = "products"` |
| `MaxRows` | `25` | `JsonSourceConfig.max_rows` |
| `IncludeParentColumns` | `true` | n/a (only 8 child columns mapped) |

---

## 5. Variable Mapping

`Q1.dtsx` declares **no package or project variables**. Nothing to map. All
formerly-hard-coded values (URL, row cap, table name) are externalised into
`JobConfig` / `JsonSourceConfig` so they can be overridden per environment.

---

## 6. Connection Mapping

| SSIS Connection Manager | Type | Points to | PySpark config |
|---|---|---|---|
| ZappySys JSON connection | HTTP | `https://dummyjson.com/products/` | `JsonSourceConfig` |
| `DESKTOP-HREK7MN.ASS3DB_DW` | OLE DB (SQL Server) | warehouse DB | `DeltaTableConfig("ass3db_dw.product_q1")` |

> **Assumption:** the warehouse is migrated to **Delta Lake**. If the target
> remains SQL Server, swap `append_delta` for `io_utils.write_jdbc_table` with
> `JobConfig.warehouse_jdbc`.

---

## 7. Transformation Mapping

There are **no explicit transformations** in the package; the source columns map
1:1 to the destination. The implicit transformations preserved are the
**type conversions** dictated by the destination column types:

| Output column | API value example | SSIS metadata | Warehouse type | PySpark cast |
|---|---|---|---|---|
| `id` | `1` | `i8` | `BIGINT` | `cast bigint` |
| `title` | `"iPhone 9"` | `wstr` | `varchar(152)` → `STRING` | `cast string` |
| `price` | `549.99` | `i8` | `BIGINT` | `round(price)` → `bigint` |
| `discountPercentage` | `12.96` | `r8` | `float` → `DOUBLE` | `cast double` |
| `stock` | `94` | `i8` | `BIGINT` | `cast bigint` |
| `brand` | `"Apple"` | `wstr` | `varchar(104)` → `STRING` | `cast string` |
| `category` | `"smartphones"` | `wstr` | `varchar(60)` → `STRING` | `cast string` |
| `rating` | `4.69` | `r8` | `float` → `DOUBLE` | `cast double` |

> **Assumption (documented):** `price` is `BIGINT` in the warehouse, yet the API
> returns decimals. SSIS/ZappySys converted the value to an 8-byte integer; we
> reproduce this with **half-up rounding** (`F.round`) to match SQL Server's
> numeric→integer `CONVERT` semantics. If integer-truncation is actually
> desired, change `F.round(price)` to `F.floor(price)`.

---

## 8. Error Handling Mapping

| SSIS behaviour | PySpark behaviour |
|---|---|
| OLE DB Destination `errorRowDisposition = FailComponent` | Explicit schema parse; type errors surface as nulls/exceptions and the job fails fast (no silent row drops). |
| ZappySys network error | `urllib` raises → caught by the `AuditLogger.step("extract_json")` context, logged as `FAILED`, re-raised. |
| SSIS execution log | `AuditLogger` writes a row per step (`extract_json`, `transform`, `load_product_q1`) to `ass3db_dw.etl_audit_log`. |

---

## 9. PySpark Implementation

See **`pyspark_migration/jobs/q1_product_load.py`**. Key points:

```python
rows  = fetch_json(config.json_source)          # GET + $.products[*] + MaxRows=25
raw   = json_rows_to_df(spark, rows, _JSON_PARSE_SCHEMA)   # explicit typed parse
out   = transform(raw)                           # 8-column projection + rounding
append_delta(out.select(*_OUTPUT_COLUMNS), "ass3db_dw.product_q1")
```

`load_mode="append"` mirrors the package (insert-only). Use
`load_mode="overwrite"` to reproduce the operational
`TRUNCATE TABLE Product_Q1` documented in the repo's `Query.sql` before a full
reload.

---

## 10. Unit Test Cases

See **`pyspark_migration/tests/test_q1_product_load.py`**:

| Test | Asserts |
|---|---|
| `test_transform_types_and_rounding` | 8-column projection; `price 549.99 → 550` (BIGINT, half-up); doubles preserved |
| `test_max_rows_slice_via_fetch` | end-to-end `run()` writes exactly 25 rows to Delta when the API returns more |

---

## 11. Data Validation Queries

See **`pyspark_migration/sql/validation_queries.sql`** (section Q1):

- `V1.1` row count == 25 on full load
- `V1.2` no NULL `id` / `title`
- `V1.3` domain ranges (`discountPercentage` 0–100, `rating` 0–5, `stock` ≥ 0)
- `V1.4` no duplicate `id`

---

## 12. Deployment Instructions

1. **Create the table:** run `sql/ddl_delta_tables.sql` (section `product_q1`).
2. **Configure:** set env vars (or build a `JobConfig`). Q1 needs no SQL Server
   source; only the warehouse target + outbound HTTPS access to `dummyjson.com`.
3. **Databricks:** add `pyspark_migration` as a repo/wheel; create a Job with task
   `python jobs/q1_product_load.py` (or call `q1_product_load.run(spark, cfg)`
   from a notebook). Outbound internet must be allowed from the cluster.
4. **Local / spark-submit:**
   ```bash
   pip install -r requirements.txt
   spark-submit --packages io.delta:delta-spark_2.12:3.2.0 jobs/q1_product_load.py
   ```
5. **Validate:** run the Q1 validation queries; check `etl_audit_log`.

### Assumptions & Ambiguities (Q1)
- Target is Delta Lake (see Connection Mapping for the JDBC alternative).
- `price` rounding (see Transformation Mapping).
- The package inserts; truncation before reload is operational (`overwrite` mode).
- `dummyjson.com` returns 30 products by default; `MaxRows=25` keeps the first 25.
