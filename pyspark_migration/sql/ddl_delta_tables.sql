-- =====================================================================
-- Delta Lake DDL for the migrated SSIS data warehouse (ASS3DB_DW)
-- Source of truth: ASS3DB_DW.bacpac model.xml + the SSIS lookup
-- reference metadata embedded in Q2.dtsx / Q3.dtsx.
--
-- SQL Server `int IDENTITY(1,1)` surrogate keys: Emp_Key is defined as a
-- plain INT and assigned in code (see common.io_utils.assign_surrogate_keys)
-- for engine portability / testability.  On Databricks you may instead use
-- `Emp_Key INT GENERATED ALWAYS AS IDENTITY` and drop the code-side
-- assignment.
-- SQL Server `date` -> Delta `DATE`; `nvarchar`/`varchar` -> `STRING`;
-- `int` -> `INT`; `bigint` -> `BIGINT`; `float` -> `DOUBLE`.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS ass3db_dw;

-- ---------------------------------------------------------------------
-- Q1: Product_Q1 (JSON / REST API load target)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ass3db_dw.product_q1 (
    id                 BIGINT,
    title              STRING,        -- varchar(152)
    price              BIGINT,
    discountPercentage DOUBLE,        -- float
    stock              BIGINT,
    brand              STRING,        -- varchar(104)
    category           STRING,        -- varchar(60)
    rating             DOUBLE         -- float
) USING DELTA;

-- ---------------------------------------------------------------------
-- Q2: EMPLOYEE_Q2 — Type 6 dimension
--   CURRENT* / HESTORICAL* attribute pairs + start/end dating + flag
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ass3db_dw.employee_q2 (
    Emp_Key          INT,            -- surrogate key (see note above)
    ID               INT,
    Name             STRING,          -- nvarchar(255)
    CURRENTCity      STRING,          -- nvarchar(255)
    HESTORICALCity   STRING,          -- nvarchar(255)  [sic: spelling preserved from source]
    CURRENTEmail     STRING,          -- nvarchar(255)
    HESTORICALEmail  STRING,          -- nvarchar(255)
    STARTDate        DATE,
    ENDDate          DATE,
    Active_Flag      INT
) USING DELTA;

-- ---------------------------------------------------------------------
-- Q3: EMPLOYEE_Q3 — Type 6 dimension (versioned)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ass3db_dw.employee_q3 (
    Emp_Key       INT,                -- surrogate key (see note above)
    ID            INT,
    Name          STRING,             -- nvarchar(255)
    City          STRING,             -- nvarchar(255)
    Email         STRING,             -- nvarchar(255)
    Schedule_Date DATE,
    Active_Flag   INT,
    Version_No    INT
) USING DELTA;

-- ---------------------------------------------------------------------
-- ETL audit log (replaces the SSIS execution log)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ass3db_dw.etl_audit_log (
    run_id     STRING,
    package    STRING,
    step       STRING,
    status     STRING,
    row_count  BIGINT,
    message    STRING,
    started_at TIMESTAMP,
    ended_at   TIMESTAMP
) USING DELTA;
