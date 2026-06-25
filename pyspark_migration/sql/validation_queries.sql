-- =====================================================================
-- Data Validation Queries (Spark SQL / Databricks SQL)
-- Run these after each job to assert the migrated pipelines preserved the
-- SSIS business logic and produced a correct warehouse state.
-- =====================================================================

-- =====================================================================
-- Q1 — Product_Q1
-- =====================================================================

-- V1.1 Row count must equal the ZappySys MaxRows cap (25) on a full load.
SELECT 'Q1_rowcount' AS check, COUNT(*) AS actual, 25 AS expected
FROM ass3db_dw.product_q1;

-- V1.2 No NULL natural keys / titles (REST payload completeness).
SELECT 'Q1_null_keys' AS check, COUNT(*) AS violations
FROM ass3db_dw.product_q1
WHERE id IS NULL OR title IS NULL;

-- V1.3 Domain sanity: discount %, rating and stock within expected ranges.
SELECT 'Q1_domain' AS check, COUNT(*) AS violations
FROM ass3db_dw.product_q1
WHERE discountPercentage < 0 OR discountPercentage > 100
   OR rating < 0 OR rating > 5
   OR stock < 0;

-- V1.4 No duplicate product ids.
SELECT 'Q1_dupes' AS check, COUNT(*) AS violations FROM (
  SELECT id FROM ass3db_dw.product_q1 GROUP BY id HAVING COUNT(*) > 1
);

-- =====================================================================
-- Q2 — EMPLOYEE_Q2 (Type 6 SCD)
-- =====================================================================

-- V2.1 At most ONE active row per natural key (ID).
SELECT 'Q2_one_active_per_id' AS check, COUNT(*) AS violations FROM (
  SELECT ID FROM ass3db_dw.employee_q2 WHERE Active_Flag = 1
  GROUP BY ID HAVING COUNT(*) > 1
);

-- V2.2 Active rows must have an open end date; inactive rows must be closed.
SELECT 'Q2_dating' AS check, COUNT(*) AS violations
FROM ass3db_dw.employee_q2
WHERE (Active_Flag = 1 AND ENDDate IS NOT NULL)
   OR (Active_Flag = 0 AND ENDDate IS NULL);

-- V2.3 Type 6 history integrity: for a member with >1 version, the active
--      row's HESTORICAL* must equal the previously-active CURRENT* value of
--      the attribute that changed. (Spot check via window.)
WITH ranked AS (
  SELECT *,
         LAG(CURRENTCity)  OVER (PARTITION BY ID ORDER BY STARTDate, Emp_Key) AS prev_city,
         LAG(CURRENTEmail) OVER (PARTITION BY ID ORDER BY STARTDate, Emp_Key) AS prev_email
  FROM ass3db_dw.employee_q2
)
SELECT 'Q2_type6_history' AS check, COUNT(*) AS violations
FROM ranked
WHERE prev_city IS NOT NULL
  AND CURRENTCity <> prev_city          -- city changed on this version
  AND HESTORICALCity <> prev_city;      -- but history not captured -> violation

-- V2.4 Surrogate key uniqueness.
SELECT 'Q2_emp_key_unique' AS check, COUNT(*) AS violations FROM (
  SELECT Emp_Key FROM ass3db_dw.employee_q2
  GROUP BY Emp_Key HAVING COUNT(*) > 1
);

-- =====================================================================
-- Q3 — EMPLOYEE_Q3 (Type 6 versioned SCD)
-- =====================================================================

-- V3.1 At most ONE active row per natural key (ID).
SELECT 'Q3_one_active_per_id' AS check, COUNT(*) AS violations FROM (
  SELECT ID FROM ass3db_dw.employee_q3 WHERE Active_Flag = 1
  GROUP BY ID HAVING COUNT(*) > 1
);

-- V3.2 Version numbers are positive.
SELECT 'Q3_version_positive' AS check, COUNT(*) AS violations
FROM ass3db_dw.employee_q3
WHERE Version_No < 1;

-- V3.3 Surrogate key uniqueness.
SELECT 'Q3_emp_key_unique' AS check, COUNT(*) AS violations FROM (
  SELECT Emp_Key FROM ass3db_dw.employee_q3
  GROUP BY Emp_Key HAVING COUNT(*) > 1
);

-- =====================================================================
-- Audit log spot-check (all packages)
-- =====================================================================
SELECT package, step, status, row_count, started_at
FROM ass3db_dw.etl_audit_log
ORDER BY started_at DESC
LIMIT 50;
