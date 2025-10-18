/* ===========================================================================
   Quality Checks
   ===========================================================================
   Script Purpose:
       Simple post-load quality checks for the 'silver' schema tables.
       Run these after loading the Silver layer to validate basic data quality.

   Checks included (simple & obvious):
       - Row counts
       - Null primary keys
       - Duplicate primary keys
       - Missing important fields (with % threshold tests)
       - Invalid / unexpected enum values
       - Date range/order problems
       - Basic numeric validation (cost/price >= 0)
       - Calculation consistency (sales = quantity * abs(price))

   Usage:
       -- Run after loading silver:
       EXECUTE this script (or call from your ETL job)
   ============================================================================ */
GO

SET NOCOUNT ON;

DECLARE @run_id UNIQUEIDENTIFIER = NEWID();

-- temp results table
CREATE TABLE #qc_results
(
    run_id UNIQUEIDENTIFIER,
    table_name SYSNAME,
    check_name NVARCHAR(200),
    expected_value NVARCHAR(200),
    actual_value NVARCHAR(200),
    status NVARCHAR(10),
    details NVARCHAR(1000)
);

----------------------------------------------------------------
-- crm_cust_info checks
----------------------------------------------------------------
INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_cust_info', 'row_count', '>=0', CAST(COUNT(*) AS NVARCHAR(50)),
       'PASS', NULL
FROM silver.crm_cust_info;

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_cust_info', 'null_cst_id', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'Primary key must not be NULL'
FROM silver.crm_cust_info
WHERE cst_id IS NULL;

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_cust_info', 'duplicate_cst_id', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'Unique cst_id expected'
FROM (
    SELECT cst_id FROM silver.crm_cust_info WHERE cst_id IS NOT NULL
    GROUP BY cst_id HAVING COUNT(*) > 1
) x;

-- missing firstname% (threshold 5%)
INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_cust_info', 'missing_firstname_pct', '<=5%',
       CONCAT(CAST(COUNT(*)*100.0/NULLIF((SELECT COUNT(*) FROM silver.crm_cust_info),1) AS DECIMAL(5,2)),'%'),
       CASE WHEN (COUNT(*)*100.0/NULLIF((SELECT COUNT(*) FROM silver.crm_cust_info),1)) <= 5 THEN 'PASS' ELSE 'FAIL' END,
       'firstname NULL or empty'
FROM silver.crm_cust_info
WHERE cst_firstname IS NULL OR LTRIM(RTRIM(cst_firstname)) = '';

-- invalid marital_status
INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_cust_info', 'invalid_marital_status', 'Single, Married, N/A', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'Allowed values enforced'
FROM silver.crm_cust_info
WHERE cst_marital_status NOT IN ('Single','Married','N/A');

-- invalid gender
INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_cust_info', 'invalid_gender', 'Female, Male, N/A', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'Allowed values enforced'
FROM silver.crm_cust_info
WHERE cst_gndr NOT IN ('Female','Male','N/A');

----------------------------------------------------------------
-- crm_prd_info checks
----------------------------------------------------------------
INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_prd_info', 'row_count', '>=0', CAST(COUNT(*) AS NVARCHAR(50)), 'PASS', NULL
FROM silver.crm_prd_info;

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_prd_info', 'null_prd_id', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'Primary key prd_id should not be NULL'
FROM silver.crm_prd_info
WHERE prd_id IS NULL;

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_prd_info', 'duplicate_prd_id', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'Unique prd_id expected'
FROM (
    SELECT prd_id FROM silver.crm_prd_info WHERE prd_id IS NOT NULL
    GROUP BY prd_id HAVING COUNT(*) > 1
) x;

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_prd_info', 'invalid_prd_cost', 'numeric >= 0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'prd_cost must be numeric and >= 0'
FROM silver.crm_prd_info
WHERE TRY_CAST(prd_cost AS DECIMAL(18,4)) IS NULL OR TRY_CAST(prd_cost AS DECIMAL(18,4)) < 0;

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_prd_info', 'invalid_prd_line', 'Mountain, Road, Other Sales, Touring, N/A', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'Allowed product line values'
FROM silver.crm_prd_info
WHERE prd_line NOT IN ('Mountain','Road','Other Sales','Touring','N/A');

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_prd_info', 'prd_end_before_start', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'prd_end_dt >= prd_start_dt when both present'
FROM silver.crm_prd_info
WHERE prd_start_dt IS NOT NULL AND prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt;

----------------------------------------------------------------
-- crm_sales_details checks
----------------------------------------------------------------
INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_sales_details', 'row_count', '>=0', CAST(COUNT(*) AS NVARCHAR(50)), 'PASS', NULL
FROM silver.crm_sales_details;

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_sales_details', 'null_sls_ord_num', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'Order number should not be NULL'
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL;

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_sales_details', 'duplicate_sls_ord_num', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'Unique order numbers expected'
FROM (
    SELECT sls_ord_num FROM silver.crm_sales_details WHERE sls_ord_num IS NOT NULL
    GROUP BY sls_ord_num HAVING COUNT(*) > 1
) x;

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_sales_details', 'invalid_sls_quantity', 'sls_quantity > 0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'quantity must be > 0'
FROM silver.crm_sales_details
WHERE sls_quantity IS NULL OR sls_quantity <= 0;

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_sales_details', 'null_or_zero_price', 'non-zero price preferred', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'price missing or zero may indicate problem'
FROM silver.crm_sales_details
WHERE sls_price IS NULL OR sls_price = 0;

-- sales consistency: sales should equal quantity * abs(price)
INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_sales_details', 'sales_mismatch', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'sls_sales should equal sls_quantity * ABS(sls_price)'
FROM silver.crm_sales_details
WHERE sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR sls_sales <> sls_quantity * ABS(sls_price);

INSERT INTO #qc_results
SELECT @run_id, 'silver.crm_sales_details', 'missing_order_date', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'sls_order_dt expected'
FROM silver.crm_sales_details
WHERE sls_order_dt IS NULL;

----------------------------------------------------------------
-- erp_cust_az12 checks
----------------------------------------------------------------
INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_cust_az12', 'row_count', '>=0', CAST(COUNT(*) AS NVARCHAR(50)), 'PASS', NULL
FROM silver.erp_cust_az12;

INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_cust_az12', 'null_cid', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'cid should not be NULL'
FROM silver.erp_cust_az12
WHERE cid IS NULL;

INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_cust_az12', 'duplicate_cid', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'unique cid expected'
FROM (
    SELECT cid FROM silver.erp_cust_az12 WHERE cid IS NOT NULL
    GROUP BY cid HAVING COUNT(*) > 1
) x;

INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_cust_az12', 'bdate_in_future', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'birthdate should not be > today'
FROM silver.erp_cust_az12
WHERE bdate IS NOT NULL AND bdate > GETDATE();

INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_cust_az12', 'invalid_gen', 'Female, Male, n/a', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'gen allowed values'
FROM silver.erp_cust_az12
WHERE gen NOT IN ('Female','Male','n/a');

----------------------------------------------------------------
-- erp_loc_a101 checks
----------------------------------------------------------------
INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_loc_a101', 'row_count', '>=0', CAST(COUNT(*) AS NVARCHAR(50)), 'PASS', NULL
FROM silver.erp_loc_a101;

INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_loc_a101', 'null_cid', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'cid should not be NULL'
FROM silver.erp_loc_a101
WHERE cid IS NULL;

INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_loc_a101', 'duplicate_cid', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'unique cid expected'
FROM (
    SELECT cid FROM silver.erp_loc_a101 WHERE cid IS NOT NULL
    GROUP BY cid HAVING COUNT(*) > 1
) x;

-- percent of 'n/a' countries (threshold 10%)
INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_loc_a101', 'cntry_na_pct', '<=10%',
       CONCAT(CAST(COUNT(*)*100.0/NULLIF((SELECT COUNT(*) FROM silver.erp_loc_a101),1) AS DECIMAL(5,2)),'%'),
       CASE WHEN (COUNT(*)*100.0/NULLIF((SELECT COUNT(*) FROM silver.erp_loc_a101),1)) <= 10 THEN 'PASS' ELSE 'FAIL' END,
       'country unknown or n/a'
FROM silver.erp_loc_a101
WHERE cntry IS NULL OR cntry = 'n/a';

----------------------------------------------------------------
-- erp_px_cat_g1v2 checks
----------------------------------------------------------------
INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_px_cat_g1v2', 'row_count', '>=0', CAST(COUNT(*) AS NVARCHAR(50)), 'PASS', NULL
FROM silver.erp_px_cat_g1v2;

INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_px_cat_g1v2', 'null_id', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'id should not be NULL'
FROM silver.erp_px_cat_g1v2
WHERE id IS NULL;

INSERT INTO #qc_results
SELECT @run_id, 'silver.erp_px_cat_g1v2', 'duplicate_id', '0', CAST(COUNT(*) AS NVARCHAR(50)),
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'unique id expected'
FROM (
    SELECT id FROM silver.erp_px_cat_g1v2 WHERE id IS NOT NULL
    GROUP BY id HAVING COUNT(*) > 1
) x;

----------------------------------------------------------------
-- show results
----------------------------------------------------------------
SELECT run_id, table_name, check_name, expected_value, actual_value, status, details
FROM #qc_results
ORDER BY table_name, check_name;

-- optional: persist to a permanent audit table (uncomment to use)
-- INSERT INTO silver.data_quality_log(run_id, run_time, table_name, check_name, status, actual_value, expected_value, details)
-- SELECT run_id, SYSUTCDATETIME(), table_name, check_name, status, actual_value, expected_value, details
-- FROM #qc_results;

DROP TABLE #qc_results;
GO
