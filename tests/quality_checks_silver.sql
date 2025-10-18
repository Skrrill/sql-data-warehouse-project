/***********************************************************
 Data quality audit table
***********************************************************/
IF NOT EXISTS (SELECT 1 FROM sys.objects o WHERE o.name = 'data_quality_log' AND SCHEMA_NAME(o.schema_id) = 'silver')
BEGIN
    CREATE TABLE silver.data_quality_log
    (
        id INT IDENTITY(1,1) PRIMARY KEY,
        run_id UNIQUEIDENTIFIER NOT NULL,
        run_time DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        table_name SYSNAME NOT NULL,
        check_name NVARCHAR(200) NOT NULL,
        status NVARCHAR(10) NOT NULL,           -- PASS / FAIL
        actual_value NVARCHAR(200) NULL,        -- numeric or text result
        expected_value NVARCHAR(200) NULL,      -- expected or threshold
        details NVARCHAR(4000) NULL
    );
END
GO

/***********************************************************
 Stored procedure: run quality checks for all silver tables
 - Single-call runs checks for all tables and logs results
 - Add/adjust checks or thresholds in the procedure body
***********************************************************/
CREATE OR ALTER PROCEDURE silver.run_silver_qc
    @run_id UNIQUEIDENTIFIER = NULL  -- optional: provide if you want to correlate with a job id
AS
BEGIN
    SET NOCOUNT ON;
    IF @run_id IS NULL SET @run_id = NEWID();

    DECLARE 
        @now DATETIME2 = SYSUTCDATETIME(),
        @tbl SYSNAME,
        @cnt BIGINT,
        @dup_cnt BIGINT,
        @null_cnt BIGINT,
        @pct DECIMAL(6,2),
        @msg NVARCHAR(4000);

    ----------------------------------------------------------------
    -- Helper: insert a QC result
    ----------------------------------------------------------------
    CREATE TABLE #qc_tmp
    (
        table_name SYSNAME,
        check_name NVARCHAR(200),
        status NVARCHAR(10),
        actual_value NVARCHAR(200),
        expected_value NVARCHAR(200),
        details NVARCHAR(4000)
    );

    ----------------------------------------------------------------
    -- TABLE: crm_cust_info
    ----------------------------------------------------------------
    SET @tbl = 'silver.crm_cust_info';

    -- rowcount
    SELECT @cnt = COUNT(*) FROM silver.crm_cust_info;
    INSERT INTO #qc_tmp VALUES(@tbl, 'row_count', 'PASS', CAST(@cnt AS NVARCHAR(50)), NULL, NULL);

    -- null primary key (cst_id)
    SELECT @null_cnt = COUNT(*) FROM silver.crm_cust_info WHERE cst_id IS NULL;
    INSERT INTO #qc_tmp VALUES(@tbl, 'null_cst_id', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'cst_id should not be NULL');

    -- duplicate cst_id
    SELECT @dup_cnt = COUNT(*) FROM (
        SELECT cst_id, COUNT(*) AS c FROM silver.crm_cust_info WHERE cst_id IS NOT NULL GROUP BY cst_id HAVING COUNT(*) > 1
    ) x;
    INSERT INTO #qc_tmp VALUES(@tbl, 'duplicate_cst_id', CASE WHEN @dup_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@dup_cnt AS NVARCHAR(50)), '0', 'unique cst_id expected');

    -- firstname / lastname null rate (allow small %)
    SELECT @null_cnt = COUNT(*) FROM silver.crm_cust_info WHERE cst_firstname IS NULL OR LTRIM(RTRIM(cst_firstname)) = '';
    SET @pct = CASE WHEN @cnt = 0 THEN 0 ELSE (CAST(@null_cnt AS DECIMAL(12,2)) * 100.0 / @cnt) END;
    INSERT INTO #qc_tmp VALUES(@tbl, 'null_firstname_pct', CASE WHEN @pct <= 5 THEN 'PASS' ELSE 'FAIL' END, CONCAT(CAST(@pct AS NVARCHAR(20)),'%'), '<=5%', 'percent of missing first names');

    SELECT @null_cnt = COUNT(*) FROM silver.crm_cust_info WHERE cst_lastname IS NULL OR LTRIM(RTRIM(cst_lastname)) = '';
    SET @pct = CASE WHEN @cnt = 0 THEN 0 ELSE (CAST(@null_cnt AS DECIMAL(12,2)) * 100.0 / @cnt) END;
    INSERT INTO #qc_tmp VALUES(@tbl, 'null_lastname_pct', CASE WHEN @pct <= 5 THEN 'PASS' ELSE 'FAIL' END, CONCAT(CAST(@pct AS NVARCHAR(20)),'%'), '<=5%', 'percent of missing last names');

    -- invalid marital_status / gender values
    SELECT @null_cnt = COUNT(*) FROM silver.crm_cust_info WHERE cst_marital_status NOT IN ('Single','Married','N/A');
    INSERT INTO #qc_tmp VALUES(@tbl, 'invalid_marital_status', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'allowed: Single, Married, N/A');

    SELECT @null_cnt = COUNT(*) FROM silver.crm_cust_info WHERE cst_gndr NOT IN ('Female','Male','N/A');
    INSERT INTO #qc_tmp VALUES(@tbl, 'invalid_gender', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'allowed: Female, Male, N/A');

    ----------------------------------------------------------------
    -- TABLE: crm_prd_info
    ----------------------------------------------------------------
    SET @tbl = 'silver.crm_prd_info';

    SELECT @cnt = COUNT(*) FROM silver.crm_prd_info;
    INSERT INTO #qc_tmp VALUES(@tbl, 'row_count', 'PASS', CAST(@cnt AS NVARCHAR(50)), NULL, NULL);

    SELECT @null_cnt = COUNT(*) FROM silver.crm_prd_info WHERE prd_id IS NULL;
    INSERT INTO #qc_tmp VALUES(@tbl, 'null_prd_id', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'prd_id should not be NULL');

    SELECT @dup_cnt = COUNT(*) FROM (
        SELECT prd_id, COUNT(*) c FROM silver.crm_prd_info WHERE prd_id IS NOT NULL GROUP BY prd_id HAVING COUNT(*) > 1
    ) x;
    INSERT INTO #qc_tmp VALUES(@tbl, 'duplicate_prd_id', CASE WHEN @dup_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@dup_cnt AS NVARCHAR(50)), '0', 'unique prd_id expected');

    -- prd_cost numeric & >= 0
    SELECT @null_cnt = COUNT(*) FROM silver.crm_prd_info
        WHERE TRY_CAST(prd_cost AS DECIMAL(18,4)) IS NULL OR TRY_CAST(prd_cost AS DECIMAL(18,4)) < 0;
    INSERT INTO #qc_tmp VALUES(@tbl, 'invalid_prd_cost', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'prd_cost must be numeric and >= 0');

    -- prd_line allowed values
    SELECT @null_cnt = COUNT(*) FROM silver.crm_prd_info WHERE prd_line NOT IN ('Mountain','Road','Other Sales','Touring','N/A');
    INSERT INTO #qc_tmp VALUES(@tbl, 'invalid_prd_line', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'allowed: Mountain, Road, Other Sales, Touring, N/A');

    -- prd_end_dt logical with prd_start_dt (if both present)
    SELECT @null_cnt = COUNT(*) FROM silver.crm_prd_info WHERE prd_start_dt IS NOT NULL AND prd_end_dt IS NOT NULL AND prd_end_dt < prd_start_dt;
    INSERT INTO #qc_tmp VALUES(@tbl, 'prd_end_before_start', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'prd_end_dt must be >= prd_start_dt');

    ----------------------------------------------------------------
    -- TABLE: crm_sales_details
    ----------------------------------------------------------------
    SET @tbl = 'silver.crm_sales_details';

    SELECT @cnt = COUNT(*) FROM silver.crm_sales_details;
    INSERT INTO #qc_tmp VALUES(@tbl, 'row_count', 'PASS', CAST(@cnt AS NVARCHAR(50)), NULL, NULL);

    SELECT @null_cnt = COUNT(*) FROM silver.crm_sales_details WHERE sls_ord_num IS NULL;
    INSERT INTO #qc_tmp VALUES(@tbl, 'null_sls_ord_num', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'sls_ord_num should not be NULL');

    SELECT @dup_cnt = COUNT(*) FROM (
        SELECT sls_ord_num, COUNT(*) c FROM silver.crm_sales_details WHERE sls_ord_num IS NOT NULL GROUP BY sls_ord_num HAVING COUNT(*) > 1
    ) x;
    INSERT INTO #qc_tmp VALUES(@tbl, 'duplicate_sls_ord_num', CASE WHEN @dup_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@dup_cnt AS NVARCHAR(50)), '0', 'unique order numbers expected');

    -- quantity <= 0 or NULL
    SELECT @null_cnt = COUNT(*) FROM silver.crm_sales_details WHERE sls_quantity IS NULL OR sls_quantity <= 0;
    INSERT INTO #qc_tmp VALUES(@tbl, 'invalid_sls_quantity', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'sls_quantity should be > 0');

    -- price 0 or null (flag)
    SELECT @null_cnt = COUNT(*) FROM silver.crm_sales_details WHERE sls_price IS NULL OR sls_price = 0;
    INSERT INTO #qc_tmp VALUES(@tbl, 'null_or_zero_price', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'sls_price should be populated; zero may indicate an issue');

    -- sales calculation mismatch: sls_sales <> sls_quantity * ABS(sls_price)
    SELECT @null_cnt = COUNT(*) FROM silver.crm_sales_details WHERE sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR sls_sales <> sls_quantity * ABS(sls_price);
    INSERT INTO #qc_tmp VALUES(@tbl, 'sales_mismatch_count', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'sls_sales should equal sls_quantity * ABS(sls_price)');

    -- order date format check (if present as date)
    SELECT @null_cnt = COUNT(*) FROM silver.crm_sales_details WHERE sls_order_dt IS NULL;
    INSERT INTO #qc_tmp VALUES(@tbl, 'missing_order_date', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'sls_order_dt expected for orders');

    ----------------------------------------------------------------
    -- TABLE: erp_cust_az12
    ----------------------------------------------------------------
    SET @tbl = 'silver.erp_cust_az12';

    SELECT @cnt = COUNT(*) FROM silver.erp_cust_az12;
    INSERT INTO #qc_tmp VALUES(@tbl, 'row_count', 'PASS', CAST(@cnt AS NVARCHAR(50)), NULL, NULL);

    SELECT @null_cnt = COUNT(*) FROM silver.erp_cust_az12 WHERE cid IS NULL;
    INSERT INTO #qc_tmp VALUES(@tbl, 'null_cid', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'cid should not be NULL');

    SELECT @dup_cnt = COUNT(*) FROM (
        SELECT cid, COUNT(*) c FROM silver.erp_cust_az12 WHERE cid IS NOT NULL GROUP BY cid HAVING COUNT(*) > 1
    ) x;
    INSERT INTO #qc_tmp VALUES(@tbl, 'duplicate_cid', CASE WHEN @dup_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@dup_cnt AS NVARCHAR(50)), '0', 'unique cid expected');

    -- bdate in future
    SELECT @null_cnt = COUNT(*) FROM silver.erp_cust_az12 WHERE bdate IS NOT NULL AND bdate > GETDATE();
    INSERT INTO #qc_tmp VALUES(@tbl, 'bdate_in_future', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'birthdate should not be in future');

    -- gen allowed values
    SELECT @null_cnt = COUNT(*) FROM silver.erp_cust_az12 WHERE gen NOT IN ('Female','Male','n/a');
    INSERT INTO #qc_tmp VALUES(@tbl, 'invalid_gen', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'allowed: Female, Male, n/a');

    ----------------------------------------------------------------
    -- TABLE: erp_loc_a101
    ----------------------------------------------------------------
    SET @tbl = 'silver.erp_loc_a101';

    SELECT @cnt = COUNT(*) FROM silver.erp_loc_a101;
    INSERT INTO #qc_tmp VALUES(@tbl, 'row_count', 'PASS', CAST(@cnt AS NVARCHAR(50)), NULL, NULL);

    SELECT @null_cnt = COUNT(*) FROM silver.erp_loc_a101 WHERE cid IS NULL;
    INSERT INTO #qc_tmp VALUES(@tbl, 'null_cid', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'cid should not be NULL');

    SELECT @dup_cnt = COUNT(*) FROM (
        SELECT cid, COUNT(*) c FROM silver.erp_loc_a101 WHERE cid IS NOT NULL GROUP BY cid HAVING COUNT(*) > 1
    ) x;
    INSERT INTO #qc_tmp VALUES(@tbl, 'duplicate_cid', CASE WHEN @dup_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@dup_cnt AS NVARCHAR(50)), '0', 'unique cid expected');

    -- 'n/a' countries (flag)
    SELECT @null_cnt = COUNT(*) FROM silver.erp_loc_a101 WHERE cntry = 'n/a' OR cntry IS NULL;
    SET @pct = CASE WHEN @cnt = 0 THEN 0 ELSE (CAST(@null_cnt AS DECIMAL(12,2)) * 100.0 / @cnt) END;
    INSERT INTO #qc_tmp VALUES(@tbl, 'cntry_na_pct', CASE WHEN @pct <= 10 THEN 'PASS' ELSE 'FAIL' END, CONCAT(CAST(@pct AS NVARCHAR(20)),'%'), '<=10%', 'percent of unknown country codes');

    ----------------------------------------------------------------
    -- TABLE: erp_px_cat_g1v2
    ----------------------------------------------------------------
    SET @tbl = 'silver.erp_px_cat_g1v2';

    SELECT @cnt = COUNT(*) FROM silver.erp_px_cat_g1v2;
    INSERT INTO #qc_tmp VALUES(@tbl, 'row_count', 'PASS', CAST(@cnt AS NVARCHAR(50)), NULL, NULL);

    SELECT @null_cnt = COUNT(*) FROM silver.erp_px_cat_g1v2 WHERE id IS NULL;
    INSERT INTO #qc_tmp VALUES(@tbl, 'null_id', CASE WHEN @null_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@null_cnt AS NVARCHAR(50)), '0', 'id should not be NULL');

    SELECT @dup_cnt = COUNT(*) FROM (
        SELECT id, COUNT(*) c FROM silver.erp_px_cat_g1v2 WHERE id IS NOT NULL GROUP BY id HAVING COUNT(*) > 1
    ) x;
    INSERT INTO #qc_tmp VALUES(@tbl, 'duplicate_id', CASE WHEN @dup_cnt = 0 THEN 'PASS' ELSE 'FAIL' END, CAST(@dup_cnt AS NVARCHAR(50)), '0', 'unique id expected');

    ----------------------------------------------------------------
    -- Persist checks to permanent log table
    ----------------------------------------------------------------
    INSERT INTO silver.data_quality_log(run_id, run_time, table_name, check_name, status, actual_value, expected_value, details)
    SELECT @run_id, SYSUTCDATETIME(), table_name, check_name, status, actual_value, expected_value, details
    FROM #qc_tmp;

    ----------------------------------------------------------------
    -- Optional: print a concise summary to message window
    ----------------------------------------------------------------
    PRINT '--- Data Quality Summary ---';
    SELECT table_name, check_name, status, actual_value, expected_value
    FROM #qc_tmp
    ORDER BY table_name, check_name;

    DROP TABLE #qc_tmp;
END
GO
