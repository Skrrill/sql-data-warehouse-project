/*
==============================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==============================================================

Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
==============================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @proc_start DATETIME2 = SYSUTCDATETIME(),
        @step_start DATETIME2,
        @step_end DATETIME2,
        @step_ms BIGINT,
        @section_name SYSNAME;

    PRINT '===== START: Loading SILVER layer =====';
    PRINT CONCAT('Procedure start time: ', CONVERT(varchar(30), @proc_start, 121));

    ---------------------------------------------------------------------
    -- Helper local procedure pattern:
    -- For each section: set @section_name, record @step_start, TRY { TRUNCATE; INSERT }
    -- on success compute and print duration; on error print error details and duration.
    ---------------------------------------------------------------------

    ---------------------------------------------------------------------
    -- Section: crm_cust_info
    ---------------------------------------------------------------------
    SET @section_name = 'crm_cust_info';
    PRINT '---';
    PRINT CONCAT('SECTION START: ', @section_name);
    SET @step_start = SYSUTCDATETIME();

    BEGIN TRY
        PRINT 'Step: Truncating table silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT 'Truncate successful';

        PRINT 'Step: Inserting data into silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info(
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date)
        SELECT 
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'N/A'
            END cst_marital_status,
            CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'N/A'
            END cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        PRINT 'Insert successful';
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('SECTION COMPLETE: ', @section_name, ' | Duration: ', @step_ms, ' ms');
    END TRY
    BEGIN CATCH
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('ERROR in section: ', @section_name);
        PRINT CONCAT('Message: ', ERROR_MESSAGE());
        PRINT CONCAT('Severity: ', ERROR_SEVERITY(), ' State: ', ERROR_STATE());
        PRINT CONCAT('Section duration until error: ', @step_ms, ' ms');
        -- Continue to next section
    END CATCH

    ---------------------------------------------------------------------
    -- Section: crm_prd_info
    ---------------------------------------------------------------------
    SET @section_name = 'crm_prd_info';
    PRINT '---';
    PRINT CONCAT('SECTION START: ', @section_name);
    SET @step_start = SYSUTCDATETIME();

    BEGIN TRY
        PRINT 'Step: Truncating table silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT 'Truncate successful';

        PRINT 'Step: Inserting data into silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info(
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            CASE 
                WHEN prd_cost IS NULL OR prd_cost = '' THEN '0'
                ELSE prd_cost
            END AS prd_cost,
            CASE UPPER(TRIM(prd_line)) 
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'N/A'
            END AS prd_line,
            prd_start_dt,
            DATEADD(day,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
        FROM bronze.crm_prd_info
        WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (SELECT sls_prd_key FROM bronze.crm_sales_details);

        PRINT 'Insert successful';
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('SECTION COMPLETE: ', @section_name, ' | Duration: ', @step_ms, ' ms');
    END TRY
    BEGIN CATCH
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('ERROR in section: ', @section_name);
        PRINT CONCAT('Message: ', ERROR_MESSAGE());
        PRINT CONCAT('Severity: ', ERROR_SEVERITY(), ' State: ', ERROR_STATE());
        PRINT CONCAT('Section duration until error: ', @step_ms, ' ms');
    END CATCH

    ---------------------------------------------------------------------
    -- Section: crm_sales_details
    ---------------------------------------------------------------------
    SET @section_name = 'crm_sales_details';
    PRINT '---';
    PRINT CONCAT('SECTION START: ', @section_name);
    SET @step_start = SYSUTCDATETIME();

    BEGIN TRY
        PRINT 'Step: Truncating table silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT 'Truncate successful';

        PRINT 'Step: Inserting data into silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS VARCHAR(50))) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR(50)) AS DATE)
            END AS sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        PRINT 'Insert successful';
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('SECTION COMPLETE: ', @section_name, ' | Duration: ', @step_ms, ' ms');
    END TRY
    BEGIN CATCH
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('ERROR in section: ', @section_name);
        PRINT CONCAT('Message: ', ERROR_MESSAGE());
        PRINT CONCAT('Severity: ', ERROR_SEVERITY(), ' State: ', ERROR_STATE());
        PRINT CONCAT('Section duration until error: ', @step_ms, ' ms');
    END CATCH

    ---------------------------------------------------------------------
    -- Section: erp_cust_az12
    ---------------------------------------------------------------------
    SET @section_name = 'erp_cust_az12';
    PRINT '---';
    PRINT CONCAT('SECTION START: ', @section_name);
    SET @step_start = SYSUTCDATETIME();

    BEGIN TRY
        PRINT 'Step: Truncating table silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT 'Truncate successful';

        PRINT 'Step: Inserting data into silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                 ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        PRINT 'Insert successful';
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('SECTION COMPLETE: ', @section_name, ' | Duration: ', @step_ms, ' ms');
    END TRY
    BEGIN CATCH
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('ERROR in section: ', @section_name);
        PRINT CONCAT('Message: ', ERROR_MESSAGE());
        PRINT CONCAT('Severity: ', ERROR_SEVERITY(), ' State: ', ERROR_STATE());
        PRINT CONCAT('Section duration until error: ', @step_ms, ' ms');
    END CATCH

    ---------------------------------------------------------------------
    -- Section: erp_loc_a101
    ---------------------------------------------------------------------
    SET @section_name = 'erp_loc_a101';
    PRINT '---';
    PRINT CONCAT('SECTION START: ', @section_name);
    SET @step_start = SYSUTCDATETIME();

    BEGIN TRY
        PRINT 'Step: Truncating table silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT 'Truncate successful';

        PRINT 'Step: Inserting data into silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(cid, cntry)
        SELECT
            REPLACE(cid, '-', '') cid,
            CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        PRINT 'Insert successful';
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('SECTION COMPLETE: ', @section_name, ' | Duration: ', @step_ms, ' ms');
    END TRY
    BEGIN CATCH
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('ERROR in section: ', @section_name);
        PRINT CONCAT('Message: ', ERROR_MESSAGE());
        PRINT CONCAT('Severity: ', ERROR_SEVERITY(), ' State: ', ERROR_STATE());
        PRINT CONCAT('Section duration until error: ', @step_ms, ' ms');
    END CATCH

    ---------------------------------------------------------------------
    -- Section: erp_px_cat_g1v2
    ---------------------------------------------------------------------
    SET @section_name = 'erp_px_cat_g1v2';
    PRINT '---';
    PRINT CONCAT('SECTION START: ', @section_name);
    SET @step_start = SYSUTCDATETIME();

    BEGIN TRY
        PRINT 'Step: Truncating table silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT 'Truncate successful';

        PRINT 'Step: Inserting data into silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;

        PRINT 'Insert successful';
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('SECTION COMPLETE: ', @section_name, ' | Duration: ', @step_ms, ' ms');
    END TRY
    BEGIN CATCH
        SET @step_end = SYSUTCDATETIME();
        SET @step_ms = DATEDIFF(ms, @step_start, @step_end);
        PRINT CONCAT('ERROR in section: ', @section_name);
        PRINT CONCAT('Message: ', ERROR_MESSAGE());
        PRINT CONCAT('Severity: ', ERROR_SEVERITY(), ' State: ', ERROR_STATE());
        PRINT CONCAT('Section duration until error: ', @step_ms, ' ms');
    END CATCH

    ---------------------------------------------------------------------
    -- Final summary
    ---------------------------------------------------------------------
    DECLARE @proc_end DATETIME2 = SYSUTCDATETIME();
    DECLARE @proc_ms BIGINT = DATEDIFF(ms, @proc_start, @proc_end);

    PRINT '===== COMPLETE: Loading SILVER layer =====';
    PRINT CONCAT('Procedure end time: ', CONVERT(varchar(30), @proc_end, 121));
    PRINT CONCAT('Total duration: ', @proc_ms, ' ms');

END;
GO
