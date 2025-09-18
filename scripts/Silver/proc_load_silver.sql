/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
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
===============================================================================
*/

DELIMITER $$

CREATE PROCEDURE load_silver()
BEGIN
    DECLARE v_start_time DATETIME;
    DECLARE v_end_time DATETIME;
    DECLARE v_batch_start DATETIME;
    DECLARE v_batch_end DATETIME;

    -- Error handler
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET v_end_time = NOW();
        SELECT ' ERROR OCCURRED DURING LOADING SILVER LAYER' AS status,
               NOW() AS error_time;
        ROLLBACK;
    END;

    -- Batch start
    SET v_batch_start = NOW();
    START TRANSACTION;

    SELECT '================================================' AS msg;
    SELECT 'Loading Silver Layer' AS msg;
    SELECT '================================================' AS msg;

    -- ====================================================
    -- 1. Load crm_cust_info
    -- ====================================================
    SET v_start_time = NOW();
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;

    SET v_end_time = NOW();
    SELECT ' crm_cust_info loaded' AS table_name,
           TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) AS duration_seconds;

    -- ====================================================
    -- 2. Load crm_prd_info
    -- ====================================================
    SET v_start_time = NOW();
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
        SUBSTRING(prd_key, 7),
        prd_nm,
        IFNULL(prd_cost, 0),
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        DATE_SUB(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
            INTERVAL 1 DAY
        )
    FROM bronze.crm_prd_info;

    SET v_end_time = NOW();
    SELECT ' crm_prd_info loaded' AS table_name,
           TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) AS duration_seconds;

    -- ====================================================
    -- 3. Load crm_sales_details
    -- ====================================================
    SET v_start_time = NOW();
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        NULLIF(STR_TO_DATE(sls_order_dt, '%Y%m%d'), '0000-00-00'),
        NULLIF(STR_TO_DATE(sls_ship_dt, '%Y%m%d'), '0000-00-00'),
        NULLIF(STR_TO_DATE(sls_due_dt, '%Y%m%d'), '0000-00-00'),
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    SET v_end_time = NOW();
    SELECT ' crm_sales_details loaded' AS table_name,
           TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) AS duration_seconds;

    -- ====================================================
    -- 4. Load erp_cust_az12
    -- ====================================================
    SET v_start_time = NOW();
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (
        cid, bdate, gen
    )
    SELECT
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END,
        CASE WHEN bdate > NOW() THEN NULL ELSE bdate END,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    SET v_end_time = NOW();
    SELECT '✅ erp_cust_az12 loaded' AS table_name,
           TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) AS duration_seconds;

    -- ====================================================
    -- 5. Load erp_loc_a101
    -- ====================================================
    SET v_start_time = NOW();
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (
        cid, cntry
    )
    SELECT
        REPLACE(cid, '-', ''),
        CASE 
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    SET v_end_time = NOW();
    SELECT ' erp_loc_a101 loaded' AS table_name,
           TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) AS duration_seconds;

    -- ====================================================
    -- 6. Load erp_px_cat_g1v2
    -- ====================================================
    SET v_start_time = NOW();
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
        id, cat, subcat, maintenance
    )
    SELECT
        id, cat, subcat, maintenance
    FROM bronze.erp_px_cat_g1v2;

    SET v_end_time = NOW();
    SELECT ' erp_px_cat_g1v2 loaded' AS table_name,
           TIMESTAMPDIFF(SECOND, v_start_time, v_end_time) AS duration_seconds;

    -- Commit batch
    COMMIT;
    SET v_batch_end = NOW();
    SELECT ' Loading Silver Layer Completed' AS status,
           TIMESTAMPDIFF(SECOND, v_batch_start, v_batch_end) AS total_duration_seconds;

END$$

DELIMITER ;
