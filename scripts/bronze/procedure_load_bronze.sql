-- ===============================================================================
-- Stored Procedure: bronze.load_bronze()
-- ===============================================================================
-- Script Purpose:
--   This stored procedure loads raw source data (CSV files) into the 'bronze' schema.
--   It performs the following actions:
--     - Truncates existing data in the bronze tables.
--     - Uses the COPY command to import fresh data from CSV files.
--     - Logs row counts and load durations for each table.
--
-- Parameters:
--   None. 
--   This stored procedure does not accept any parameters or return any values.
--
-- Usage Example:
--   CALL bronze.load_bronze();
--
-- Requirements:
--   - Place all source CSV files into /tmp/pg_import/
--   - Run the following commands before executing the procedure:
--       mkdir -p /tmp/pg_import
--       cp [your_path]/source_crm/*.csv /tmp/pg_import/
--       cp [your_path]/source_erp/*.csv /tmp/pg_import/
--       chmod 755 /tmp/pg_import
--       chmod 644 /tmp/pg_import/*.csv
--
-- Error Handling:
--   If any error occurs during loading, the procedure will output:
--     - The error message (SQLERRM)
--     - The SQL state (SQLSTATE)
--   using RAISE NOTICE.
-- ===============================================================================


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time          TIMESTAMP;
    end_time            TIMESTAMP;
    duration            INTERVAL;
    batch_start_time    TIMESTAMP;
    batch_end_time      TIMESTAMP;
    batch_duration      INTERVAL;
BEGIN
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '=============================================';

    RAISE NOTICE '-------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '-------------------------------------';

    -- CRM Customers
    batch_start_time := clock_timestamp();
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    FROM '/tmp/pg_import/cust_info.csv'
    WITH (FORMAT csv, DELIMITER ',', HEADER TRUE);
    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE '   Rows inserted into crm_cust_info: %', (SELECT COUNT(*) FROM bronze.crm_cust_info);
    RAISE NOTICE '   Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM duration)::numeric, 3);

    -- CRM Products
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info (
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    FROM '/tmp/pg_import/prd_info.csv'
    WITH (FORMAT csv, DELIMITER ',', HEADER TRUE);
    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE '   Rows inserted into crm_prd_info: %', (SELECT COUNT(*) FROM bronze.crm_prd_info);
    RAISE NOTICE '   Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM duration)::numeric, 3);

    -- CRM Sales
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details (
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
    FROM '/tmp/pg_import/sales_details.csv'
    WITH (FORMAT csv, DELIMITER ',', HEADER TRUE);
    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE '   Rows inserted into crm_sales_details: %', (SELECT COUNT(*) FROM bronze.crm_sales_details);
    RAISE NOTICE '   Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM duration)::numeric, 3);

    RAISE NOTICE '-------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '-------------------------------------';

    -- ERP Customers
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    FROM '/tmp/pg_import/cust_az12.csv'
    WITH (FORMAT csv, DELIMITER ',', HEADER TRUE);
    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE '   Rows inserted into erp_cust_az12: %', (SELECT COUNT(*) FROM bronze.erp_cust_az12);
    RAISE NOTICE '   Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM duration)::numeric, 3);

    -- ERP Locations
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101 (
        cid,
        cntry
    )
    FROM '/tmp/pg_import/loc_a101.csv'
    WITH (FORMAT csv, DELIMITER ',', HEADER TRUE);
    end_time := clock_timestamp();
    duration := end_time - start_time;

    RAISE NOTICE '   Rows inserted into erp_loc_a101: %', (SELECT COUNT(*) FROM bronze.erp_loc_a101);
    RAISE NOTICE '   Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM duration)::numeric, 3);

    -- ERP Product Categories
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    FROM '/tmp/pg_import/px_cat_g1v2.csv'
    WITH (FORMAT csv, DELIMITER ',', HEADER TRUE);
    end_time := clock_timestamp();
    batch_end_time := clock_timestamp();
    duration := end_time - start_time;
    batch_duration := batch_end_time - batch_start_time;

    RAISE NOTICE '   Rows inserted into erp_px_cat_g1v2: %', (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2);
    RAISE NOTICE '   Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM duration)::numeric, 3);

    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Loading Bronze Layer Finished';
    RAISE NOTICE '>> Bronze Layer Load Duration: % seconds', ROUND(EXTRACT(EPOCH FROM batch_duration)::numeric, 3);
    RAISE NOTICE '=============================================';
EXCEPTION
    WHEN others THEN
        RAISE NOTICE '=============================================';
        RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        RAISE NOTICE 'Error Message: %', SQLERRM;
        RAISE NOTICE 'Error State: %', SQLSTATE;
        RAISE NOTICE '=============================================';
END;
$$;