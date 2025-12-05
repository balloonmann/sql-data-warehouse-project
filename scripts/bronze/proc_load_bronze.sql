/*
  This script creates the stored procedure responsible for loading Bronze tables

  Truncates tables before loading data, and Bulk Inserts to load data from the csv files to the tables
*/

EXEC bronze.load_bronze    
    CREATE OR ALTER PROCEDURE bronze.load_bronze AS
    BEGIN
        DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

        BEGIN TRY
            SET @batch_start_time = GETDATE();
            PRINT '===========================================================';
            PRINT 'STARTING BRONZE LAYER LOAD';
            PRINT '===========================================================';
            PRINT '';
            PRINT 'Environment  : Local SQL Server';
            PRINT 'Target Layer : BRONZE';
            PRINT 'Started At    : ' + CAST(GETDATE() AS NVARCHAR);
            PRINT '-----------------------------------------------------------';
            PRINT '';

            -----------------------------------------------------------
            -- CRM DATA LOAD
            -----------------------------------------------------------
            PRINT '==================== LOADING CRM TABLES ====================';
            PRINT '';

            --------------------- crm_cust_info ----------------------
            PRINT 'Loading Table: bronze.crm_cust_info';
            SET @start_time = GETDATE();

            TRUNCATE TABLE bronze.crm_cust_info;
            BULK INSERT bronze.crm_cust_info
            FROM 'datasets\source_crm\cust_info.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

            SET @end_time = GETDATE();
            PRINT 'Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-----------------------------------------------------------';

            --------------------- crm_prd_info -----------------------
            PRINT 'Loading Table: bronze.crm_prd_info';
            SET @start_time = GETDATE();

            TRUNCATE TABLE bronze.crm_prd_info;
            BULK INSERT bronze.crm_prd_info
            FROM 'datasets\source_crm\prd_info.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

            SET @end_time = GETDATE();
            PRINT 'Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-----------------------------------------------------------';

            ------------------ crm_sales_details ---------------------
            PRINT 'Loading Table: bronze.crm_sales_details';
            SET @start_time = GETDATE();

            TRUNCATE TABLE bronze.crm_sales_details;
            BULK INSERT bronze.crm_sales_details
            FROM 'datasets\source_crm\sales_details.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

            SET @end_time = GETDATE();
            PRINT 'Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '';
            PRINT '=================== CRM TABLE LOAD COMPLETE =================';
            PRINT '';

            -----------------------------------------------------------
            -- ERP DATA LOAD
            -----------------------------------------------------------
            PRINT '==================== LOADING ERP TABLES ====================';
            PRINT '';

            --------------------- erp_cust_az12 ----------------------
            PRINT 'Loading Table: bronze.erp_cust_az12';
            SET @start_time = GETDATE();

            TRUNCATE TABLE bronze.erp_cust_az12;
            BULK INSERT bronze.erp_cust_az12
            FROM 'datasets\source_erp\cust_az12.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

            SET @end_time = GETDATE();
            PRINT 'Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-----------------------------------------------------------';

            --------------------- erp_loc_a101 -----------------------
            PRINT 'Loading Table: bronze.erp_loc_a101';
            SET @start_time = GETDATE();

            TRUNCATE TABLE bronze.erp_loc_a101;
            BULK INSERT bronze.erp_loc_a101
            FROM 'datasets\source_erp\loc_a101.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

            SET @end_time = GETDATE();
            PRINT 'Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '-----------------------------------------------------------';

            -------------------- erp_px_cat_g1v2 ---------------------
            PRINT 'Loading Table: bronze.erp_px_cat_g1v2';
            SET @start_time = GETDATE();

            TRUNCATE TABLE bronze.erp_px_cat_g1v2;
            BULK INSERT bronze.erp_px_cat_g1v2
            FROM 'datasets\source_erp\px_cat_g1v2.csv'
            WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

            SET @batch_end_time = GETDATE();
            SET @end_time = GETDATE();
            PRINT 'Completed in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
            PRINT '';
            PRINT '=================== ERP TABLE LOAD COMPLETE =================';
            PRINT '';

            -----------------------------------------------------------
            -- END
            -----------------------------------------------------------
            PRINT '===========================================================';
            PRINT 'BRONZE LAYER LOAD FINISHED SUCCESSFULLY';
            PRINT 'Completed At : ' + CAST(GETDATE() AS NVARCHAR);
            PRINT 'Total Time Taken to Load Bronze Layer: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
            PRINT '===========================================================';
        END TRY

        BEGIN CATCH
            PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        END CATCH
    END;
