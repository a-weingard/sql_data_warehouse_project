/*
========================================================================================
Stored Procedure: Load Bronze Layer (Source --> Bronze)
========================================================================================
Script purpose: 
    This storee dprocedure loads data into the 'bronze' schema from external CSV files.
    It performs the following steps:
    - Truncates the bronze tables to clear existing data before loading new data.
    - Loads data into the bronze tables using the BULK INSERT command from CSV files.

Parameters:
None
This stored procedure does not accept any parameters or return any values.

Usage example:
    EXEC bronze.load_bronze;

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	DECLARE @rowcount INT;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '====================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '====================================================';
		
		PRINT '----------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------------';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM '/tmp/cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
		PRINT '>> --------------';
		
		SET @rowcount = (SELECT COUNT(*) FROM bronze.crm_cust_info);
		PRINT '>> Rows in bronze.crm_cust_info: ' + CAST(@rowcount AS NVARCHAR(10));
		PRINT '>> --------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM '/tmp/prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
		PRINT '>> --------------';
		
		SET @rowcount = (SELECT COUNT(*) FROM bronze.crm_prd_info);
		PRINT '>> Rows in bronze.crm_prd_info: ' + CAST(@rowcount AS NVARCHAR(10));
		PRINT '>> --------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM '/tmp/sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
		PRINT '>> --------------';
		
		SET @rowcount = (SELECT COUNT(*) FROM bronze.crm_sales_details);
		PRINT '>> Rows in bronze.crm_sales_details: ' + CAST(@rowcount AS NVARCHAR(10));
		PRINT '>> --------------';
		
		PRINT '----------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM '/tmp/CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
		PRINT '>> --------------';
		
		SET @rowcount = (SELECT COUNT(*) FROM bronze.erp_cust_az12);
		PRINT '>> Rows in bronze.erp_cust_az12: ' + CAST(@rowcount AS NVARCHAR(10));
		PRINT '>> --------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM '/tmp/LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
		PRINT '>> --------------';
		
		SET @rowcount = (SELECT COUNT(*) FROM bronze.erp_loc_a101);
		PRINT '>> Rows in bronze.erp_loc_a101: ' + CAST(@rowcount AS NVARCHAR(10));
		PRINT '>> --------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM '/tmp/PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR(10)) + ' seconds';
		PRINT '>> --------------';
		
		SET @rowcount = (SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2);
		PRINT '>> Rows in bronze.erp_px_cat_g1v2: ' + CAST(@rowcount AS NVARCHAR(10));
		PRINT '>> --------------';
		
		SET @batch_end_time = GETDATE();
		PRINT '===================================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '  - Total Load Duration: ' + CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR(10)) + ' seconds';
		PRINT '====================================================';
	END TRY
	BEGIN CATCH
		PRINT '===================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===================================================';
	END CATCH

END;
