/*
==========================================================================================================
DDL Script: Create Bronze Tables
==========================================================================================================
Sript Purpose:
  This script creates tables in the 'bronze' schema, dropping existing tables
  if they already exist.
  Run this script to re-define the DDL structure of 'bronze' Tables
==========================================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================================';


		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_cust_info <<';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> inserting Data Into: bronze.crm_cust_info <<';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Rami\Desktop\Data_Analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '-------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_prd_info <<';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> inserting Data Into: crm_prd_info <<';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Rami\Desktop\Data_Analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '-------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_sales_details <<';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> inserting Data Into: bronze.crm_sales_details<<';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Rami\Desktop\Data_Analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '-------------';
		PRINT '-------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_cust_az12 <<';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> inserting Data Into: bronze.erp_cust_az12 <<';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Rami\Desktop\Data_Analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '-------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_loc_a101 <<';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> inserting Data Into: bronze.erp_loc_a101 <<';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Rami\Desktop\Data_Analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '-------------';
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2 <<';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> inserting Data Into: bronze.erp_px_cat_g1v2 <<';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Rami\Desktop\Data_Analysis\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '-------------';
		SET @batch_end_time = GETDATE();
		PRINT '===============================================';
		PRINT'Loading The Bronze Layer is Completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'Seconds';
	END TRY
	BEGIN CATCH
		PRINT '===============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Erro Message' + ERROR_MESSAGE();
		PRINT 'Erro Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Erro Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===============================================';
	END CATCH
END
