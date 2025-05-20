/*
====================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
====================================================================
Script Purpose:
  This stored procedure performs the ELT(Extract, Transform, Load) process to
  populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
  -Truncates Silver tables.
  -Idnserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
  None.
  This stored procedure does not accept any paramedters or return any values.

Usage Example:  
  EXEC silver.load_silver;
====================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=================================================';

		PRINT '-------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------';
		
			--Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		print '>> Insterting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_material_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case
			when upper(trim(cst_material_status)) = 'S' then 'Single'
			when upper(trim(cst_material_status)) = 'M' then 'Married'
			else 'n/a'
		end as cst_material_status,
		case
			when upper(trim(cst_gndr)) = 'F' then 'Female'
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
		end as cst_gndr,
		cst_create_date
		from(
			select
			*,
			ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		)t
		where flag_last =1;
		SET @end_time = GETDATE();
		PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '-------------';
		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		print '>> Insterting Datza Into:: silver.crm_prd_info';
		insert into silver.crm_prd_info(
			prd_id,
			prd_key,
			cat_id,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
		END AS prd_line,
		CAST (prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		SET @end_time = GETDATE();
		PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
		PRINT '-------------';
		-- Loading silver.crm_sales_details
		SET @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		print '>> Insterting Datza Into: silver.crm_sales_details';
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
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
	-- sls_order_dt, transfer to int =>
	CASE WHEN sls_order_dt = 0 or len(sls_order_dt) != 8 then null
		 else cast(cast(sls_order_dt as varchar) as date)
	end as sls_order_dt,
	-- sls_ship_dt, no issue but transfer to int =>
	CASE WHEN sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
		 else cast(cast(sls_ship_dt as varchar) as date)
	end as sls_ship_dt,
	-- sls_due_dt, no issue but transfer to int =>
	CASE WHEN sls_due_dt = 0 or len(sls_due_dt) != 8 then null
		 else cast(cast(sls_due_dt as varchar) as date)
	end as sls_due_dt,
	case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
		 then sls_quantity * abs(sls_price)
		 else sls_sales
	end as sls_sales,
	sls_quantity,
	case when sls_price is null or sls_price <=0
		 then sls_sales/nullif(sls_quantity,0)
		 else sls_price
	end as sls_price
	from bronze.crm_sales_details
	SET @end_time = GETDATE();
	PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
	PRINT '-------------';
	
	PRINT '-------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '-------------------------------------------------';
	-- Loading silver.erp_cust_az12
	SET @start_time = GETDATE();
	print '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	print '>> Insterting Datza Into:: silver.erp_cust_az12';
	insert into silver.erp_cust_az12(cid,bdate,gen)
	select
	case when cid like 'NAS%' 
		 then substring(cid, 4, len(cid))
		 else cid 
	end cid,
	case when bdate > getdate() then null
		 else bdate
	end as bdate,
	--bdate,
	--gen
	case when upper(trim(gen)) in ('F', 'Famale') then 'Female'
		 when upper(trim(gen)) in ('M', 'MALE') then 'Male'
		 else 'n/a'
	end gen
	from bronze.erp_cust_az12
	SET @end_time = GETDATE();
	PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
	PRINT '-------------';
	-- Loading silver.erp_loc_a101
	SET @start_time = GETDATE();
	print '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	print '>> Insterting Datza Into:: silver.erp_loc_a101';
	insert into silver.erp_loc_a101
	( cid, cntry)
	select
	Replace(cid, '-',''),
	case when trim(cntry) = 'DE' then 'Germany'
		 when trim(cntry) in ('US', 'USA') then 'United States'
		 when trim(cntry) = '' or cntry IS NULL then 'n/a'
		 else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101
	SET @end_time = GETDATE();
	PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
	PRINT '-------------';
	-- Loading silver.erp_px_cat_g1v2
	SET @start_time = GETDATE();
	print '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	print '>> Insterting Datza Into: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2
	(id,cat,subcat,maintenance)
	select 
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
	PRINT 'The Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'Seconds';
	PRINT '-------------';
	SET @batch_end_time = GETDATE();
	PRINT '===============================================';
	PRINT'Loading The Silver Layer is Completed';
	PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'Seconds';
	END TRY
	BEGIN CATCH
    PRINT '===============================================';
		PRINT 'ERROR OCCURED DURING LOADING Silver LAYER';
		PRINT 'Erro Message' + ERROR_MESSAGE();
		PRINT 'Erro Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Erro Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===============================================';
	END CATCH;
END
