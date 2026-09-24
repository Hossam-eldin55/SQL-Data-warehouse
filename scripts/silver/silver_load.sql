/*
==================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==================================================================================
Script Purpose:
	This procedure perform the ETL (Extract, Transform, Load) Process
	to populate the silver schema	tables from the bronze schema

Action performed: 
	- Truncate Silver Tables 
	- Insert Transformed and cleaned data from bronze into silver tables.

Parameters:
	None

Usage Examble:
	EXEC silver.load_silver;
===================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY

			SET @batch_start_time = GETDATE();
			--------------------------------------------------------------------------------------
			PRINT ' =============================================== ';
			PRINT ' LOADING SILVER LAYER ';
			PRINT ' =============================================== ';

			PRINT ' ----------------------------------------------- ';
			PRINT ' LOADING CRM TABLES';
			PRINT ' ----------------------------------------------- ';

			PRINT ' ----------------------------------------------- ';
	
		-----------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin Talbe: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
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
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			 WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
		END cst_material_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male' 
			 ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM 
		(SELECT *, ROW_NUMBER() OVER(Partition by cst_id ORDER BY cst_create_date DESC) as flag
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL	) t

		WHERE flag = 1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ----------------------------------------------- ';
		---------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin Talbe: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
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
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) as prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'	
			 ELSE 'n/a' 
		END as prd_line, 
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt ASC) - 1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ----------------------------------------------- ';

		-------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin Talbe: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details
		(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_price,
		sls_quantity,
		sls_sales
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,

		CASE WHEN sls_order_dt = 0 OR len(sls_order_dt) != 8 
			 THEN NULL
			 ELSE CAST(sls_order_dt AS DATE)    -- Cleaned order_date
			 END AS sls_order_dt,

		CASE WHEN sls_ship_dt = 0 OR len(sls_ship_dt) != 8 
			 THEN NULL
			 ELSE CAST(sls_ship_dt AS DATE)    -- Cleaned ship_date
			 END AS sls_ship_dt,

		CASE WHEN sls_due_dt = 0 OR len(sls_due_dt) != 8 
			 THEN NULL
			 ELSE CAST(sls_due_dt AS DATE)     -- Cleaned due_date
			 END AS sls_due_dt,

		CASE WHEN sls_price IS NULL OR  sls_price <= 0 
		THEN ABS(sls_sales / ISNULL(sls_quantity, 0))
		ELSE sls_price                         -- Cleaned price
		END AS sls_price,

		CASE WHEN sls_quantity  IS NULL OR sls_quantity <= 0 
		THEN ABS(sls_sales / NULLIF(sls_price, 0))
		ELSE sls_quantity                      -- Cleaned quantity
		END AS sls_quantity ,

		CASE WHEN sls_sales IS NULL OR sls_sales <= 0
		OR sls_sales != ABS(sls_quantity * sls_price) 
		THEN ABS(sls_quantity * sls_price) 
		ELSE sls_sales                         -- Cleaned sales                         
		END AS sls_sales

		FROM bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ----------------------------------------------- ';

		---------------------------------------------------------------------------------
		PRINT ' ----------------------------------------------- ';
		PRINT ' LOADING ERP TABLES';
		PRINT ' ----------------------------------------------- ';
		PRINT ' ----------------------------------------------- ';
	
		SET @start_time = GETDATE();
		PRINT '>> Truncatin Talbe: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
		CID,
		BDATE,
		GEN
		)

		SELECT 
		CASE WHEN CID LIKE 'NAS%'
		THEN SUBSTRING(CID, 4,LEN(CID))
		ELSE CID                 -- Cleaned cid
		END CID,

		CASE WHEN BDATE > GETDATE() THEN NULL
		ELSE BDATE               -- Cleaned Bdate
		END BDATE,

		CASE 
		WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'n/a'               -- Cleaned gender
		END GEN        

		FROM bronze.erp_cust_az12;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ----------------------------------------------- ';

		---------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin Talbe: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (CID, CNTRY)
		SELECT 
		REPLACE(CID, '-','') CID,

		CASE WHEN UPPER(TRIM(CNTRY)) IN ('USA', 'US', 'UNITED STATES') THEN 'United States'
			 WHEN UPPER(TRIM(CNTRY)) IN ('DE', 'GERMANY') THEN 'Germany'
			 WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
			 ELSE TRIM(CNTRY)
			 END CNTRY

		FROM bronze.erp_loc_a101;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ----------------------------------------------- ';

		---------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT '>> Truncatin Talbe: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(ID, CAT, SUBCAT, MAINTENANCE)
		SELECT 
		CASE WHEN ID = 'CO_PD' THEN 'CO_PE'
		ELSE ID              
		END ID,
		CAT,
		SUBCAT,
		MAINTENANCE

		FROM bronze.erp_px_cat_g1v2;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT ' ----------------------------------------------- ';

		SET @batch_end_time = GETDATE();
		PRINT ' =============================================== ';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '- Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) +' seconds'
		PRINT ' =============================================== ';

	END TRY
	BEGIN CATCH
		PRINT '=========================================';
		PRINT 'ERROR OCURED WHILE LOADING SILVEER LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + ERROR_STATE();
		PRINT '=========================================';
	END CATCH
END

---------------------------------------------------------------------------------


