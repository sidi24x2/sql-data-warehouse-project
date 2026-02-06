/*
=======================================================================
Stored Pocedure : Load Silver Layer (Bronze -> Silver)
=======================================================================
Script Purpose : 
        This stored procedure performs the ETL (Extract , Transform , Load) process
        to populate the 'silver' schema tables from the 'bronze' schema.
        It performs the following action :
         - Trunkcates silver tables.
         - Insert transformed and clean data from bronze to silver tables.

Parameters : 
      None.
      This stored procedure does not accept any parameters or return any values.

Usage Example :
        EXEC silver.load_silver;
=======================================================================
*/

/*
===================================================================
STORED PROCEDURE - Load silver Layer (Bronze -> silver )

Script Purpose - 
		This Stored Procedure performs the ETL (Extract, Transform ,Load) process
		to populate teh 'silver' schema tables from the 'bronze' schema.	
		- It Truncates Silver Tables.
		- Insert transformed & clean data from bronze to silver tables.
Parameters : 
		This stored procedure does not accept any parameter or 
		return any value 

Usage : EXEC silver.load_silver;
===================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN 
	DECLARE @start_time DATETIME , @end_time DATETIME, @batch_start_time DATETIME , @batch_end_time DATETIME
	BEGIN TRY 
		SET @batch_start_time = GETDATE();
		PRINT '************************************';
		PRINT 'Loading silver Tables';
		PRINT '************************************';
		PRINT '';
		PRINT '====================================';
		PRINT 'Loading Data Into CRM tables';
		PRINT '====================================';

		PRINT ('Truncating Data Into Table : silver.crm_cust_info');
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT ('Inserting Data Into Table : silver.crm_cust_info');
		--SET @start_time = GETDATE();
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname),
			TRIM(cst_lastname),
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 ELSE 'n/a'
			END AS cst_marital_status,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_marital_status,
			cst_create_date
		FROM (
			SELECT 
				* ,
				RANK()OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS c_rank
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			) t 
		WHERE c_rank = 1 
	
		SET @end_time = GETDATE();
		PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
		PRINT '------------------------------------';

		PRINT ('Truncating Data Into Table : silver.crm_prd_info ');
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT ('Inserting Data Into Table : silver.crm_prd_info ');
		SET @start_time = GETDATE();
		INSERT INTO silver.crm_prd_info (
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
			SUBSTRING(prd_key , 7 ,LEN(prd_key) )AS prd_key, 
			TRIM(prd_nm) AS prd_nm,
			ISNULL(prd_cost , 0) AS prd_cost,
			CASE 
				WHEN prd_line = 'S' THEN 'Other Sales'
				WHEN prd_line = 'M' THEN 'Mountains'
				WHEN prd_line = 'R' THEN 'Road'
				WHEN prd_line = 'T' THEN 'Transport'
				ELSE 'n/a'
			END AS prd_line,
			CAST (prd_start_dt AS DATE) AS prd_start_dt,
			CAST (LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		WHERE prd_id IS NOT NULL 
		SET @end_time = GETDATE();
		PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
		PRINT '------------------------------------';

		PRINT ('Truncating Data Into Table : silver.crm_sales_details ');
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT ('Inserting Data Into Table : silver.crm_sales_details ');
		SET @start_time = GETDATE();
		INSERT INTO silver.crm_sales_details (
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
			sls_ord_num ,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN LEN(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL 
				ELSE CAST(CAST (sls_order_dt AS NVARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt = 0 THEN NULL 
				ELSE CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN LEN(sls_due_dt) != 8 OR sls_due_dt = 0 THEN NULL 
				ELSE CAST (CAST (sls_due_dt AS NVARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN ABS(sls_price) *sls_quantity
					 ELSE sls_sales
				END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
					ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
		PRINT '------------------------------------';

		PRINT '';
		PRINT '====================================';
		PRINT 'Loading Data Into ERP tables';
		PRINT '====================================';
		PRINT ('Truncating Data Into Table : silver.erp_cust_az12 ');
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT ('Inserting Data Into Table : silver.erp_cust_az12 ');
		SET @start_time = GETDATE();
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE 
				WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid ,4 , LEN(cid))
				ELSE cid 
			END AS cid,
			CASE 
				WHEN bdate > GETDATE() THEN NULL 
				ELSE bdate
			END AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'Female'
				ELSE 'n/a'
			END AS gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
		PRINT '------------------------------------';

		PRINT ('Truncating Data Into Table : silver.erp_loc_a101');
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT ('Inserting Data Into Table : silver.erp_loc_a101');
		SET @start_time = GETDATE();
		INSERT INTO silver.erp_loc_a101 (
			cid, 
			cntry
		)
		SELECT 
		REPLACE(cid, '-' , '') AS cid,
		CASE 
			WHEN cntry IN ('United States' , 'US') THEN 'USA'
			WHEN cntry = 'DE' THEN 'Germany'
			WHEN cntry = '' OR cntry IS NULL THEN 'n/a' 
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
		PRINT '------------------------------------';

		PRINT ('Truncating Data Into Table : silver.erp_px_cat_g1v2');
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT ('Inserting Data Into Table : silver.erp_px_cat_g1v2');
		SET @start_time = GETDATE();
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT 
			id,
			TRIM(cat) AS cat,
			TRIM(subcat) AS subcat,
			TRIM(maintenance) AS maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
		PRINT '------------------------------------';
	END TRY 

	BEGIN CATCH 
		PRINT '************************************';
		PRINT'silver LOAD ERROR'
		PRINT '************************************';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number - ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State - ' + CAST (ERROR_STATE() AS NVARCHAR);
	END CATCH 
	SET @batch_end_time = GETDATE();
		PRINT '';
		PRINT '************************************';
		PRINT ('Batch Load Duration - ') + CAST ( DATEDIFF(second , @batch_start_time , @batch_end_time)  AS NVARCHAR) + 's.'
		PRINT '************************************';
END
-- EXEC silver.load_silver
