/*
===================================================================
STORED PROCEDURE - Load Bronze Layer (Source -> Bronze )

Script Purpose - 
		This Stored Procedure loads the data to the bronze tables
		from external CSV files. 
		It first truncates the bronze tables then uses the BULK 
		INSERT to load the data to the tables. 

Parameters : 
		This stored procedure does not accept any parameter or 
		return any value 

Usage : EXEC bronze.load_bronze;
===================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
DECLARE @start_time DATETIME , @end_time DATETIME, @batch_start_time DATETIME , @batch_end_time DATETIME
BEGIN TRY 
	SET @batch_start_time = GETDATE();
	PRINT '************************************';
	PRINT 'Loading Bronze Tables';
	PRINT '************************************';
	PRINT '';
	PRINT '====================================';
	PRINT 'Loading Data Into CRM tables';
	PRINT '====================================';

	PRINT ('Truncating Data Into Table : bronze.crm_cust_info');
	TRUNCATE TABLE bronze.crm_cust_info;
	PRINT ('Inserting Data Into Table : bronze.crm_cust_info');
	SET @start_time = GETDATE();
	BULK INSERT bronze.crm_cust_info
	FROM 'C:\Users\insti\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2 ,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
	SET @end_time = GETDATE();
	PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
	PRINT '------------------------------------';

	PRINT ('Truncating Data Into Table : bronze.crm_prd_info ');
	TRUNCATE TABLE bronze.crm_prd_info;
	PRINT ('Inserting Data Into Table : bronze.crm_prd_info ');
	SET @start_time = GETDATE();
	BULK INSERT bronze.crm_prd_info 
	FROM 'C:\Users\insti\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2 ,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
	SET @end_time = GETDATE();
	PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
	PRINT '------------------------------------';

	PRINT ('Truncating Data Into Table : bronze.crm_sales_details ');
	TRUNCATE TABLE bronze.crm_sales_details;
	PRINT ('Inserting Data Into Table : bronze.crm_sales_details ');
	SET @start_time = GETDATE();
	BULK INSERT bronze.crm_sales_details 
	FROM 'C:\Users\insti\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2 ,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
	SET @end_time = GETDATE();
	PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
	PRINT '------------------------------------';

	PRINT '';
	PRINT '====================================';
	PRINT 'Loading Data Into ERP tables';
	PRINT '====================================';
	PRINT ('Truncating Data Into Table : bronze.erp_cust_az12 ');
	TRUNCATE TABLE bronze.erp_cust_az12;
	PRINT ('Inserting Data Into Table : bronze.erp_cust_az12 ');
	SET @start_time = GETDATE();
	BULK INSERT bronze.erp_cust_az12 
	FROM 'C:\Users\insti\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
	WITH (
		FIRSTROW = 2 ,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
	SET @end_time = GETDATE();
	PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
	PRINT '------------------------------------';

	PRINT ('Truncating Data Into Table : bronze.erp_loc_a101');
	TRUNCATE TABLE bronze.erp_loc_a101;
	PRINT ('Inserting Data Into Table : bronze.erp_loc_a101');
	SET @start_time = GETDATE();
	BULK INSERT bronze.erp_loc_a101
	FROM 'C:\Users\insti\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
	WITH (
		FIRSTROW = 2 ,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
	SET @end_time = GETDATE();
	PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
	PRINT '------------------------------------';

	PRINT ('Truncating Data Into Table : bronze.erp_px_cat_g1v2');
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	PRINT ('Inserting Data Into Table : bronze.erp_px_cat_g1v2');
	SET @start_time = GETDATE();
	BULK INSERT bronze.erp_px_cat_g1v2 
	FROM 'C:\Users\insti\OneDrive\Desktop\SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
	WITH (
		FIRSTROW = 2 ,
		FIELDTERMINATOR = ',',
		TABLOCK
		)
	SET @end_time = GETDATE();
	PRINT ('Load Duration - ') + CAST ( DATEDIFF(second , @start_time , @end_time)  AS NVARCHAR) + 's.'
	PRINT '------------------------------------';
END TRY 

BEGIN CATCH 
	PRINT '************************************';
	PRINT'BRONZE LOAD ERROR'
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

-- EXEC bronze.load_bronze

