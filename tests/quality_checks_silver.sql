/*
=======================================================================================
Quality Check -> Silver Tables 

Script Purpose :
	This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
=======================================================================================
*/-- Checking crm_cust_info

-- Checking Duplicate customers 
-- Expectation - 0 result
SELECT 
	COUNT(*) AS total_count
FROM silver.crm_cust_info GROUP BY cst_id
HAVING COUNT(*) > 1

-- Leading or Trailing Spaces 
-- Expectation - 0 result
SELECT 
	* 
FROM silver.crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname) 
	  OR cst_lastname != TRIM(cst_lastname)

-- Data Standardization & Consistencyand handling missing values 
-- Expectaion good values
SELECT DISTINCT 
	cst_marital_status
FROM silver.crm_cust_info

SELECT DISTINCT 
	cst_gndr
FROM silver.crm_cust_info

-- Checking crm_prd_info

-- Duplicate prd_key OR NULL
-- Expectation - 0 result
SELECT 
	COUNT(*) AS total_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- Data Normalisation & HANDLING NULLS
SELECT DISTINCT 
	prd_line 
FROM  silver.crm_prd_info

SELECT 
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

-- Checking Date 
SELECT 
	*
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt

-- Checking crm_sales_details

--Checking Date Validity ! 
SELECT 
* 
FROM silver.crm_sales_details 
WHERE sls_order_dt = 0 
	  OR LEN(sls_order_dt) != 8 OR
	  sls_ship_dt = 0 
	  OR LEN(sls_ship_dt) != 8 OR 
	  sls_due_dt = 0 
	  OR LEN(sls_due_dt) != 8 

--Cheking Sales & Price 
SELECT *
FROM silver.crm_sales_details 
WHERE sls_sales != sls_price * sls_quantity 
	  OR sls_price <= 0

--Cheking erp_cust_az12

-- BirthDate Errors 
SELECT 
*
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()

--Normalisation 
SELECT DISTINCT 
gen 
FROM silver.erp_cust_az12

-- Checking erp_loc_a101
SELECT DISTINCT
	cntry 
FROM silver.erp_loc_a101

-- Checking erp_px_cat_g1v2 

SELECT 
	*
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR
	  subcat != TRIM(subcat) OR
	  maintenance != TRIM(maintenance)

SELECT DISTINCT
maintenance 
FROM silver.erp_px_cat_g1v2


SELECT TOP 1 * FROM bronze.crm_sales_details
