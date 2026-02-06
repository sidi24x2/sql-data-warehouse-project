
/*
===============================================================================
DDL Script: Create Gold Views

Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

IF OBJECT_ID('gold.dim_customers' , 'V') IS NOT NULL 
	DROP VIEW gold.dim_customers 
GO

-- Creating dim_customers
CREATE VIEW gold.dim_customers AS (
SELECT
	ROW_NUMBER() OVER (ORDER BY c.cst_id ) AS customer_key, -- Surrogate Key
	c.cst_id AS customer_id, 
	c.cst_key AS customer_number,
	c.cst_firstname AS first_name,
	c.cst_lastname AS last_name,
	ecl.cntry AS country,
	CASE 
		WHEN c.cst_gndr = 'n/a' THEN ec.gen
		ELSE C.cst_gndr
	END AS gender,
	c.cst_marital_status AS marital_status,
	ec.bdate AS birth_date,
	c.cst_create_date AS create_date
FROM silver.crm_cust_info AS c
LEFT JOIN silver.erp_cust_az12 AS ec
ON ec.cid = c.cst_key
LEFT JOIN silver.erp_loc_a101 AS ecl
ON ecl.cid = c.cst_key
where ec.gen != c.cst_gndr
)

GO

IF OBJECT_ID('gold.dim_products') IS NOT NULL 
	DROP VIEW gold.dim_products
GO

--CREATING dim_products
CREATE VIEW gold.dim_products AS (
SELECT 
	ROW_NUMBER() OVER(ORDER BY p.prd_start_dt ,p.prd_id) AS product_key, 
	p.prd_id AS product_id,
	p.prd_key AS product_number,
	p.prd_nm AS product_name,
	p.cat_id AS category_id,
	ep.cat AS category,
	ep.subcat AS subcategory,
	ep.maintenance,
	p.prd_cost AS product_cost,
	p.prd_line AS product_line,
	p.prd_start_dt AS start_date
FROM silver.crm_prd_info AS p
LEFT JOIN silver.erp_px_cat_g1v2 AS ep
ON p.cat_id = ep.id
)
GO 


IF OBJECT_ID('gold.fact_sales') IS NOT NULL 
	DROP VIEW gold.fact_sales
GO

-- Creating fact_sales
CREATE VIEW gold.fact_sales AS (
SELECT 
	s.sls_ord_num AS order_number,
	p.prd_key AS product_key,
	c.cst_key AS customer_key,
	s.sls_order_dt AS order_date,
	s.sls_ship_dt AS shipping_date,
	s.sls_due_dt AS due_date,
	s.sls_sales AS sales_amount,
	s.sls_quantity AS quantity,
	s.sls_price AS price
FROM silver.crm_sales_details AS s 
LEFT JOIN silver.crm_cust_info AS c 
ON c.cst_id = s.sls_cust_id
LEFT JOIN silver.crm_prd_info AS p
ON p.prd_key = s.sls_prd_key
)

