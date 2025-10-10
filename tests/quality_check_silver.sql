/*
======================================================================================
testing script: check quality of silver layer
======================================================================================
Script Purpose:
	This script performs quality checks for data consistency, accuracy and standardizations
    in the silver layer including checks for:
    	- null or duplicates in primary keys
        - unwanted spaces in string fields
        - data standardization and consistency
        - invalid date ranges and orders
        - data consistency between related fields
======================================================================================
*/

# =============================================================================================================== #
# checking table silver_crm_cust_info
# =============================================================================================================== #
-- check for Nulls or duplicates in primary key
-- expectation: no result
SELECT
	cst_id,
    count(*)
FROM silver_crm_cust_info
GROUP BY cst_id
HAVING count(*) >1 OR cst_id IS NULL;

-- check for unwanted spaces in string columns
-- expectation: no result
SELECT
	*
FROM silver_crm_cust_info
WHERE 	cst_key != TRIM(cst_key)
	OR 	cst_firstname != TRIM(cst_firstname)
    OR 	cst_lastname != TRIM(cst_lastname)
    OR 	cst_marital_status != TRIM(cst_marital_status)
    OR 	cst_gender != TRIM(cst_gender);

-- data standardization & consistency
SELECT distinct cst_marital_status FROM silver_crm_cust_info;
SELECT distinct	cst_gender FROM silver_crm_cust_info;

# =============================================================================================================== #
# checking table silver_crm_prd_info
# =============================================================================================================== #
-- check for Nulls or duplicates in primary key
-- expectation: no result
SELECT
	prd_id,
    count(*)
FROM silver_crm_prd_info
GROUP BY prd_id
HAVING count(*) >1 OR prd_id IS NULL;

-- check for unwanted spaces in string columns
-- expectation: no result
SELECT
	*
FROM silver_crm_prd_info
WHERE 	prd_nm != TRIM(prd_nm);

-- check for NULLs or negative numbers
-- expectation: no result
SELECT
	prd_cost
FROM silver_crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- data standardization & consistency
SELECT DISTINCT 
    prd_line 
FROM silver_crm_prd_info;

-- check for invalid date orders
SELECT 
	*
FROM
	silver_crm_prd_info
WHERE prd_start_dt > prd_end_dt
;
# =============================================================================================================== #
# checking table silver_crm_sales_details
# =============================================================================================================== #
-- check for invalid date orders 
-- expectation: no results
SELECT 
    *
FROM silver_crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
OR sls_order_dt > sls_due_dt 
OR sls_order_dt > sls_due_dt
;

-- check for data consistency: sales = quantity * price 
-- expectation: no results
SELECT DISTINCT
	sls_sales,
    sls_quantity,
    sls_price
FROM silver_crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0    
ORDER BY sls_sales, sls_quantity, sls_price
;
# =============================================================================================================== #
# checking table silver_erp_cust_az12
# =============================================================================================================== #
-- checking for valid birthdates (between 1920 and current date)
-- expectation: only birthdates < 1920
SELECT DISTINCT 
    bdate 
FROM silver_erp_cust_az12
WHERE bdate < '1920-01-01' 
   OR bdate > CURDATE()
;

-- data standardization & consistency
-- expectation: 'Male', 'Female' and 'n/a'
SELECT DISTINCT gen FROM silver_erp_cust_az12;

# =============================================================================================================== #
# checking table silver_erp_cust_az12
# =============================================================================================================== #
-- data standardization & consistency
-- expectation: 'Australia', 'United States', 'Canada', 'Germany',
--              'United Kingdom', 'France' and 'n/a'
SELECT DISTINCT cntry FROM silver_erp_loc_a101;

# =============================================================================================================== #
# checking table silver_erp_px_cat_g1v2
# =============================================================================================================== #
-- check for unwanted spaces in string columns
-- expectation: no result
SELECT
	*
FROM silver_erp_px_cat_g1v2
WHERE cat != TRIM(cat)
	OR subcat != TRIM(subcat)
	OR maintenance != TRIM(maintenance);
    
-- data standardization & consistency
SELECT DISTINCT
	cat
FROM silver_erp_px_cat_g1v2;

SELECT DISTINCT
	subcat
FROM silver_erp_px_cat_g1v2;

SELECT DISTINCT
	maintenance
FROM silver_erp_px_cat_g1v2;
