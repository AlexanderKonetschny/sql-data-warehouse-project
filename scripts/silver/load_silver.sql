/*
======================================================================================
Loading script: Load silver layer (bronze -> silver)
======================================================================================
Script Purpose:
	This script loads data into the 'silver' layer from the bronze layer by performing
    the following actions:
		- Truncate the silver table before loading data
        - Uses the 'INSERT INTO' command to load transformed and cleansed data from
          bronze layer
        - logs the start, end and the loading duration of each table into 'loading_log'
======================================================================================
*/
# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE silver_crm_cust_info;

-- loading silver_crm_cust_info
SET @load_start = NOW();

INSERT INTO silver_crm_cust_info(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gender, cst_create_date)
SELECT 
	cst_id,
    cst_key,
    TRIM(cst_firstname) as cst_firstname,
    TRIM(cst_lastname) as cst_lastname,
    CASE
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'n/a'
	END as cst_marital_status,
    CASE 
		WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END as cst_gender,
    cst_create_date
FROM(
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
FROM bronze_crm_cust_info
WHERE cst_id IS NOT NULL) t 
WHERE flag_last = 1;


SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('silver_crm_cust_info', @load_start, @load_end, @load_duration_seconds)
;
# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE silver_crm_prd_info;

-- loading silver_crm_prd_info
SET @load_start = NOW();

INSERT INTO silver_crm_prd_info(prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
SELECT 
	prd_id,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id, 				-- extract cat_id
    SUBSTRING(prd_key, 7, CHAR_LENGTH(prd_key)) as prd_key, 		-- extract prd_key
    prd_nm,
    IFNULL(prd_cost, 0) as prd_cost,
        CASE UPPER(TRIM(prd_line)) 
		WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN  'Touring'
        ELSE 'n/a'
    END as prd_line,
    prd_start_dt,
    DATE_SUB(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY)
    as prd_end_dt
FROM bronze_crm_prd_info
;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('silver_crm_prd_info', @load_start, @load_end, @load_duration_seconds)
;
# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE silver_crm_sales_details;

-- loading silver_crm_sales_details
SET @load_start = NOW();

INSERT INTO silver_crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE
	WHEN sls_order_dt <= 0 THEN NULL
    WHEN CHAR_LENGTH(CAST(sls_order_dt as CHAR)) != 8 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_order_dt AS CHAR),'%Y%m%d')
	END AS sls_order_dt,
CASE
	WHEN sls_ship_dt <= 0 THEN NULL
    WHEN CHAR_LENGTH(CAST(sls_ship_dt as CHAR)) != 8 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_ship_dt AS CHAR),'%Y%m%d')
	END AS sls_ship_dt,
CASE
	WHEN sls_due_dt <= 0 THEN NULL
    WHEN CHAR_LENGTH(CAST(sls_due_dt as CHAR)) != 8 THEN NULL
    ELSE STR_TO_DATE(CAST(sls_due_dt AS CHAR),'%Y%m%d')
	END AS sls_due_dt,
/*
BUSINESS RULES TO FOLLOW FOR UPCOMING COLUMNS:
- if sales is negative, zero or null 	-> derive it using quantity and price
- if price is zero or null 				-> calculate it using sales and quantity
- if price is negative 					-> convert it to a positive value
*/
CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales 
	END AS sls_sales,
sls_quantity,
CASE
		WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price
    END as sls_price
FROM bronze_crm_sales_details
;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('silver_crm_sales_details', @load_start, @load_end, @load_duration_seconds)
;
# =============================================================================================================== #
# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE silver_erp_cust_az12;

-- loading silver_erp_cust_az12
SET @load_start = NOW();

INSERT INTO silver_erp_cust_az12(cid, bdate, gen)
SELECT
    CASE
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, CHAR_LENGTH(cid))
        ELSE cid
	END as cid,
    CASE
		WHEN bdate > CURDATE() THEN NULL
        ELSE bdate
	END AS bdate,
    CASE
		WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        ELSE 'n/a'
	END AS gen
FROM bronze_erp_cust_az12
;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('silver_erp_cust_az12', @load_start, @load_end, @load_duration_seconds)
;
# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE silver_erp_loc_a101;

-- loading silver_erp_loc_a101
SET @load_start = NOW();

INSERT INTO silver_erp_loc_a101(cid, cntry)
SELECT
    REPLACE(cid, '-','') AS cid,
    CASE
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
	END AS cntry
FROM
    bronze_erp_loc_a101
;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('silver_erp_loc_a101', @load_start, @load_end, @load_duration_seconds)
;
# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE silver_erp_px_cat_g1v2;

-- loading silver_erp_px_cat_g1v2
SET @load_start = NOW();

INSERT INTO silver_erp_px_cat_g1v2(id, cat, subcat, maintenance)
SELECT 
    id,
    TRIM(cat) as cat,
    TRIM(subcat) as subcat,
    TRIM(maintenance) as maintenance
FROM
    bronze_erp_px_cat_g1v2;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('silver_erp_px_cat_g1v2', @load_start, @load_end, @load_duration_seconds)
;
