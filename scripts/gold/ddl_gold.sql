/*
======================================================================================
DDL script: Create gold views
======================================================================================
Script Purpose:
	This script creates views for the gold layer in the data warehouse. Each view
    transforms and combines data from the silver layer to produce a business-ready
    dataset.
    It logs the start, end and the creating duration of each view into 'loading_log'.
======================================================================================
*/

-- select database
USE dwh_project; 

-- drop views if exists
DROP VIEW IF EXISTS gold_dim_customer;

-- create view
SET @load_start = NOW();

CREATE VIEW gold_dim_customer AS
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) as customer_key, -- creating surrogate key
	ci.cst_id as customer_id,
    ci.cst_key as customer_number,
    ci.cst_firstname as fist_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
        CASE
		WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- crm source acting as master
        ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
    ca.bdate as birthdate,
    ci.cst_create_date as create_date
FROM
    silver_crm_cust_info ci
LEFT JOIN silver_erp_cust_az12 ca
ON	ci.cst_key = ca.cid
LEFT JOIN silver_erp_loc_a101 la
ON ci.cst_key = la.cid
;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('gold_dim_customer', @load_start, @load_end, @load_duration_seconds)
;

# =============================================================================================================== #
-- drop views if exists
DROP VIEW IF EXISTS gold_dim_products;

-- create view
SET @load_start = NOW();

CREATE VIEW gold_dim_products as 
SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_id) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as category_id,
	pc.cat as category,
    pc.subcat as subcategory,
    pc.maintenance as maintenance, 
    pn.prd_cost as cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
FROM
    silver_crm_prd_info pn
LEFT JOIN silver_erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- filter out all historical data    
;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('gold_dim_products', @load_start, @load_end, @load_duration_seconds)
;

# =============================================================================================================== #
-- drop views if exists
DROP VIEW IF EXISTS gold_fact_sales;

-- create view
SET @load_start = NOW();

CREATE VIEW gold_fact_sales AS
SELECT 
    sd.sls_ord_num as order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
FROM
    silver_crm_sales_details sd
LEFT JOIN gold_dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold_dim_customer cu
ON sd.sls_cust_id = cu.customer_id
;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('gold_fact_sales', @load_start, @load_end, @load_duration_seconds)
;
