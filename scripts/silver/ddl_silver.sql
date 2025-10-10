/*
======================================================================================
DDL script: Create silver tables
======================================================================================
Script Purpose:
	Instead of creating schemas, tables with the prefix 'silver' are created, dropping
    existing tables if they already exist. Run this script to re-define the DDL 
    structure of the silver tables.
======================================================================================
*/

-- select database
USE dwh_project; 

-- drop table if exists
DROP TABLE IF EXISTS silver_crm_cust_info;

-- create table 
CREATE TABLE silver_crm_cust_info(
cst_id 				INT,
cst_key				VARCHAR(50),
cst_firstname		VARCHAR(50),
cst_lastname		VARCHAR(50),
cst_marital_status	VARCHAR(50),
cst_gender 			VARCHAR(50),
cst_create_date 	DATE,
dwh_create_date		DATETIME DEFAULT CURRENT_TIMESTAMP()
);

# =============================================================================================================== #
-- drop table if exists
DROP TABLE IF EXISTS silver_crm_prd_info;

-- create table 
CREATE TABLE silver_crm_prd_info(
prd_id 				INT,
cat_id				VARCHAR(50),
prd_key				VARCHAR(50),
prd_nm				VARCHAR(50),
prd_cost			INT,
prd_line			VARCHAR(50),
prd_start_dt		DATE,
prd_end_dt			DATE,
dwh_create_date		DATETIME DEFAULT CURRENT_TIMESTAMP()
);

# =============================================================================================================== #
-- drop table if exists
DROP TABLE IF EXISTS silver_crm_sales_details;

-- create table 
CREATE TABLE silver_crm_sales_details(
sls_ord_num			VARCHAR(50),
sls_prd_key			VARCHAR(50),
sls_cust_id			INT,
sls_order_dt		DATE,
sls_ship_dt			DATE,
sls_due_dt			DATE,
sls_sales			INT,
sls_quantity		INT,
sls_price			INT,
dwh_create_date		DATETIME DEFAULT CURRENT_TIMESTAMP()
);

# =============================================================================================================== #
-- drop table if exists
DROP TABLE IF EXISTS silver_erp_cust_az12;

-- create table 
CREATE TABLE silver_erp_cust_az12(
cid					VARCHAR(50),
bdate				DATE,
gen					VARCHAR(50),
dwh_create_date		DATETIME DEFAULT CURRENT_TIMESTAMP()
);

# =============================================================================================================== #
-- drop table if exists
DROP TABLE IF EXISTS silver_erp_loc_a101;

-- create table 
CREATE TABLE silver_erp_loc_a101(
cid					VARCHAR(50),
cntry				VARCHAR(50),
dwh_create_date		DATETIME DEFAULT CURRENT_TIMESTAMP()
);

# =============================================================================================================== #
-- drop table if exists
DROP TABLE IF EXISTS silver_erp_px_cat_g1v2;

-- create table 
CREATE TABLE silver_erp_px_cat_g1v2(
id					VARCHAR(50),
cat					VARCHAR(50),
subcat				VARCHAR(50),
maintenance			VARCHAR(50),
dwh_create_date		DATETIME DEFAULT CURRENT_TIMESTAMP()
);
