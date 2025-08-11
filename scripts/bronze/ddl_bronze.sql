/*
======================================================================================
DDL script: Create bronze tables
======================================================================================
Script Purpose:
	Instead of creating schemas, tables with the prefix 'bronze' are created, dropping
    existing tables if they already exist. Run this script to re-define the DDL 
    structure of the bronze tables.
======================================================================================
*/

-- select database
USE dwh_project; 


-- drop table if exists
DROP TABLE IF EXISTS bronze_crm_cust_info;

-- create table 
CREATE TABLE bronze_crm_cust_info(
cst_id 				INT,
cst_key				VARCHAR(50),
cst_firstname		VARCHAR(50),
cst_lastname		VARCHAR(50),
cst_marital_status	VARCHAR(50),
cst_gender 			VARCHAR(50),
cst_create_date 	DATE
);

# =============================================================================================================== #
-- drop table if exists
DROP TABLE IF EXISTS bronze_crm_prd_info;

-- create table 
CREATE TABLE bronze_crm_prd_info(
prd_id 				INT,
prd_key				VARCHAR(50),
prd_nm				VARCHAR(50),
prd_cost			INT,
prd_line			VARCHAR(50),
prd_start_dt		DATE,
prd_end_dt			DATE
);

# =============================================================================================================== #
-- drop table if exists
DROP TABLE IF EXISTS bronze_crm_sales_details;

-- create table 
CREATE TABLE bronze_crm_sales_details(
sls_ord_num			VARCHAR(50),
sls_prd_key			VARCHAR(50),
sls_cust_id			INT,
sls_order_dt		INT,
sls_ship_dt			INT,
sls_due_dt			INT,
sls_sales			INT,
sls_quantity		INT,
sls_price			INT
);

# =============================================================================================================== #
-- drop table if exists
DROP TABLE IF EXISTS bronze_erp_cust_az12;

-- create table 
CREATE TABLE bronze_erp_cust_az12(
CID					VARCHAR(50),
BDATE				DATE,
GEN					VARCHAR(50)
);

# =============================================================================================================== #
-- drop table if exists
DROP TABLE IF EXISTS bronze_erp_loc_a101;

-- create table 
CREATE TABLE bronze_erp_loc_a101(
CID					VARCHAR(50),
CNTRY				VARCHAR(50)
);

# =============================================================================================================== #
-- drop table if exists
DROP TABLE IF EXISTS bronze_erp_px_cat_g1v2;

-- create table 
CREATE TABLE bronze_erp_px_cat_g1v2(
ID					VARCHAR(50),
CAT					VARCHAR(50),
SUBCAT				VARCHAR(50),
MAINTENANCE			VARCHAR(50)
);

