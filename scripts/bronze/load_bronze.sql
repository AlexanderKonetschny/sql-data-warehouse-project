/*
======================================================================================
Loading script: Load bronze layer (source -> bronze)
======================================================================================
Script Purpose:
	This script loads data into the 'bronze' layer from external CSV files by performing
    the following actions:
		- Truncate the bronze table before loading data
        - Uses the 'LOAD DATA LOCAL INFILE' command
        - logs the start, end and the loading duration of each table into 'loading_log'
======================================================================================
*/
-- for loading csv-files
SET GLOBAL local_infile=1; 

# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE bronze_crm_cust_info;

-- loading csv-file
SET @load_start = NOW();

LOAD DATA LOCAL INFILE 'C:/Users/AKony/OneDrive/Desktop/mySQL-data-warehouse-project/datasets/source_crm/cust_info.csv' # path to files
INTO TABLE bronze_crm_cust_info
FIELDS TERMINATED BY ','
    IGNORE 1 LINES
(@cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gender, cst_create_date)	-- INT set NULL if empty
	SET cst_id = NULLIF(@cst_id, '');
;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('bronze_crm_cust_info', @load_start, @load_end, @load_duration_seconds)
;

# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE bronze_crm_prd_info;

-- loading csv-file
SET @load_start = NOW();

LOAD DATA LOCAL INFILE 'C:/Users/AKony/OneDrive/Desktop/mySQL-data-warehouse-project/datasets/source_crm/prd_info.csv' # path to files
INTO TABLE bronze_crm_prd_info
FIELDS TERMINATED BY ','
    IGNORE 1 LINES
(@prd_id, prd_key, prd_nm, @prd_cost, prd_line, prd_start_dt, prd_end_dt) -- INT set NULL if empty
SET 
	prd_id =	 NULLIF(@prd_id, ''), 
	prd_cost =	 NULLIF(@prd_cost, '')
;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('bronze_crm_prd_info', @load_start, @load_end, @load_duration_seconds)
;
# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE bronze_crm_sales_details;

-- loading csv-file
SET @load_start = NOW();

LOAD DATA LOCAL INFILE 'C:/Users/AKony/OneDrive/Desktop/mySQL-data-warehouse-project/datasets/source_crm/sales_details.csv' # path to files
INTO TABLE bronze_crm_sales_details
FIELDS TERMINATED BY ','
    IGNORE 1 LINES
(sls_ord_num, sls_prd_key, @sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, @sls_sales, @sls_quantity, @sls_price) -- INT set NULL if empty
SET 
	sls_cust_id =	 NULLIF(@sls_cust_id, ''), 
	sls_order_dt =	 NULLIF(@sls_order_dt, ''),
    sls_ship_dt =	 NULLIF(@sls_ship_dt, ''),
    sls_due_dt =	 NULLIF(@sls_due_dt, ''),
    sls_sales =	 	 NULLIF(@sls_sales, ''),
    sls_quantity =	 NULLIF(@sls_quantity, ''),
    sls_price =		 NULLIF(@sls_price, '')
;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('bronze_crm_sales_details', @load_start, @load_end, @load_duration_seconds)
;   
# =============================================================================================================== #
# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE bronze_erp_cust_az12;

-- loading csv-file
SET @load_start = NOW();

LOAD DATA LOCAL INFILE 'C:/Users/AKony/OneDrive/Desktop/mySQL-data-warehouse-project/datasets/source_erp/cust_az12.csv' # path to files
INTO TABLE bronze_erp_cust_az12
FIELDS TERMINATED BY ','
    IGNORE 1 LINES;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('bronze_erp_cust_az12', @load_start, @load_end, @load_duration_seconds)
;   
# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE bronze_erp_loc_a101;

-- loading csv-file
SET @load_start = NOW();

LOAD DATA LOCAL INFILE 'C:/Users/AKony/OneDrive/Desktop/mySQL-data-warehouse-project/datasets/source_erp/loc_a101.csv' # path to files
INTO TABLE bronze_erp_loc_a101
FIELDS TERMINATED BY ','
    IGNORE 1 LINES;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('bronze_erp_loc_a101', @load_start, @load_end, @load_duration_seconds)
;
# =============================================================================================================== #

-- truncate table before loading
TRUNCATE TABLE bronze_erp_px_cat_g1v2;

-- loading csv-file
SET @load_start = NOW();

LOAD DATA LOCAL INFILE 'C:/Users/AKony/OneDrive/Desktop/mySQL-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv' # path to files
INTO TABLE bronze_erp_px_cat_g1v2
FIELDS TERMINATED BY ','
    IGNORE 1 LINES;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO loading_log(table_name, load_start, load_end, load_duration_seconds)
	VALUES('bronze_erp_px_cat_g1v2', @load_start, @load_end, @load_duration_seconds)
;
