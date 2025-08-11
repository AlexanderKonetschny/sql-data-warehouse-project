/*
======================================================================================
Loading script: Load bronze layer (source -> bronze)
======================================================================================
Script Purpose:
	This script loads data into the 'bronze' layer from external CSV files by performing
    the following actions:
		- Truncate the bronze table before loading data
        - Uses the 'LOAD DATA LOCAL INFILE' command
        - logs the start, end and the loading duration of each table into 'bronze_log_load'
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
    IGNORE 1 LINES;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO bronze_log_load(table_name, load_start, load_end, load_duration_seconds)
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
    IGNORE 1 LINES;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO bronze_log_load(table_name, load_start, load_end, load_duration_seconds)
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
    IGNORE 1 LINES;

SET @load_end = NOW();
SET @load_duration_seconds = TIMESTAMPDIFF(SECOND, @load_start, @load_end);

INSERT INTO bronze_log_load(table_name, load_start, load_end, load_duration_seconds)
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

INSERT INTO bronze_log_load(table_name, load_start, load_end, load_duration_seconds)
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

INSERT INTO bronze_log_load(table_name, load_start, load_end, load_duration_seconds)
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

INSERT INTO bronze_log_load(table_name, load_start, load_end, load_duration_seconds)
	VALUES('bronze_erp_px_cat_g1v2', @load_start, @load_end, @load_duration_seconds)
;
