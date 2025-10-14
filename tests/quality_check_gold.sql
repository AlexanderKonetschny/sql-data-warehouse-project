/*
======================================================================================
testing script: check quality of gold layer
======================================================================================
Script Purpose:
	This script performs quality checks to validate integrity, consistency and 
    accuracy of the gold layer. These includes that:
		- the surrogate keys in the dimension tables are unique
        - the referential integrity between the fact and dimension tables are ensured
        - the relationships in the data model for analytical purposes are validated
======================================================================================
*/

# =============================================================================================================== #
# checking gold_dim_customer
# =============================================================================================================== #
-- check for uniqueness of customer_key (surrogate key)
-- expectation: no result
SELECT
	customer_key,
    COUNT(*) as duplicates_count
FROM
    gold_dim_customer
GROUP BY customer_key
HAVING COUNT(*) >1
;

# =============================================================================================================== #
# checking gold_dim_products
# =============================================================================================================== #
-- check for uniqueness of product_key (surrogate key)
-- expectation: no result
SELECT 
    product_key,
    COUNT(*) as duplicates_count
FROM
    gold_dim_products
GROUP BY product_key
HAVING COUNT(*) > 1
;

# =============================================================================================================== #
# checking gold_fact_sales
# =============================================================================================================== #
-- check for data model connectivity between fact and dimensions
-- expectation: no result 
SELECT 
    *
FROM
    gold_fact_sales f
LEFT JOIN gold_dim_customer cu
ON f.customer_key = cu.customer_key
LEFT JOIN gold_dim_products pr
ON f.product_key = pr.product_key
WHERE cu.customer_key IS NULL OR pr.product_key IS NULL 
;
