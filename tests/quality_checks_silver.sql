/*
========================================================================================================================
Quality Checks
========================================================================================================================

Script Purpose:
    This script performs various quality checks for data consistency, accuracy,
    and standardization across the 'silver' schemas. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
========================================================================================================================
*/

-- Check for invalid dates

SELECT
NULLIF(sls_order_dt, 0) sls_order_dt -- To replace zero thith NULL
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101 -- To check if we do have the dates within the frame
OR sls_order_dt < 19000101  -- To check if we do have the dates within the frame


-- Check for Invalid DAte Orders 
SELECT * 
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- Check Data Consistency: Between Sales, Quanity, and Price
-->> Sales = Quanity * Price
--> Value most not be NULL, zero, or negative. 

SELECT DISTINCT
    sls_sales AS old_sls_sales, 
    sls_quantity, 
    sls_price AS old_sls_price,

    CASE -- The CASE expression is used to correct the calculation between the three columns.
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) -- ABS added to convert negative to positive
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price


-- This part about the calcualtions to check if it's correct 
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL  
   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

SELECT * FROM silver.crm_sales_details
-----------------------------------------------------------------------------------------------------

-- Identify out-of-range dates

SELECT DISTINCT 
bdate
FROM bronze.erp_cust_az12
WHERE bdate > ' 1924-01-01' OR bdate > GETDATE()

-- DATA Standardization & Consistency
SELECT DISTINCT 
gen
FROM bronze.erp_cust_az12

---------------------------------------------------------

INSERT INTO silver.erp_px_cat_g1v2 
(id, cat, subcat, maintenance)
SELECT 
id, 
cat, 
subcat, 
maintenance
FROM bronze.erp_px_cat_g1v2


-- Checking for unwnted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM (maintenance)

-- Data Standardization and Consistency
SELECT DISTINCT 
cat 
FROM bronze.erp_px_cat_g1v2


SELECT * FROM silver.erp_px_cat_g1v2
--------------------------------------------------------------------------------

SELECT 
REPLACE(cid, '-', '') cid, --- Handled -
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN ' United State'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'  -- handled missing values
	ELSE TRIM(cntry)
END AS cntry

FROM bronze.erp_loc_a101


-- Data Standardization and Consistency
SELECT DISTINCT cntry 
FROM bronze.erp_loc_a101
ORDER BY cntry

--WHERE REPLACE(cid, '-', '') NOT IN (SELECT cst_key FROM silver.crm_cust_info) -- Using this to find any unmatching data.

-----------------------------------------------------------------------------------------

SELECT 
	prd_id, 
	prd_key, 
	prd_nm, 
	prd_cost, 
	prd_line, 
	prd_start_dt, 
	prd_end_dt
FROM bronze.crm_prd_info


SELECT prd_id, COUNT (*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;



-- To check if there is any spaces 
SELECT prd_nm
FROM  bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm); 

-- Check for NULLS or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM  bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM  bronze.crm_prd_info;

-- Check for Invalid Date Orders
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt  ;



SELECT * FROM silver.crm_prd_info

/*SELECT 
    prd_id,
    prd_key,
    prd_nm,
    prd_start_dt,
    prd_end_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');*/


SELECT 
    prd_id, 
    prd_key, 
    prd_nm, 
    prd_start_dt, 
    prd_end_dt,
    DATEADD(day, -1,
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
    ) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

------------------------------------------------------------------------------------------------

SELECT cst_gndr , COUNT(*) AS Total_Null FROM bronze.crm_cust_info
WHERE cst_gndr IS NULL 
GROUP BY cst_gndr 

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT cst_id, COUNT(*) AS duplicate FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

---------------------------------------------------------

-- Check for unwanted spaces
-- Expectation: No Reults
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr 
FROM bronze.crm_cust_info

SELECT DISTINCT cst_marital_status 
FROM bronze.crm_cust_info

-------------------------------------------------------------------------------

SELECT cst_gndr , COUNT(*) AS Total_Null FROM bronze.crm_cust_info
WHERE cst_gndr IS NULL 
GROUP BY cst_gndr 

-- Check for Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT cst_id, COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;


-- Check for unwanted spaces
-- Expectation: No Reults
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr 
FROM bronze.crm_cust_info

SELECT DISTINCT cst_gndr 
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info




 

