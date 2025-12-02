/*
==================================================================================
Quality Checks
==================================================================================
Script purpose:
  This script executes various quality checks to ensure data consistency, accuracy,
  and standardization within the 'silver' schema.
It verifies the following aspects:
  - Null (missing) or duplicate primary keys.
  - Unwanted spaces in string fields.
  - Data consistency and standardization.
  - Invalid or illogical date ranges and sequences.
  - Data consistency between related fields.

Usage Notes:
- Run these checks after loading data into 'silver' layer.
- Investigate and resolve any discrepancies detected during the checks.
==================================================================================
*/

-- ==============================================================
-- Checking 'silver.crm_cust_info'
-- ==============================================================

-- Check for Nulls or Duplicates in Primary Keys
-- Expectation: No result

SELECT
cst_id, 
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL;

SELECT *
FROM silver.crm_cust_info
WHERE cst_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: No result

SELECT cst_gndr
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
;

-- Data Standardization & Consistency

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
;

SELECT DISTINCT cst_material_status
FROM silver.crm_cust_info
;

-- ==============================================================
-- Checking 'silver.crm_prd_info'
-- ==============================================================

-- Check for Nulls or Duplicates in Primary Keys
-- Expectation: No result

SELECT
prd_id, 
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id 
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: No result

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)
;

-- Check for NULLs or Negative Numbers
-- Expectation: No Results

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL
;

-- Data Standardization & Consistency

SELECT DISTINCT prd_line
FROM silver.crm_prd_info
;

-- Check for Invalid Date orders:

SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;


SELECT
prd_id, 
prd_key,
prd_nm, 
prd_start_dt, 
prd_end_dt,
DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

SELECT * FROM silver.crm_prd_info;


-- ==============================================================
-- Checking 'silver.crm_sales_details'
-- ==============================================================

-- Check for Valid Dates

SELECT 
NULLIF(sls_due_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 
OR LEN(sls_due_dt) != 8 
OR sls_due_dt > 20500101 
OR sls_due_dt < 19000101;

--CHECK for Invalid Date Orders

SELECT
*
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL; zero, or negative

SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_price <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price
;


-- ==============================================================
-- Checking 'silver.cerp_cust_az12
-- ==============================================================

-- Identify Out-of-Range Dates

SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()
;

-- Data Standardization & Consistency

SELECT DISTINCT 
gen, 
CASE 

	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen_new
FROM bronze.erp_cust_az12;

SELECT DISTINCT 
gen 
FROM silver.erp_cust_az12;

SELECT DISTINCT gen, LEN(gen), DATALENGTH(gen)
FROM bronze.erp_cust_az12;

SELECT DISTINCT 
    gen,
    LEN(gen) AS len_chars,
    DATALENGTH(gen) AS len_bytes,
    '"' + gen + '"' AS value_shown
FROM bronze.erp_cust_az12;

SELECT DISTINCT
    gen,
    CASE 
        WHEN UPPER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            LTRIM(RTRIM(gen)),
                            CHAR(160), ''),       -- non-breaking space 
                        CHAR(9), ''),            -- Tab (\t)
                    CHAR(10), ''),             -- Linefeed (\n)
                CHAR(13), '')                 -- Carriage return (\r)
        ) IN ('F', 'FEMALE') THEN 'Female'

        WHEN UPPER(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            LTRIM(RTRIM(gen)),
                            CHAR(160), ''
                        ), 
                        CHAR(9), ''
                    ), 
                    CHAR(10), ''
                ), 
                CHAR(13), ''
            )
        ) IN ('M', 'MALE') THEN 'Male'

        ELSE 'n/a'
    END AS gen_clean
FROM bronze.erp_cust_az12;


-- ==============================================================
-- Checking 'silver.erp_loc_a101'
-- ==============================================================

-- Data Standardization & Consistency

SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry;

SELECT DISTINCT cntry,
LEN(cntry) AS len_chars,
    DATALENGTH(cntry) AS len_bytes,
    '"' + cntry + '"' AS value_shown
FROM silver.erp_loc_a101
ORDER BY cntry;


SELECT DISTINCT
    cntry,
    CASE 
        WHEN ca.cleaned = 'DE' THEN 'Germany'
        WHEN ca.cleaned IN ('US', 'USA') THEN 'United States'
        WHEN ca.cleaned = '' OR ca.cleaned IS NULL THEN 'n/a'
        ELSE cntry
    END AS cntry_clean
FROM bronze.erp_loc_a101
CROSS APPLY (
    SELECT UPPER(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        TRIM(cntry),
                        CHAR(160), ''
                    ),
                    CHAR(9), ''
                ),
                CHAR(10), ''
            ),
            CHAR(13), ''
        )
    ) AS cleaned
) ca; -- Cross Apply-Block

SELECT cntry, COUNT(*) AS cnt
FROM silver.erp_loc_a101
GROUP BY cntry
ORDER BY cnt DESC;

SELECT cid, cntry, COUNT(*) 
FROM silver.erp_loc_a101
GROUP BY cid, cntry
HAVING COUNT(*) > 1;

-- ==============================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ==============================================================

-- Check for unwanted Spaces

SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency

SELECT DISTINCT 
maintenance,
LEN(maintenance) as len_chars,
DATALENGTH(maintenance) as len_bytes,
'"' + maintenance + '"' AS value_shown
FROM silver.erp_px_cat_g1v2;
