/*
============================================================================
Quality Checks
============================================================================
Script Purpose:
  This Script performs various quality checks for data consistency, accuracy,
  and standardization across the 'silver' schema. It includes checks for:
  - Null or duplicate primary keys.
  - unwanted spaces in string fields.
  - Data standardization and consistency.
  - Invalid date ranges and orders.
  - Data consistency between related fields.

Usage Notes:
  - Run these checks after data loading silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
============================================================================
*/
-- ==========================================================================
-- Checking 'silver.crm_cust_info'
-- ==========================================================================

-- Check for Nulls or Doplicates in Primarz Key
-- Expectation:No Result

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
having count(*) > 1 OR cst_id IS NULL

-- Check for unwanted Spaces
-- EXpectation: NO Results
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

-- Data Standardization & Consistency

SELECT DISTINCT cst_create_date
FROM silver.crm_cust_info
WHERE  cst_create_date IS NULL

SELECT * FROM silver.crm_cust_info
WHERE cst_id = 29424

-- Check for Nulls or Doplicates in Primarz Key
-- Expectation:No Result

SELECT
prd_id,
prd_key,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_key
having count(*) > 1 OR prd_key IS NULL
