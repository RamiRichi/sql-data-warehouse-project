/*
==============================================================================
Quality Checks
==============================================================================
Script Purpose:
  This script performs quality checks to validate the integrity, consistency,
  and accuracy of the Gold Layer. These checks ensure:
  - Uniqueness of durrogate keys in dimension tables.
  - Referential integrity between fact and dimension tables.
  - Validation of relationships in the data model for analyical purposes.

Usage Notes:
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks.
==============================================================================
*/
-- ===========================================================================
-- Check 'gold.dim_customers'
--============================================================================
-- check for uniqueness of customer key in gold.dim_costumer
-- Ecpectation: No results
select
customer_key,
count(*) as duplicate_count
from gold.dim_customers
group by customer_key
having count(*) > 1
-- ===========================================================================
-- Check 'gold.dim_products'
--============================================================================
-- === Mismatch in join condition check====
SELECT DISTINCT sd.sls_prd_key, pr.product_number 
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number;
-- === Mismatch in data type check====
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'dim_customer';
-- ===========================================================================
-- Check 'gold.fact_sales'
--============================================================================

-- Foreign key integrity (Dimensions)
select *
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null
