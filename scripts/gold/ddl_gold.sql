/*
===========================================================================
DDL Script: Create Gold Views
===========================================================================
Script Purpose:
  This cript creates views for the Gold layer in the data warehouse.
  The Gold layer represents the final dimension and fact tables (Star Schema)
  
  Each view performs transformations and combines data from the Silver layer
  to produce a clean, enriched, and business-ready dataset.

Usage:
  - These views can be queried directly for analytics and reporting.
===========================================================================
*/
--==========================================================================
-- Create Dimension: gold.dim_customers
--==========================================================================
if OBJECT_ID('gold.dim_customers', 'V') is not null
    Drop View gold.dim_customers
GO
create VIEW gold.dim_customers AS
SELECT
	row_number() over(order by cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_material_status AS material_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr --CRM is the Master for genger info
		else coalesce(ca.gen, 'n/a')
	end as gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
	ON	ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
	ON	ci.cst_key = la.cid

--==========================================================================
-- Create Dimension: gold.dim_product
--==========================================================================

if OBJECT_ID('gold.dim_products', 'V') is not null
    Drop View gold.dim_products
GO
  create view gold.dim_products AS
Select 
row_number() over (order by pn.prd_start_dt , pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date


from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.prd_key = pc.id
where prd_end_dt is null -- filter out all historical data

--==========================================================================
-- Create Dimension: gold.fact_sales
--==========================================================================

if OBJECT_ID('gold.fact_sales', 'V') is not null
    Drop View gold.fact_sales
GO
create view gold.fact_sales AS
select
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.category_id
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id
