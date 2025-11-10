--Fact table
CREATE VIEW gold.fact_sales as 
SELECT 
sd.sls_ord_num as order_number,
pr.product_number,  --add surrogant id
--sd.sls_prd_key,
--sd.sls_cust_id,
cu.customer_key,    --add surrogant id
sd.sls_ord_dt as order_date,
sd.sls_ship_dt as shiping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr on sd.sls_prd_key=pr.Product_key
LEFT JOIN gold.dim_customers cu on sd.sls_cust_id=cu.Customer_id

--Foriegn key integrity(dimension)-check if all dimensions table can successfully join tp the fact table
SELECT *FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c 
ON c.customer_key=f.customer_key 
LEFT JOIN gold.dim_products p
on p.product_number=f.product_number
WHERE p.product_number is null
******************************************************************************
CREATE VIEW gold.dim_products as
SELECT 
ROW_NUMBER()OVER(ORDER BY pi.prd_start_dt,pi.prd_key) as product_number,
pi.prd_id as Product_id,
pi.prd_key as Product_key,
pi.prd_nm as Product_name,
pi.cat_id as Category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintetance,
pi.prd_cost as Product_cost,
pi.prd_line as Product_line,
pi.prd_start_dt
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc on pi.cat_id=pc.id
WHERE pi.prd_end_dt is null --Filter out historical data


--SELECT *FROM silver.erp_px_cat_g1v2
--SELECT *FROM silver.crm_prd_info
/*
--CHECK IF WE HAVE DUPPLICATE NUMBER OF KEYS

SELECT product_key,COUNT(*) FROM (
SELECT 
ROW_NUMBER()OVER(ORDER BY pi.prd_start_dt,pi.prd_key) as product_number,
pi.prd_id as Product_id,
pi.prd_key as Product_key,
pi.prd_nm as Product_name,
pi.cat_id as Category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintetance,
pi.prd_cost as Product_cost,
pi.prd_line as Product_line,
pi.prd_start_dt

--pi.prd_end_dt
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc on pi.cat_id=pc.id
WHERE pi.prd_end_dt is null) t --Filter out historical data
GROUP BY product_key
HAVING COUNT(*)>1
*/
*************************************************************************************
CREATE VIEW gold.dim_customers AS
SELECT
*
FROM(
SELECT
   ROW_NUMBER()OVER(ORDER BY cst_id ) AS customer_key,     --Surogant key
    ci.cst_id as Customer_id,
    ROW_NUMBER()OVER(PARTITION BY ci.cst_id ORDER BY ci.cst_create_date DESC) as newid,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    la.cntry as country,
    ci.cst_marital_status as marital_status,
    --ci.cst_gndr,
    ci.cst_create_date as create_date,
    ca.bdate as birthdate,
    --ca.gen,
    CASE WHEN ci.cst_gndr != 'UNKNOWN' THEN ci.cst_gndr --CRM is Master for gen
     ELSE COALESCE(ca.gen,'UNKOWN')
    END as newgen
    
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid)t 
WHERE newid=1;  --Filter dupplicate


----------------------------------
SELECT DISTINCT
    ci.cst_gndr,
    ca.gen,
     CASE WHEN ci.cst_gndr != 'UNKNOWN' THEN ci.cst_gndr
     ELSE COALESCE(ca.gen,'UNKOWN')
    END as newgen

FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid
ORDER BY 1,2
---------------------------------
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;
---------------------------------
SELECT cst_id, COUNT(*) FROM(
SELECT
     ci.cst_create_date,
     ci.cst_id,
    ROW_NUMBER()OVER(PARTITION BY ci.cst_id ORDER BY ci.cst_create_date DESC) AS newid
      
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key=la.cid)t 
WHERE newid=1
GROUP BY cst_id
HAVING COUNT(*)>1
---------------------------------------------
SELECT DISTINCT newgen FROM gold.dim_customers
