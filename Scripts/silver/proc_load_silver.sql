--Load silver layer- bronze -> silver
--Used ETL(Extract,Transform,Load) process to populate the "Silver" schema table from the bronze schema
--Truncate silver table
--Inserted transformed and cleansed data from Bronze to Silver table
--Quality checks 


/* DATA TRANSFORMATION & CLEANSING*/
CREATE OR ALTER PROCEDURE Silver.crm_cust_info AS 
BEGIN
TRUNCATE TABLE Silver.crm_cust_info;
INSERT INTO Silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    --Remove uneanted spaces to ensure data consistancy and uniformly across all records
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
   
    CASE 
     --Data normalization & cleansing- here mapping coded values to meaningful,user-friendly discriptions
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'UNKNOWN'  --handeled missing valuses- fills blank with defualt value
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'UNKNOWN'
    END AS cst_gndr,
    cst_create_date
FROM (
    --remove dupplicates-Ensure only one record per entity by identifying the most relevant row
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
           
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1;    --data filtering
-------------------------------------------------
/*Check for unwanted spaces
Expectation= no results*/
SELECT cst_firstname FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
----------------------------------------------------
SELECT cst_create_date
FROM bronze.crm_cust_info
WHERE TRY_CAST(TRIM(cst_create_date) AS DATE) IS NULL  
    AND cst_create_date IS NOT NULL;
  --------------------------------------------------------

/* Data standardization & consistancy*/
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info
-----------------------------------------------------------
SELECT cst_create_date, LEN(cst_create_date) AS length
FROM bronze.crm_cust_info
WHERE TRY_CAST(cst_create_date AS DATE) IS NULL
  AND cst_create_date IS NOT NULL;
 --- -------------------------------------------------------

SELECT *FROM silver.crm_cust_info
-------------------------------------------------------------
UPDATE bronze.crm_cust_info
SET cst_create_date = LEFT(cst_create_date, LEN(cst_create_date) - 1)
WHERE LEN(cst_create_date) = 11;
--------------------------------------------------------------
--Expectation is no results- Check the quality
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id is NULL
-----------------------------------
/* DATA TRANSFORMATION & CLEANSING*/
SELECT *FROM silver.crm_cust_info 
WHERE cst_id=14324
------------------------------------

END
*******************************************************************************
CREATE OR ALTER PROCEDURE silver.crm_prd_info AS 
BEGIN
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,  ---Extract product id
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,       ---Extract product key
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
     WHEN 'M' THEN 'Mountain'
     WHEN 'R' THEN 'Road'
     WHEN 'S' THEN 'Other sales'
     WHEN 'T' THEN 'Touring'
     ELSE 'UNKOWN'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS ped_start_dt,
/* LEAD() use to access the next value from next column*/
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE) AS prd_end_dt

FROM bronze.crm_prd_info
----------------------------------
--Expectation is no results- Check the quality
SELECT
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id is NULL
-----------------------------------------
/* Data standardization & consistancy*/
SELECT DISTINCT prd_line FROM silver.crm_prd_info
SELECT DISTINCT prd_line from bronze.crm_prd_info
---------------------------------------------
--chech if there is negative or Null numbers
SELECT prd_cost FROM silver.crm_prd_info where prd_cost <0 or prd_cost is null
---------------------------------------------------
/*check for invalid date orders*/
SELECT *FROM bronze.crm_prd_info where prd_end_dt < prd_start_dt
SELECT *FROM silver.crm_prd_info where prd_end_dt < prd_start_dt
--------------------------------------------------------
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)
-----------------------------------------------------------
SELECT *FROM silver.crm_prd_info

END
**************************************************************************
CREATE OR ALTER PROCEDURE silver.crm_sales_details AS 
BEGIN
TRUNCATE TABLE silver.crm_sales_details;
INSERT INTO silver.crm_sales_details(
    sls_ord_num, 
    sls_prd_key,
    sls_cust_id,
    sls_ord_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_ord_dt =0 or LEN(sls_ord_dt) !=8 then null    --Handel invalid data
        ELSE CAST(CAST( sls_ord_dt AS varchar)AS DATE)  --In SQL SERVER we cannot direct change type from INT to DATE,change it first to VARCHAR then DATE
    END AS sls_ord_dt,

    CASE WHEN sls_ship_dt=0 or LEN(sls_ship_dt)!=8 THEN NULL
        ELSE CAST(CAST( sls_ship_dt AS varchar)AS DATE) 
    END AS sls_ship_dt,
 
    CASE WHEN sls_due_dt=0 or LEN(sls_due_dt)!=8 THEN NULL
        ELSE CAST(CAST( sls_due_dt AS varchar)AS DATE) 
    END AS sls_due_dt,

    
    CASE WHEN TRY_CAST(sls_sales as INT) IS NULL OR TRY_CAST(sls_sales as INT) <= 0 OR TRY_CAST(sls_sales as INT)! = TRY_CAST(sls_quantity as INT) *ABS(TRY_CAST(sls_price as INT))
        THEN TRY_CAST(sls_quantity as INT) * ABS(TRY_CAST(sls_price as INT))
       ELSE TRY_CAST(sls_sales as INT)
    END sls_sales,  --recalculate sls_sales if orginal value is missing or incorrect

    TRY_CAST(sls_quantity as INT) sls_quantity,

    CASE WHEN TRY_CAST(sls_price as INT) IS NULL OR TRY_CAST(sls_price as INT) <= 0 
        THEN TRY_CAST(sls_sales as INT) / NULLIF(sls_quantity,0)
       ELSE TRY_CAST(sls_price as INT)
    END sls_price   --Derive price if orginal value is invalid

FROM bronze.crm_sales_details

WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
---------------------------------------------------
SELECT *FROM bronze.crm_sales_details
----------------------------------------------------
---------------------------------------------
--check if there is negative or Null numbers
SELECT sls_ord_dt 
FROM bronze.crm_sales_details 
where sls_ord_dt <= 0 
OR LEN(sls_ord_dt)!=8
----------------------------------------------
/*check for invalid Date order */
SELECT *FROM bronze.crm_sales_details where sls_ord_dt > sls_ship_dt or sls_ord_dt >sls_due_dt
SELECT *FROM silver.crm_sales_details where sls_ord_dt > sls_ship_dt or sls_ord_dt >sls_due_dt
---------------------------------------------
/*Check Data Consistancy : between sales, quantity and price
Sales=Quantyty*price 
Value must not be zer, null or negative */
SELECT DISTINCT
sls_quantity,
sls_sales,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <= 0 OR sls_price <=0 OR sls_price <=0
ORDER BY sls_quantity,sls_sales,sls_price

--------------------------------------------------------------------

SELECT DISTINCT
sls_sales,
CASE WHEN TRY_CAST(sls_sales as INT) IS NULL OR TRY_CAST(sls_sales as INT) <= 0 OR TRY_CAST(sls_sales as INT)! = TRY_CAST(sls_quantity as INT) *ABS(TRY_CAST(sls_price as INT))
        THEN TRY_CAST(sls_quantity as INT) * ABS(TRY_CAST(sls_price as INT))
       ELSE TRY_CAST(sls_sales as INT)
    END new_sls_sales,
--sls_quantity,
sls_price,
TRY_CAST(sls_quantity as int) sls_quantity,
CASE WHEN TRY_CAST(sls_price as INT) IS NULL OR TRY_CAST(sls_price as INT) <= 0 
        THEN TRY_CAST(sls_sales as INT) / NULLIF(sls_quantity,0)
       ELSE TRY_CAST (sls_price as INT)
    END new_sls_price
FROM bronze.crm_sales_details
-------------------------------------------------------
SELECT *FROM silver.crm_sales_details where sls_price <=0

END
************************************************************************************
CREATE OR ALTER PROCEDURE silver.erp_cust_az12 AS 
BEGIN
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12(
    cid,
    bdate,
    gen
)
SELECT 
CASE WHEN  cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))      --SUBSTRUNG() extract part of the text(handeled invalid value)
    ELSE cid
END as cid,

CASE WHEN bdate > GETDATE() THEN NULL   --set future birthdays to NULL
    ELSE bdate
END as bdate,


CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'  -- Normalize gender values and handel unkown cases
     WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
    ELSE 'UNKOWN'
END as gen

FROM bronze.erp_cust_az12

--Check if there are not matched values
WHERE CASE WHEN  cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))      
    ELSE cid
END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)


----------------------------
SELECT *from silver.crm_cust_info
--------------------------------
SELECT DISTINCT bdate 
from silver.erp_cust_az12 where bdate < '1924-01-01' or bdate > GETDATE()
-------------------------------------------------
/* Data standardization and consistancy*/
SELECT DISTINCT gen,
 CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
    WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
    ELSE 'UNKOWN'
END as gen
from bronze.erp_cust_az12 

SELECT DISTINCT gen FROM silver.erp_cust_az12 
--------------------------
UPDATE bronze.erp_cust_az12
SET gen = LEFT(gen, LEN(gen) - 1)
----------------------------------
ALTER TABLE silver.erp_cust_az12 ALTER COLUMN cid NVARCHAR(50);

END
******************************************************************************
CREATE OR ALTER PROCEDURE silver.erp_loc_a101 AS 
BEGIN
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT 
REPLACE(cid,'-','') cid,

CASE WHEN TRIM(cntry)='DE' THEN 'Germany'
    WHEN  TRIM(cntry) IN ('USA','US') THEN 'United States'
    WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'UNKOWN'
    ELSE TRIM(cntry)
END as cntry
FROM bronze.erp_loc_a101 
---Check for unmatched values
WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM bronze.crm_cust_info)
------------------------------------------
SELECT cst_key from bronze.crm_cust_info
-----------------------------------------
/* Data standardization and consistancy*/
SELECT DISTINCT 
cntry
FROM silver.erp_loc_a101
order by cntry
----------------------------------------
UPDATE bronze.erp_loc_a101
SET cntry = LEFT(cntry, LEN(cntry) - 1)
--------------------------------------
ALTER TABLE silver.erp_loc_a101 ALTER COLUMN cid NVARCHAR(50);
----------------------------------------
SELECT *FROM silver.erp_loc_a101

END
**************************************************************************************
CREATE OR ALTER PROCEDURE silver.erp_px_cat_g1v2 AS 
BEGIN
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2(
    id,
    cat,
    subcat,
    maintetance

)

SELECT 
id,
cat,
subcat,
maintetance
FROM bronze.erp_px_cat_g1v2
-------------------------------
SELECT *from silver.crm_prd_info
---------------------------------
/* Check for unwanted category*/
SELECT * FROM bronze.erp_px_cat_g1v2 WHERE cat != TRIM(cat)
--------------------------------
/* Data standardization & consistancy*/
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2
------------------------------
ALTER TABLE silver.erp_px_cat_g1v2 ALTER COLUMN id NVARCHAR(50);
-------------------------------
SELECT *FROM silver.erp_px_cat_g1v2

END
