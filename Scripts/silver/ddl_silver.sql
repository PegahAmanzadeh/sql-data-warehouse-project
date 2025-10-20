---Create Silver table,dropping exsitning table if they alredy exist

IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
        cst_id INT,
        cst_key NVARCHAR(50),
        cst_firstname NVARCHAR(50),
        cst_lastname NVARCHAR(50),
        cst_marital_status NVARCHAR(50),
        cst_gndr NVARCHAR(50),
        cst_create_date DATE,
        dwh_create_date DATETIME2 DEFAULT GETDATE ()
);
INSERT INTO silver.crm_cust_info
SELECT 
    TRY_CAST(LTRIM(RTRIM(cst_id)) AS INT) AS cst_id,
    LTRIM(RTRIM(cst_key)) AS cst_key,
    LTRIM(RTRIM(cst_firstname)) AS cst_firstname,
    LTRIM(RTRIM(cst_lastname)) AS cst_lastname,
    LTRIM(RTRIM(cst_marital_status)) AS cst_marital_status,
    LTRIM(RTRIM(cst_gndr)) AS cst_gndr,
    TRY_CAST(LTRIM(RTRIM(cst_create_date)) AS DATE) AS cst_create_date
  
FROM bronze.crm_cust_info;

IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
    prd_id INT,
    prd_key NVARCHAR(50),
    cat_id  NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE ()
);
INSERT INTO silver.crm_prd_info
SELECT
    TRY_CAST(LTRIM(RTRIM(prd_id)) AS INT),
    LTRIM(RTRIM(prd_key)),
    LTRIM(RTRIM(prd_nm)),
    TRY_CAST(LTRIM(RTRIM(prd_cost)) AS DECIMAL),
    LTRIM(RTRIM(prd_line)),
    TRY_CAST(LTRIM(RTRIM(prd_start_dt)) AS DATETIME),
    TRY_CAST(LTRIM(RTRIM(prd_end_dt)) AS DATETIME)
FROM bronze.crm_prd_info;

IF OBJECT_ID('silver.crm_sales_details','U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT ,
    sls_ord_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE ()
);
INSERT INTO silver.crm_sales_details
SELECT
    LTRIM(RTRIM(sls_ord_num)),
    LTRIM(RTRIM(sls_prd_key)),
    TRY_CAST(LTRIM(RTRIM(sls_cust_id)) AS INT),
    TRY_CAST(LTRIM(RTRIM(sls_ord_dt)) AS DATE),
    TRY_CAST(LTRIM(RTRIM(sls_ship_dt)) AS DATE),
    TRY_CAST(LTRIM(RTRIM(sls_due_dt)) AS DATE),
    TRY_CAST(LTRIM(RTRIM(sls_sales)) AS INT),
    TRY_CAST(LTRIM(RTRIM(sls_quantity)) AS INT),
    TRY_CAST(LTRIM(RTRIM(sls_price)) AS DECIMAL)
FROM bronze.crm_sales_details;

IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
    cid INT,
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE ()
);
INSERT INTO silver.erp_loc_a101
SELECT
    TRY_CAST(LTRIM(RTRIM(cid)) AS INT),
    LTRIM(RTRIM(cntry))
FROM bronze.erp_loc_a101;

IF OBJECT_ID('silver.erp_cust_az12','U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
    cid INT,
    bdate DATE,
    gen NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE ()
);
INSERT INTO silver.erp_cust_az12
SELECT
    TRY_CAST(LTRIM(RTRIM(cid)) AS INT),
    TRY_CAST(LTRIM(RTRIM(bdate)) AS DATE),
    LTRIM(RTRIM(gen))
FROM bronze.erp_cust_az12;

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
    id INT,
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintetance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE ()
);
INSERT INTO silver.erp_px_cat_g1v2
SELECT
    TRY_CAST(LTRIM(RTRIM(id)) AS INT),
    LTRIM(RTRIM(cat)),
    LTRIM(RTRIM(subcat)),
    LTRIM(RTRIM(maintetance))
FROM bronze.erp_px_cat_g1v2;

ALTER TABLE silver.erp_cust_az12
ALTER COLUMN bdate DATE;

