/*
CREATES OUR SILVER LAYER
*/

IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
	cst_ID INT, 
	cst_key NVARCHAR(50), 
	cst_firstname NVARCHAR(50), 
	cst_lastname NVARCHAR(50), 
	cst_marital_status NVARCHAR(50), 
	cst_gender NVARCHAR(50), 
	cst_create_date DATE,
	dwh_create_time DATETIME2 DEFAULT GETDATE() 
);

IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
	prd_id INT,
	cat_ID NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_time DATETIME2 DEFAULT GETDATE() 
	);

IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
	sls_order_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_time DATETIME2 DEFAULT GETDATE() 
);

IF OBJECT_ID ('silver.erp_LOC_A101', 'U') IS NOT NULL
	DROP TABLE silver.erp_LOC_A101;
CREATE TABLE silver.erp_LOC_A101 (
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50),
	dwh_create_time DATETIME2 DEFAULT GETDATE() 
);

IF OBJECT_ID ('silver.erp_CUST_AZ12', 'U') IS NOT NULL
	DROP TABLE silver.erp_CUST_AZ12;
CREATE TABLE silver.erp_CUST_AZ12 (
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(50),
	dwh_create_time DATETIME2 DEFAULT GETDATE() 
);

IF OBJECT_ID ('silver.erp_PX_CAT_G1V2', 'U') IS NOT NULL
	DROP TABLE silver.erp_PX_CAT_G1V2;
CREATE TABLE silver.erp_PX_CAT_G1V2 (
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50),
	dwh_create_time DATETIME2 DEFAULT GETDATE() 
);

/* 
LOADS CLEANED, STANRDARDIZED DATAT INTO OUR SILVER LAYER
*/

SELECT *
FROM bronze.crm_cust_info
WHERE cst_id = 29466; 

/*
CHECKS FOR NULLS/DUPLICATES IN PRIMARY KEY
*/

SELECT prd_id, 
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT *, 
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id = 29466

/*
SELECTS AND RANKS ALL cust_ID VALUES THAT ARE NOT DUPLICATES
*/

SELECT *
FROM (
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t
WHERE flag_last = 1; 

/*
CHECK FOR UNWANTED SPACES
*/

SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname); 



/* 
STANDARDIZATION - CAN REPEAT FOR EACH VALUE AS NEEDED
*/

SELECT DISTINCT cst_gender
FROM bronze.crm_cust_info

/* 
INSERTS SINGLE QUERY INTO SILVER LAYER FOLLOWING TRANSFORMATION
REPLACES * WITH EACH COLUMN NAME TO APPLY TRANSFORMATIONS TO EACH COLUMN
*/

INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status, 
	cst_gender, 
	cst_create_date)

SELECT 
cst_ID, 
cst_key, 
TRIM(cst_firstname) as cst_firstname, 
TRIM(cst_lastname) as cst_lastname, 
CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_marital_status, 
CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gender,
cst_create_date
FROM (
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL
)t
WHERE flag_last = 1; 

/*
CHECKS OUR TRANSFORMATIONS
*/

SELECT *
FROM silver.crm_cust_info; 

/*
REPEAT CLEANING FOR crm_prd_info
*/

INSERT INTO silver.crm_prd_info (
	prd_id, 
	cat_id, 
	prd_key, 
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)

SELECT prd_id, 
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- will match with erp_px_cat_G1V2 key to join
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- now able to join with crm_sales_details
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost, -- replaces nulls with argument in second position
	CASE UPPER(TRIM(prd_line)) -- shortens syntax if replacements in case aren't complicated
		 WHEN 'M' THEN 'Mountain'
		 WHEN 'R' THEN 'Road'
		 WHEN 'S' THEN 'Other Sales'
		 WHEN 'T' THEN 'Touring'
		 ELSE 'n/a'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
; 

SELECT prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL; 

/*
CHECK INVALID ORDER DATE - SHIPPED BEFORE ORDER
*/

SELECT * 
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

SELECT * 
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;


/*
REPEAT CLEANING FOR crm_sales_details
*/

INSERT INTO silver.crm_sales_details (
	sls_order_num,
	sls_prd_key, 
	sls_cust_id, 
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)

SELECT sls_order_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN (sls_order_dt <= 0 OR LEN(sls_order_dt) != 8) THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN (sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8) THEN NULL
	ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN (sls_due_dt <= 0 OR LEN(sls_due_dt) != 8) THEN NULL
	ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales, 
	sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
	 THEN sls_sales / NULLIF(sls_quantity, 0)
	 ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details; 

/*
CHECK FOR INVALID DATES
*/

SELECT
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8; 

/* 
PER BUSINESS RULES: SALES, QUANTITY, PRICE CANNOT BE NULL OR ZERO
CHECK FOR INVALID CALCULATIONS
*/

SELECT DISTINCT
	sls_sales AS old_sls_sales, 
	sls_quantity,
	sls_price AS old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales, 

CASE WHEN sls_price IS NULL OR sls_price <= 0
	 THEN sls_sales / NULLIF(sls_quantity, 0)
	 ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_quantity, sls_price
; 

/*
REPEAT CLEANING FOR erp_cust_az12
*/

-- CHECK TO MAKE SURE KEYS MATCH

SELECT
	cid, -- CONTAINS 3 LEADING CHARACTERS vs. crm.cust_info KEY
	bdate, 
	gen
FROM bronze.erp_CUST_AZ12; 

INSERT INTO silver.erp_CUST_AZ12 (
	cid,
	bdate,
	gen)

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_CUST_AZ12; 

SELECT DISTINCT bdate
FROM bronze.erp_CUST_AZ12
WHERE bdate < '1925-01-01' OR bdate > GETDATE();  -- Selects customers over 100 years old or with birthdays in the future

/*
REPEAT CLEANING FOR erp_cust_az12
*/

INSERT INTO silver.erp_LOC_A101 (
	CID,
	CNTRY)

SELECT 
	REPLACE(CID, '-', '') cid,
CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
	 WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(CNTRY) = '' or CNTRY IS NULL THEN 'n/a'
	 ELSE TRIM(CNTRY)
END AS CNTRY
FROM bronze.erp_LOC_A101; 

-- Compares key formats for potential joining of data

SELECT
	cst_key
FROM silver.crm_cust_info; 

/*
REPEAT CLEANING FOR erp_px_cat_g1v2
*/

INSERT INTO silver.erp_PX_CAT_G1V2 (
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE)

SELECT 
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
FROM bronze.erp_px_cat_G1V2; 

/* 
Check for unwanted spaces
*/

SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)
; 

/*



TRUNCATE AND LOAD FULL DATA



*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status, 
		cst_gender, 
		cst_create_date)

	SELECT 
	cst_ID, 
	cst_key, 
	TRIM(cst_firstname) as cst_firstname, 
	TRIM(cst_lastname) as cst_lastname, 
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_marital_status, 
	CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
		 WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gender,
	cst_create_date
	FROM (
	SELECT *, 
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t
	WHERE flag_last = 1; 

	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info (
		prd_id, 
		cat_id, 
		prd_key, 
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt)

	SELECT prd_id, 
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- will match with erp_px_cat_G1V2 key to join
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- now able to join with crm_sales_details
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost, -- replaces nulls with argument in second position
		CASE UPPER(TRIM(prd_line)) -- shortens syntax if replacements in case aren't complicated
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info
	; 

	TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details (
		sls_order_num,
		sls_prd_key, 
		sls_cust_id, 
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)

	SELECT sls_order_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN (sls_order_dt <= 0 OR LEN(sls_order_dt) != 8) THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN (sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8) THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN (sls_due_dt <= 0 OR LEN(sls_due_dt) != 8) THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales, 
		sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
		 THEN sls_sales / NULLIF(sls_quantity, 0)
		 ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details; 


	TRUNCATE TABLE silver.erp_CUST_AZ12;
	INSERT INTO silver.erp_CUST_AZ12 (
		cid,
		bdate,
		gen)

	SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END AS cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
	FROM bronze.erp_CUST_AZ12; 


	TRUNCATE TABLE silver.erp_LOC_A101;
	INSERT INTO silver.erp_LOC_A101 (
		CID,
		CNTRY)

	SELECT 
		REPLACE(CID, '-', '') cid,
	CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
		 WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(CNTRY) = '' or CNTRY IS NULL THEN 'n/a'
		 ELSE TRIM(CNTRY)
	END AS CNTRY
	FROM bronze.erp_LOC_A101; 

	TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
	INSERT INTO silver.erp_PX_CAT_G1V2 (
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE)

	SELECT 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
	FROM bronze.erp_px_cat_G1V2; 

END

EXEC silver.load_silver