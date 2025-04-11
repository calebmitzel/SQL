/*
CREATING VIEWS FOR THE GOLD LAYER
*/

SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname, 
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gender,
		ci.cst_create_date, 
		ca.bdate,
		ca.gen, 
		la.cntry
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_A101 AS la
	ON ci.cst_key = la.cid
;

/*
Checks for duplicates
*/

SELECT cst_id, COUNT(*) FROM
	(SELECT 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname, 
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gender,
		ci.cst_create_date, 
		ca.bdate,
		ca.gen, 
		la.cntry
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_A101 AS la
	ON ci.cst_key = la.cid)t
GROUP BY cst_id 
HAVING COUNT(*) > 1; 

/*
Identifying descrepancies
For this project, we will asume the .crm tables are the most accurate source of information
*/

SELECT DISTINCT
		ci.cst_gender,
		ca.gen, 
		CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- CRM is the master source for gender information
		ELSE COALESCE(ca.gen, 'n/a')
		END AS new_gen
	FROM silver.crm_cust_info AS ci
	LEFT JOIN silver.erp_cust_az12 AS ca
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_A101 AS la
	ON ci.cst_key = la.cid
ORDER BY 1,2
;

/*
Join following data integration of mismatched gender values
*/

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name, 
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender -- CRM is the master source for gender information
	ELSE COALESCE(ca.gen, 'n/a')
	END AS gender, 
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_A101 AS la
ON ci.cst_key = la.cid
;

/*
Checks view quality
*/

SELECT *
FROM gold.dim_customers; 


CREATE VIEW gold.dim_product AS
SELECT
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
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
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_PX_CAT_G1V2 AS pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- identifies only current products (without end dates)
;

/*
Checks view quality
*/

SELECT *
FROM gold.dim_product; 

/*
Create a Fact/Transaction table using our previous 2 Dimension tables and views
*/

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_order_num AS order_number,
	pr.product_key, -- acts as surrogate key
	cu.customer_key, -- acts as surrogate key
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_product AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id
; 

SELECT *
FROM gold.fact_sales; 

/*
Checks foreign key integrity by identifying nulls in a join
Should return no values
*/

SELECT *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_product AS p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL; 