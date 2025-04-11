/* 

-- CREATE DATABASE AND SCHEMAS
This will create a new database called 'DataWarehouseProject'. This script will run to see if that database already exists, and if it does exist, this will drop the old database and create a new one. 
This will also create 3 schemas within the database: 'bronze', 'silver', and 'gold'. 
All data in previous instances of this database will be DELETED. Ensure data is backed up prior to running this script. 

*/ 

USE master;
GO

-- DROP AND RECREATE THE DataWarehouseProject DATABASE

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = "DataWarehouseProject")
BEGIN 
	ALTER DATABASE DataWarehouseProject SET SINGLE_USER WITH ROLLBACK IMMEDIATE; 
	DROP DATABASE DataWarehouseProject; 
END; 
GO

CREATE DATABASE DataWarehouseProject; 

USE DataWarehouseProject; 

CREATE SCHEMA bronze; 
GO
CREATE SCHEMA silver; 
GO
CREATE SCHEMA gold; 
GO

IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_ID INT, 
	cst_key NVARCHAR(50), 
	cst_firstname NVARCHAR(50), 
	cst_lastname NVARCHAR(50), 
	cst_marital_status NVARCHAR(50), 
	cst_gender NVARCHAR(50), 
	cst_create_date DATE
);

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info (
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
	);

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_order_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

IF OBJECT_ID ('bronze.erp_LOC_A101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_LOC_A101;
CREATE TABLE bronze.erp_LOC_A101 (
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_CUST_AZ12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_CUST_AZ12;
CREATE TABLE bronze.erp_CUST_AZ12 (
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(50)
);

IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_PX_CAT_G1V2;
CREATE TABLE bronze.erp_PX_CAT_G1V2 (
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50)
);

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT 'Loading Bronze Layer';
		PRINT 'Loading CRM Tables';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info; 
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\caleb\OneDrive\Desktop\Database Project\cust_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		

		/* It is a good idea to check that our imported data is in the correct place 
	
		SELECT *
		FROM bronze.crm_cust_info; 

		SELECT COUNT(*)
		FROM bronze.crm_cust_info; 
	
		*/

		TRUNCATE TABLE bronze.crm_prd_info; 
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\caleb\OneDrive\Desktop\Database Project\prd_info.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',', 
			TABLOCK
		);

		TRUNCATE TABLE bronze.crm_sales_details; 
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\caleb\OneDrive\Desktop\Database Project\sales_details.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		PRINT 'Loading ERP Tables';

		TRUNCATE TABLE bronze.erp_loc_A101; 
		BULK INSERT bronze.erp_loc_A101
		FROM 'C:\Users\caleb\OneDrive\Desktop\Database Project\LOC_A101.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.erp_CUST_AZ12; 
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\caleb\OneDrive\Desktop\Database Project\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2; 
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\caleb\OneDrive\Desktop\Database Project\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURRED WHILE LOADING BRONZE LAYER'
	END CATCH
END

EXEC bronze.load_bronze;

