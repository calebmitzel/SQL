--- DATA CLEANING --- 

SELECT *
FROM hospital_staffing;

--- Create Staging Table --
--- We can work in this identical table so the raw data remains intact ---

CREATE TABLE hospital_staffing_copy AS (SELECT * FROM hospital_staffing);

SELECT *
FROM hospital_staffing_copy;

--- Identifying and Removing Duplicate Rows ---

SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY 'year', facility_number, facility_name, begin_date, end_date, county_name, type_of_control, hours_type, productive_hours, productive_hours_per_adjusted_patient_day) AS row_num
FROM hospital_staffing_copy;

WITH duplicate_cte AS
(
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY 'year', facility_number, facility_name, begin_date, end_date, county_name, type_of_control, hours_type, productive_hours, productive_hours_per_adjusted_patient_day) AS row_num
FROM hospital_staffing_copy
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1; 

--- No rows are returned, indicating every entry in our table is unique ---
--- Any returned records may be explored individually
--- As appropriate, 'SELECT *' may be replaced with 'DELETE' in line 28 to remove duplicates

--- Standardizing Data ---

SELECT DISTINCT facility_name
FROM hospital_staffing_copy; 

SELECT DISTINCT (TRIM(facility_name))
FROM hospital_staffing_copy; 

UPDATE hospital_staffing_copy
SET facility_name = TRIM(facility_name);

SELECT DISTINCT facility_name
FROM hospital_staffing_copy
ORDER BY 1; 

--- Lines 389 and 390 match and are only made distinct by a space between a dash:
--- "Southwest Healthcare System-Murrieta" vs "Southwest Healthcare System - Murrieta"
--- The same is true for lines 417/418
--- Line 458 abbreviates "California" to "Calif" and is repeated at line 461
--- Lines 478 - 481 have spaces trimmed out between a dash

SELECT *
FROM hospital_staffing_copy
WHERE facility_name LIKE 'SOUTHWEST%';

--- We appreciate a space between dashes that exists only records only for the year 2010

UPDATE hospital_staffing_copy
SET facility_name = 'SOUTHWEST HEALTHCARE SYSTEM-MURRIETA'
WHERE facility_name LIKE 'SOUTHWEST%';

--- This process can be repeated for our earlier noted inconsistencies within 'facility_name'

--- Treating Null Values ---

SELECT *
FROM hospital_staffing_copy;

SELECT *
FROM hospital_staffing_copy
WHERE facility_name IS NULL
OR facility_name = ''; 

SELECT *
FROM hospital_staffing_copy
WHERE county_name = 'Statewide'
ORDER BY "year" ASC; 

--- 85 rows are returned, consistent with 5 years of data and 17 unique hours types
--- It appears that all null values in our table are statewide totals, so facility_number, etc will not exist
--- We can verify by adding the productive hours ourselves to get 1836820000

SELECT SUM(productive_hours)
FROM(
	SELECT *
	FROM hospital_staffing_copy
	WHERE hours_type = 'Ancillary Cost Centers'
	AND "year" = 2009
	AND NOT county_name = 'Statewide'
	ORDER BY productive_hours DESC)
; 

SELECT *
FROM hospital_staffing_copy
WHERE type_of_control IS NULL; 

SELECT *
FROM hospital_staffing_copy
WHERE type_of_control IS NULL
AND county_name != 'Statewide'; 

--- With available information, we cannot confidently fill 'facility_number' or begin/end dates
--- We may, however, be able to identify 'type_of_control' by comparing with similar entries
--- Nulls should be checked for context as above

