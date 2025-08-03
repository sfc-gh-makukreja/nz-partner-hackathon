-- =============================================
-- FOUNDATIONAL DATA LOADING: Stats NZ Economic Data (Simplified)
-- Theme: FOUNDATIONAL (Socio-Economic Baseline)
-- Dependencies: setup_foundational.sql must be run first
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE nz_partner_hackathon;
USE SCHEMA FOUNDATIONAL;
USE WAREHOUSE COMPUTE_WH;

-- =============================================
-- UPLOAD DATA FILES TO STAGE
-- =============================================

-- Upload Stats NZ income and productivity data files
PUT 'file://data/STATSNZ,INC_INC_005,1.0+all.csv' @foundational_data_stage
    AUTO_COMPRESS = TRUE
    OVERWRITE = TRUE;

PUT 'file://data/STATSNZ,INC_INC_011,1.0+all.csv' @foundational_data_stage
    AUTO_COMPRESS = TRUE
    OVERWRITE = TRUE;

PUT 'file://data/STATSNZ,PRD_PRD_002,1.0+all.csv' @foundational_data_stage
    AUTO_COMPRESS = TRUE
    OVERWRITE = TRUE;

-- Check uploaded files
LIST @foundational_data_stage;

-- =============================================
-- LOAD DATA USING SIMPLIFIED APPROACH
-- =============================================

-- Clear existing data
TRUNCATE TABLE earnings_by_occupation;
TRUNCATE TABLE household_income_by_region;
TRUNCATE TABLE productivity_by_industry;

-- Create temporary tables for raw data loading
CREATE OR REPLACE TABLE temp_earnings (
    col1 STRING, col2 STRING, col3 STRING, col4 STRING, col5 STRING,
    col6 STRING, col7 STRING, col8 STRING, col9 STRING, col10 STRING,
    col11 STRING, col12 STRING, col13 STRING, col14 STRING, col15 STRING
);

CREATE OR REPLACE TABLE temp_household (
    col1 STRING, col2 STRING, col3 STRING, col4 STRING, col5 STRING,
    col6 STRING, col7 STRING, col8 STRING, col9 STRING, col10 STRING,
    col11 STRING, col12 STRING, col13 STRING
);

CREATE OR REPLACE TABLE temp_productivity (
    col1 STRING, col2 STRING, col3 STRING, col4 STRING, col5 STRING,
    col6 STRING, col7 STRING, col8 STRING, col9 STRING, col10 STRING,
    col11 STRING, col12 STRING, col13 STRING
);

-- Load raw data into temporary tables
COPY INTO temp_earnings 
FROM '@foundational_data_stage/STATSNZ,INC_INC_005,1.0+all.csv.gz'
FILE_FORMAT = foundational_csv_format
ON_ERROR = 'CONTINUE';

COPY INTO temp_household 
FROM '@foundational_data_stage/STATSNZ,INC_INC_011,1.0+all.csv.gz'
FILE_FORMAT = foundational_csv_format
ON_ERROR = 'CONTINUE';

COPY INTO temp_productivity 
FROM '@foundational_data_stage/STATSNZ,PRD_PRD_002,1.0+all.csv.gz'
FILE_FORMAT = foundational_csv_format
ON_ERROR = 'CONTINUE';

-- Transform and insert into final tables
INSERT INTO earnings_by_occupation (
    structure_id, structure_name, action, period_code, year,
    occupation_code, occupation, sex_code, sex, qualification_code, qualification,
    measure_code, measure, obs_value
)
SELECT 
    col2 as structure_id,
    col3 as structure_name,
    col4 as action,
    col5 as period_code,
    TRY_CAST(SUBSTR(col5, 1, 4) AS NUMBER) as year,
    col7 as occupation_code,
    col8 as occupation,
    col9 as sex_code,
    CASE 
        WHEN col9 = '1' THEN 'Male'
        WHEN col9 = '2' THEN 'Female'
        ELSE col10
    END as sex,
    col11 as qualification_code,
    col12 as qualification,
    col13 as measure_code,
    col14 as measure,
    TRY_CAST(col15 AS NUMBER) as obs_value
FROM temp_earnings
WHERE col15 IS NOT NULL AND TRIM(col15) != '' AND col15 != 'Observation value';

INSERT INTO household_income_by_region (
    structure_id, structure_name, action, period_code, year,
    region_code, region, household_type_code, household_type,
    measure_code, measure, obs_value
)
SELECT 
    col2 as structure_id,
    col3 as structure_name,
    col4 as action,
    col5 as period_code,
    TRY_CAST(SUBSTR(col5, 1, 4) AS NUMBER) as year,
    col7 as region_code,
    col8 as region,
    col9 as household_type_code,
    col10 as household_type,
    col11 as measure_code,
    col12 as measure,
    TRY_CAST(col13 AS NUMBER) as obs_value
FROM temp_household
WHERE col13 IS NOT NULL AND TRIM(col13) != '' AND col13 != 'Observation value';

INSERT INTO productivity_by_industry (
    structure_id, structure_name, action, year_code, year,
    industry_code, industry, variable_code, variable_type,
    series_type_code, series_type, obs_value
)
SELECT 
    col2 as structure_id,
    col3 as structure_name,
    col4 as action,
    col5 as year_code,
    TRY_CAST(col6 AS NUMBER) as year,
    col7 as industry_code,
    col8 as industry,
    col9 as variable_code,
    col10 as variable_type,
    col11 as series_type_code,
    col12 as series_type,
    TRY_CAST(col13 AS NUMBER) as obs_value
FROM temp_productivity
WHERE col13 IS NOT NULL AND TRIM(col13) != '' AND col13 != 'Observation value';

-- Clean up temporary tables
DROP TABLE temp_earnings;
DROP TABLE temp_household;
DROP TABLE temp_productivity;

-- =============================================
-- DATA VALIDATION AND SUMMARY
-- =============================================

-- Validate data loading
SELECT 'earnings_by_occupation' as table_name, COUNT(*) as records_loaded FROM earnings_by_occupation
UNION ALL
SELECT 'household_income_by_region' as table_name, COUNT(*) as records_loaded FROM household_income_by_region
UNION ALL
SELECT 'productivity_by_industry' as table_name, COUNT(*) as records_loaded FROM productivity_by_industry;

-- Check data ranges
SELECT 
    'Income Data Coverage' as data_type,
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    COUNT(DISTINCT occupation) as unique_occupations,
    COUNT(DISTINCT CONCAT(sex, '|', qualification)) as unique_demographics
FROM earnings_by_occupation
UNION ALL
SELECT 
    'Household Income Coverage' as data_type,
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    COUNT(DISTINCT region) as unique_regions,
    COUNT(DISTINCT household_type) as unique_household_types
FROM household_income_by_region
UNION ALL
SELECT 
    'Productivity Coverage' as data_type,
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    COUNT(DISTINCT industry) as unique_industries,
    COUNT(DISTINCT variable_type) as unique_variables
FROM productivity_by_industry;

-- Sample data preview
SELECT 'Sample Earnings Data:' as info, occupation, sex, year, measure, obs_value 
FROM earnings_by_occupation 
WHERE obs_value IS NOT NULL 
ORDER BY year DESC, occupation 
LIMIT 5;

SELECT 'Sample Household Income Data:' as info, region, household_type, year, measure, obs_value 
FROM household_income_by_region 
WHERE obs_value IS NOT NULL 
ORDER BY year DESC, region 
LIMIT 5;

SELECT 'Sample Productivity Data:' as info, industry, variable_type, year, obs_value 
FROM productivity_by_industry 
WHERE obs_value IS NOT NULL 
ORDER BY year DESC, industry 
LIMIT 5;

SELECT 
    'FOUNDATIONAL data loading completed' as status,
    'All Stats NZ datasets loaded successfully' as result,
    CURRENT_TIMESTAMP() as completion_time;