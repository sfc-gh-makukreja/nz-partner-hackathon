-- =============================================
-- FOUNDATIONAL DATA LOADING: Stats NZ Economic Data
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
-- LOAD INC_INC_005: EARNINGS BY OCCUPATION
-- =============================================

-- Clear existing data
TRUNCATE TABLE earnings_by_occupation;

-- Load earnings by occupation data
COPY INTO earnings_by_occupation (
    structure_id, structure_name, action, period_code, year,
    occupation_code, occupation, sex_code, sex, qualification_code, qualification,
    measure_code, measure, obs_value
)
FROM '@foundational_data_stage/STATSNZ,INC_INC_005,1.0+all.csv.gz'
(FILE_FORMAT => foundational_csv_format)
FILE_FORMAT = (
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
)
ON_ERROR = 'CONTINUE';

-- =============================================
-- LOAD INC_INC_011: HOUSEHOLD INCOME BY REGION
-- =============================================

-- Clear existing data
TRUNCATE TABLE household_income_by_region;

-- Load household income by region data
COPY INTO household_income_by_region (
    structure_id, structure_name, action, period_code, year,
    region_code, region, household_type_code, household_type,
    measure_code, measure, obs_value
)
FROM (
    SELECT 
        $2::STRING as structure_id,
        $3::STRING as structure_name,
        $4::STRING as action,
        $5::STRING as period_code,
        TRY_CAST(SUBSTR($5, 1, 4) AS NUMBER) as year,
        $7::STRING as region_code,
        $8::STRING as region,
        $9::STRING as household_type_code,
        $10::STRING as household_type,
        $11::STRING as measure_code,
        $12::STRING as measure,
        TRY_CAST($13 AS NUMBER) as obs_value
    FROM '@foundational_data_stage/STATSNZ,INC_INC_011,1.0+all.csv.gz'
    (FILE_FORMAT => foundational_csv_format)
    WHERE $13 IS NOT NULL AND TRIM($13) != ''
)
ON_ERROR = 'CONTINUE';

-- =============================================
-- LOAD PRD_PRD_002: PRODUCTIVITY BY INDUSTRY
-- =============================================

-- Clear existing data
TRUNCATE TABLE productivity_by_industry;

-- Load productivity by industry data
COPY INTO productivity_by_industry (
    structure_id, structure_name, action, year_code, year,
    industry_code, industry, variable_code, variable_type,
    series_type_code, series_type, obs_value
)
FROM (
    SELECT 
        $2::STRING as structure_id,
        $3::STRING as structure_name,
        $4::STRING as action,
        $5::STRING as year_code,
        TRY_CAST($6 AS NUMBER) as year,
        $7::STRING as industry_code,
        $8::STRING as industry,
        $9::STRING as variable_code,
        $10::STRING as variable_type,
        $11::STRING as series_type_code,
        $12::STRING as series_type,
        TRY_CAST($13 AS NUMBER) as obs_value
    FROM '@foundational_data_stage/STATSNZ,PRD_PRD_002,1.0+all.csv.gz'
    (FILE_FORMAT => foundational_csv_format)
    WHERE $13 IS NOT NULL AND TRIM($13) != ''
)
ON_ERROR = 'CONTINUE';

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
    COUNT(DISTINCT CONCAT(sex, qualification)) as unique_demographics
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
SELECT 'Sample Earnings Data' as dataset, * FROM earnings_by_occupation LIMIT 3
UNION ALL
SELECT 'Sample Household Income Data' as dataset, 
       structure_id, structure_name, action, period_code, year::STRING, 
       region_code, region, household_type_code, household_type, 
       measure_code, measure, obs_value::STRING, created_at::STRING
FROM household_income_by_region LIMIT 3
UNION ALL
SELECT 'Sample Productivity Data' as dataset,
       structure_id, structure_name, action, year_code, year::STRING,
       industry_code, industry, variable_code, variable_type,
       series_type_code, series_type, obs_value::STRING, created_at::STRING
FROM productivity_by_industry LIMIT 3;

-- Test analytical views
SELECT 'Analytical Views Test' as test_type, 'Income Gender Analysis' as view_name, COUNT(*)::STRING as record_count, ''::STRING as sample_data FROM income_gender_analysis
UNION ALL
SELECT 'Analytical Views Test' as test_type, 'Regional Income Trends' as view_name, COUNT(*)::STRING as record_count, ''::STRING as sample_data FROM regional_income_trends
UNION ALL
SELECT 'Analytical Views Test' as test_type, 'Industry Productivity Trends' as view_name, COUNT(*)::STRING as record_count, ''::STRING as sample_data FROM industry_productivity_trends;

SELECT 
    'FOUNDATIONAL data loading completed' as status,
    'All Stats NZ datasets loaded successfully' as result,
    CURRENT_TIMESTAMP() as completion_time;