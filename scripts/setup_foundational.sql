-- =============================================
-- FOUNDATIONAL SCHEMA SETUP: Socio-Economic Data
-- Theme: FOUNDATIONAL (Socio-Economic Baseline)
-- Dependencies: nz_partner_hackathon database
-- Data Sources: Stats NZ Income & Productivity Statistics
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE nz_partner_hackathon;
USE WAREHOUSE COMPUTE_WH;

-- Create schema for foundational socio-economic data
CREATE SCHEMA IF NOT EXISTS FOUNDATIONAL
    COMMENT = 'Foundational socio-economic data: income, productivity, demographics';

USE SCHEMA FOUNDATIONAL;

-- =============================================
-- STAGE AND FILE FORMAT SETUP
-- =============================================

-- Stage for Stats NZ CSV files
CREATE OR REPLACE STAGE foundational_data_stage
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        RECORD_DELIMITER = '\n'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        TRIM_SPACE = TRUE
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        REPLACE_INVALID_CHARACTERS = TRUE
        DATE_FORMAT = 'AUTO'
        TIME_FORMAT = 'AUTO'
        TIMESTAMP_FORMAT = 'AUTO'
    )
    COMMENT = 'Stage for Stats NZ foundational economic data files';

-- File format for Stats NZ CSV data
CREATE OR REPLACE FILE FORMAT foundational_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'AUTO'
    TIME_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO';

-- =============================================
-- TABLE CREATION
-- =============================================

-- INC_INC_005: Earnings by occupation, sex, and qualification
CREATE OR REPLACE TABLE earnings_by_occupation (
    structure_id STRING,
    structure_name STRING,
    action STRING,
    period_code STRING,
    year NUMBER,
    occupation_code STRING,
    occupation STRING,
    sex_code STRING,
    sex STRING,
    qualification_code STRING,
    qualification STRING,
    measure_code STRING,
    measure STRING,
    obs_value NUMBER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Stats NZ INC_INC_005: Earnings from main wage and salary job by occupation, sex, and qualification (2013-2024)';

-- INC_INC_011: Household income by region and household type
CREATE OR REPLACE TABLE household_income_by_region (
    structure_id STRING,
    structure_name STRING,
    action STRING,
    period_code STRING,
    year NUMBER,
    region_code STRING,
    region STRING,
    household_type_code STRING,
    household_type STRING,
    measure_code STRING,
    measure STRING,
    obs_value NUMBER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Stats NZ INC_INC_011: Household income by region, household type, and source (1998-2024)';

-- PRD_PRD_002: Productivity statistics by industry
CREATE OR REPLACE TABLE productivity_by_industry (
    structure_id STRING,
    structure_name STRING,
    action STRING,
    year_code STRING,
    year NUMBER,
    industry_code STRING,
    industry STRING,
    variable_code STRING,
    variable_type STRING,
    series_type_code STRING,
    series_type STRING,
    obs_value NUMBER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Stats NZ PRD_PRD_002: Productivity statistics - Growth accounting for labour productivity by industry (1979-2024)';

-- =============================================
-- ANALYTICAL VIEWS
-- =============================================

-- Income by occupation and gender analysis
CREATE OR REPLACE VIEW income_gender_analysis AS
SELECT 
    year,
    occupation,
    sex,
    qualification,
    MAX(CASE WHEN measure_code = 'AV_HOUR_EARN' THEN obs_value END) as avg_hourly_earnings,
    MAX(CASE WHEN measure_code = 'AV_WEEK_INC' THEN obs_value END) as avg_weekly_income,
    MAX(CASE WHEN measure_code = 'MED_HOUR_EARN' THEN obs_value END) as median_hourly_earnings,
    MAX(CASE WHEN measure_code = 'MED_WEEK_INC' THEN obs_value END) as median_weekly_income
FROM earnings_by_occupation
WHERE obs_value IS NOT NULL
GROUP BY year, occupation, sex, qualification
ORDER BY year DESC, occupation, sex, qualification;

-- Regional household income trends
CREATE OR REPLACE VIEW regional_income_trends AS
SELECT 
    year,
    region,
    household_type,
    MAX(CASE WHEN measure_code = 'AV_INC_ALL_SOUR' THEN obs_value END) as avg_total_income,
    MAX(CASE WHEN measure_code = 'AV_WS_INC' THEN obs_value END) as avg_wage_salary_income,
    MAX(CASE WHEN measure_code = 'AV_SE_INC' THEN obs_value END) as avg_self_employed_income,
    MAX(CASE WHEN measure_code = 'AV_GT_INC' THEN obs_value END) as avg_government_transfer_income,
    -- Calculate income composition percentages
    ROUND(MAX(CASE WHEN measure_code = 'AV_WS_INC' THEN obs_value END) / 
          NULLIF(MAX(CASE WHEN measure_code = 'AV_INC_ALL_SOUR' THEN obs_value END), 0) * 100, 1) as wage_salary_percentage,
    ROUND(MAX(CASE WHEN measure_code = 'AV_SE_INC' THEN obs_value END) / 
          NULLIF(MAX(CASE WHEN measure_code = 'AV_INC_ALL_SOUR' THEN obs_value END), 0) * 100, 1) as self_employed_percentage,
    ROUND(MAX(CASE WHEN measure_code = 'AV_GT_INC' THEN obs_value END) / 
          NULLIF(MAX(CASE WHEN measure_code = 'AV_INC_ALL_SOUR' THEN obs_value END), 0) * 100, 1) as government_transfer_percentage
FROM household_income_by_region
WHERE obs_value IS NOT NULL
GROUP BY year, region, household_type
ORDER BY year DESC, region, household_type;

-- Industry productivity trends
CREATE OR REPLACE VIEW industry_productivity_trends AS
SELECT 
    year,
    industry,
    MAX(CASE WHEN variable_code = '2' THEN obs_value END) as labour_productivity,
    MAX(CASE WHEN variable_code = '3' THEN obs_value END) as multifactor_productivity,
    MAX(CASE WHEN variable_code = '11' THEN obs_value END) as capital_deepening_contribution,
    -- Calculate productivity change year-over-year
    LAG(MAX(CASE WHEN variable_code = '2' THEN obs_value END)) OVER (
        PARTITION BY industry ORDER BY year
    ) as prev_year_labour_productivity,
    ROUND(
        (MAX(CASE WHEN variable_code = '2' THEN obs_value END) - 
         LAG(MAX(CASE WHEN variable_code = '2' THEN obs_value END)) OVER (
             PARTITION BY industry ORDER BY year
         )) / NULLIF(LAG(MAX(CASE WHEN variable_code = '2' THEN obs_value END)) OVER (
             PARTITION BY industry ORDER BY year
         ), 0) * 100, 2
    ) as labour_productivity_yoy_change
FROM productivity_by_industry
WHERE obs_value IS NOT NULL
GROUP BY year, industry
ORDER BY year DESC, industry;

-- Economic indicators summary view
CREATE OR REPLACE VIEW economic_indicators_summary AS
SELECT 
    'Income Data' as data_type,
    'Earnings by Occupation' as dataset,
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    COUNT(DISTINCT occupation) as categories,
    COUNT(*) as total_records
FROM earnings_by_occupation
UNION ALL
SELECT 
    'Income Data' as data_type,
    'Household Income by Region' as dataset,
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    COUNT(DISTINCT region) as categories,
    COUNT(*) as total_records
FROM household_income_by_region
UNION ALL
SELECT 
    'Productivity Data' as data_type,
    'Industry Productivity' as dataset,
    MIN(year) as earliest_year,
    MAX(year) as latest_year,
    COUNT(DISTINCT industry) as categories,
    COUNT(*) as total_records
FROM productivity_by_industry;

-- =============================================
-- SUMMARY
-- =============================================

-- Show created objects
SHOW TABLES IN SCHEMA FOUNDATIONAL;
SHOW VIEWS IN SCHEMA FOUNDATIONAL;

SELECT 
    'FOUNDATIONAL schema setup completed' as status,
    'Ready for Stats NZ data loading' as next_step,
    CURRENT_TIMESTAMP() as setup_time;