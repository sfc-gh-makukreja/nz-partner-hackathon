-- Complete Setup for NZ Partner Hackathon Electricity Data
-- Run this after processing data with Python scripts

USE ROLE ACCOUNTADMIN;
USE DATABASE NZ_PARTNER_HACKATHON;
USE SCHEMA URU_RANGI;
USE WAREHOUSE COMPUTE_WH;

-- =============================================
-- 1. CREATE TABLES (from load_to_snowflake.sql)
-- =============================================

-- Create file format for CSV loading
CREATE OR REPLACE FILE FORMAT electricity_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    COMMENT = 'CSV format for electricity data loading';

-- Create internal stage
CREATE OR REPLACE STAGE electricity_data_stage
    FILE_FORMAT = electricity_csv_format
    COMMENT = 'Stage for electricity data files';

-- Zone data table (5-minute intervals)
CREATE OR REPLACE TABLE electricity_zone_data_5min (
    timestamp_nz TIMESTAMP,
    nz_total_mw NUMBER(10,2),
    nz_total_mvar NUMBER(10,2),
    ni_total_mw NUMBER(10,2),
    ni_total_mvar NUMBER(10,2),
    si_total_mw NUMBER(10,2),
    si_total_mvar NUMBER(10,2),
    northland_gz1_mw NUMBER(10,2),
    northland_gz1_mvar NUMBER(10,2),
    auckland_gz2_mw NUMBER(10,2),
    auckland_gz2_mvar NUMBER(10,2),
    hamilton_gz3_mw NUMBER(10,2),
    hamilton_gz3_mvar NUMBER(10,2),
    edgecumbe_gz4_mw NUMBER(10,2),
    edgecumbe_gz4_mvar NUMBER(10,2),
    hawkes_bay_gz5_mw NUMBER(10,2),
    hawkes_bay_gz5_mvar NUMBER(10,2),
    taranaki_gz6_mw NUMBER(10,2),
    taranaki_gz6_mvar NUMBER(10,2),
    bunnythorpe_gz7_mw NUMBER(10,2),
    bunnythorpe_gz7_mvar NUMBER(10,2),
    wellington_gz8_mw NUMBER(10,2),
    wellington_gz8_mvar NUMBER(10,2),
    nelson_gz9_mw NUMBER(10,2),
    nelson_gz9_mvar NUMBER(10,2),
    christchurch_gz10_mw NUMBER(10,2),
    christchurch_gz10_mvar NUMBER(10,2),
    canterbury_gz11_mw NUMBER(10,2),
    canterbury_gz11_mvar NUMBER(10,2),
    west_coast_gz12_mw NUMBER(10,2),
    west_coast_gz12_mvar NUMBER(10,2),
    otago_gz13_mw NUMBER(10,2),
    otago_gz13_mvar NUMBER(10,2),
    southland_gz14_mw NUMBER(10,2),
    southland_gz14_mvar NUMBER(10,2),
    load_timestamp TIMESTAMP
) COMMENT = '5-minute interval electricity demand data by geographic zone in New Zealand';

-- Fuel type table
CREATE OR REPLACE TABLE electricity_generation_by_fuel (
    calendar_year NUMBER(4,0),
    hydro_gwh NUMBER(10,2),
    geothermal_gwh NUMBER(10,2),
    biogas_gwh NUMBER(10,2),
    wind_gwh NUMBER(10,2),
    solar_pv_gwh NUMBER(10,2),
    oil_gwh NUMBER(10,2),
    coal_gwh NUMBER(10,2),
    gas_gwh NUMBER(10,2),
    electricity_only_subtotal_gwh NUMBER(10,2),
    cogeneration_gwh NUMBER(10,2),
    total_generation_gwh NUMBER(10,2),
    renewable_gwh NUMBER(10,2),
    fossil_fuel_gwh NUMBER(10,2),
    renewable_percentage NUMBER(5,2),
    fossil_fuel_percentage NUMBER(5,2),
    source_sheet STRING,
    load_timestamp TIMESTAMP
) COMMENT = 'Annual electricity generation by fuel type showing renewable vs fossil fuel breakdown';

-- Quarterly generation table
CREATE OR REPLACE TABLE electricity_quarterly_generation (
    calendar_quarter DATE,
    net_generation_gwh NUMBER(10,2),
    quarter_year NUMBER(4,0),
    quarter_number NUMBER(1,0),
    year_over_year_change_percent NUMBER(8,4),
    source_sheet STRING,
    load_timestamp TIMESTAMP
) COMMENT = 'Quarterly electricity generation data from 1974 to present for trend analysis';

-- =============================================
-- 2. LOAD DATA COMMANDS
-- =============================================

/*
-- Upload processed files to stage:
PUT file://processed_data/electricity_zone_data_5min_final.csv @electricity_data_stage AUTO_COMPRESS=TRUE;
PUT file://processed_data/electricity_generation_by_fuel_final.csv @electricity_data_stage AUTO_COMPRESS=TRUE;
PUT file://processed_data/electricity_quarterly_generation_final.csv @electricity_data_stage AUTO_COMPRESS=TRUE;

-- Load zone data
COPY INTO electricity_zone_data_5min
FROM @electricity_data_stage/electricity_zone_data_5min_final.csv.gz
FILE_FORMAT = electricity_csv_format
ON_ERROR = 'CONTINUE';

-- Load fuel type data
COPY INTO electricity_generation_by_fuel
FROM @electricity_data_stage/electricity_generation_by_fuel_final.csv.gz
FILE_FORMAT = electricity_csv_format
ON_ERROR = 'CONTINUE';

-- Load quarterly data
COPY INTO electricity_quarterly_generation
FROM @electricity_data_stage/electricity_quarterly_generation_final.csv.gz
FILE_FORMAT = electricity_csv_format
ON_ERROR = 'CONTINUE';
*/

-- =============================================
-- 3. CREATE VIEWS FOR HACKATHON PARTICIPANTS
-- =============================================

-- Daily electricity demand summary by zone
CREATE OR REPLACE VIEW electricity_daily_zone_summary AS
SELECT 
    DATE(timestamp_nz) as date_nz,
    AVG(nz_total_mw) as avg_nz_demand_mw,
    MAX(nz_total_mw) as peak_nz_demand_mw,
    MIN(nz_total_mw) as min_nz_demand_mw,
    AVG(ni_total_mw) as avg_ni_demand_mw,
    AVG(si_total_mw) as avg_si_demand_mw,
    -- Individual zones
    AVG(auckland_gz2_mw) as avg_auckland_demand_mw,
    AVG(wellington_gz8_mw) as avg_wellington_demand_mw,
    AVG(christchurch_gz10_mw) as avg_christchurch_demand_mw,
    COUNT(*) as intervals_recorded
FROM electricity_zone_data_5min
WHERE timestamp_nz IS NOT NULL
GROUP BY DATE(timestamp_nz)
ORDER BY date_nz DESC;

-- Renewable energy trends over time
CREATE OR REPLACE VIEW renewable_energy_trends AS
SELECT 
    calendar_year,
    renewable_percentage,
    renewable_gwh,
    total_generation_gwh,
    hydro_gwh,
    geothermal_gwh,
    wind_gwh,
    solar_pv_gwh,
    -- Growth metrics
    LAG(renewable_percentage) OVER (ORDER BY calendar_year) as prev_year_renewable_pct,
    renewable_percentage - LAG(renewable_percentage) OVER (ORDER BY calendar_year) as renewable_pct_change
FROM electricity_generation_by_fuel
WHERE calendar_year >= 2000  -- Focus on recent decades
ORDER BY calendar_year DESC;

-- Peak demand analysis by region
CREATE OR REPLACE VIEW peak_demand_analysis AS
SELECT 
    DATE(timestamp_nz) as date_nz,
    EXTRACT(HOUR FROM timestamp_nz) as hour_of_day,
    -- Peak demands by region
    MAX(auckland_gz2_mw) as auckland_peak_mw,
    MAX(hamilton_gz3_mw) as hamilton_peak_mw,
    MAX(wellington_gz8_mw) as wellington_peak_mw,
    MAX(christchurch_gz10_mw) as christchurch_peak_mw,
    MAX(nz_total_mw) as nz_peak_mw,
    -- Time of peak
    timestamp_nz as peak_timestamp
FROM electricity_zone_data_5min
WHERE timestamp_nz IS NOT NULL
GROUP BY DATE(timestamp_nz), EXTRACT(HOUR FROM timestamp_nz), timestamp_nz
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE(timestamp_nz) ORDER BY nz_total_mw DESC) = 1
ORDER BY date_nz DESC;

-- Wind generation potential analysis (comparing wind-heavy regions)
CREATE OR REPLACE VIEW wind_potential_zones AS
SELECT 
    DATE(timestamp_nz) as date_nz,
    EXTRACT(HOUR FROM timestamp_nz) as hour_of_day,
    -- Regions with high wind potential
    AVG(taranaki_gz6_mw) as taranaki_avg_mw,
    AVG(wellington_gz8_mw) as wellington_avg_mw,
    AVG(canterbury_gz11_mw) as canterbury_avg_mw,
    AVG(west_coast_gz12_mw) as west_coast_avg_mw,
    AVG(southland_gz14_mw) as southland_avg_mw,
    -- Variability (indicator of wind patterns)
    STDDEV(taranaki_gz6_mw) as taranaki_variability,
    STDDEV(wellington_gz8_mw) as wellington_variability,
    COUNT(*) as readings_count
FROM electricity_zone_data_5min
WHERE timestamp_nz IS NOT NULL
GROUP BY DATE(timestamp_nz), EXTRACT(HOUR FROM timestamp_nz)
ORDER BY date_nz DESC, hour_of_day;

-- =============================================
-- 4. SAMPLE QUERIES FOR PARTICIPANTS
-- =============================================

/*
-- SAMPLE HACKATHON QUERIES - Share these with participants

-- 1. New Zealand's renewable energy journey
SELECT 
    calendar_year,
    renewable_percentage,
    total_generation_gwh,
    renewable_gwh,
    renewable_pct_change
FROM renewable_energy_trends
WHERE calendar_year >= 2010
ORDER BY calendar_year;

-- 2. Peak electricity demand patterns
SELECT 
    hour_of_day,
    AVG(nz_peak_mw) as avg_peak_demand_mw,
    COUNT(*) as days_analyzed
FROM peak_demand_analysis
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 3. Regional electricity demand comparison
SELECT 
    date_nz,
    avg_auckland_demand_mw,
    avg_wellington_demand_mw,
    avg_christchurch_demand_mw,
    peak_nz_demand_mw
FROM electricity_daily_zone_summary
WHERE date_nz >= '2024-07-01'
ORDER BY date_nz;

-- 4. Wind energy potential by region and time
SELECT 
    hour_of_day,
    AVG(taranaki_avg_mw) as taranaki_potential,
    AVG(wellington_avg_mw) as wellington_potential,
    AVG(canterbury_avg_mw) as canterbury_potential,
    AVG(southland_avg_mw) as southland_potential
FROM wind_potential_zones
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 5. Quarterly generation growth analysis
SELECT 
    quarter_year,
    quarter_number,
    net_generation_gwh,
    year_over_year_change_percent
FROM electricity_quarterly_generation
WHERE quarter_year >= 2020
ORDER BY quarter_year DESC, quarter_number DESC;

-- AI/ML Use Cases for Participants:
-- • Predict peak demand times using weather and historical patterns
-- • Optimize renewable energy deployment based on regional wind patterns
-- • Forecast electricity generation needs based on growth trends
-- • Detect anomalies in regional electricity consumption
-- • Plan grid infrastructure based on demand growth patterns
*/

-- Verify data loading
SELECT 'Zone data (5-min)' as dataset, COUNT(*) as record_count FROM electricity_zone_data_5min
UNION ALL
SELECT 'Fuel type data' as dataset, COUNT(*) as record_count FROM electricity_generation_by_fuel
UNION ALL
SELECT 'Quarterly data' as dataset, COUNT(*) as record_count FROM electricity_quarterly_generation;

-- Display available views
SHOW VIEWS IN SCHEMA URU_RANGI;