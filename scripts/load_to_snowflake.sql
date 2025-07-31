-- Load processed electricity data to Snowflake
-- NZ Partner Hackathon - Uru Rangi (Wind/Energy) Theme

USE ROLE ACCOUNTADMIN;
USE DATABASE NZ_PARTNER_HACKATHON;
USE SCHEMA URU_RANGI;
USE WAREHOUSE COMPUTE_WH;

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

-- Create internal stage for data loading
CREATE OR REPLACE STAGE electricity_data_stage
    FILE_FORMAT = electricity_csv_format
    COMMENT = 'Stage for electricity data files';

-- 1. ELECTRICITY ZONE DATA (5-minute intervals) - PRIMARY TABLE
CREATE OR REPLACE TABLE electricity_zone_data_5min (
    timestamp_nz TIMESTAMP,
    -- National totals
    nz_total_mw NUMBER(10,2),
    nz_total_mvar NUMBER(10,2),
    ni_total_mw NUMBER(10,2),
    ni_total_mvar NUMBER(10,2),
    si_total_mw NUMBER(10,2),
    si_total_mvar NUMBER(10,2),
    -- Zone data (14 geographic zones)
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
    -- Metadata
    load_timestamp TIMESTAMP
) COMMENT = '5-minute interval electricity demand data by geographic zone in New Zealand';

-- 2. FUEL TYPE DATA (Annual renewable vs non-renewable breakdown)
CREATE OR REPLACE TABLE electricity_generation_by_fuel (
    calendar_year NUMBER(4,0),
    -- Renewable sources (GWh)
    hydro_gwh NUMBER(10,2),
    geothermal_gwh NUMBER(10,2),
    biogas_gwh NUMBER(10,2),
    wind_gwh NUMBER(10,2),
    solar_pv_gwh NUMBER(10,2),
    -- Non-renewable sources (GWh)
    oil_gwh NUMBER(10,2),
    coal_gwh NUMBER(10,2),
    gas_gwh NUMBER(10,2),
    -- Totals
    electricity_only_subtotal_gwh NUMBER(10,2),
    cogeneration_gwh NUMBER(10,2),
    total_generation_gwh NUMBER(10,2),
    -- Calculated metrics
    renewable_percentage NUMBER(5,2),
    fossil_fuel_percentage NUMBER(5,2),
    -- Metadata
    source_sheet STRING,
    load_timestamp TIMESTAMP
) COMMENT = 'Annual electricity generation by fuel type showing renewable vs fossil fuel breakdown';

-- 3. QUARTERLY GENERATION DATA (Historical trends)
CREATE OR REPLACE TABLE electricity_quarterly_generation (
    calendar_quarter DATE,
    net_generation_gwh NUMBER(10,2),
    quarter_year NUMBER(4,0),
    quarter_number NUMBER(1,0),
    year_over_year_change_percent NUMBER(8,4),
    -- Metadata
    source_sheet STRING,
    load_timestamp TIMESTAMP
) COMMENT = 'Quarterly electricity generation data from 1974 to present for trend analysis';

-- Example loading commands (to be run after uploading files)
/*
-- Upload files to stage first:
PUT file://processed_data/electricity_*.csv @electricity_data_stage AUTO_COMPRESS=TRUE;

-- Load aggregated data
COPY INTO electricity_aggregated
FROM @electricity_data_stage/electricity_aggregated.csv.gz
FILE_FORMAT = electricity_csv_format
ON_ERROR = 'CONTINUE';

-- Load zone data
COPY INTO electricity_zone_data_5min  
FROM @electricity_data_stage/electricity_zone_data_5min.csv.gz
FILE_FORMAT = electricity_csv_format
ON_ERROR = 'CONTINUE';

-- Verify loading
SELECT COUNT(*) as total_aggregated_records FROM electricity_aggregated;
SELECT COUNT(*) as total_zone_records FROM electricity_zone_data_5min;
SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM electricity_zone_data_5min;
*/

-- Create helpful views for participants
CREATE OR REPLACE VIEW electricity_daily_summary AS
SELECT 
    DATE(period_start) as date_nz,
    region,
    SUM(total_generation_mw) as daily_total_generation_mw,
    SUM(renewable_generation_mw) as daily_renewable_generation_mw,
    AVG(demand_mw) as avg_daily_demand_mw,
    (SUM(renewable_generation_mw) / NULLIF(SUM(total_generation_mw), 0)) * 100 as renewable_percentage
FROM electricity_aggregated
WHERE period_start IS NOT NULL
GROUP BY DATE(period_start), region
ORDER BY date_nz DESC, region;

CREATE OR REPLACE VIEW electricity_zone_hourly AS
SELECT 
    DATE_TRUNC('HOUR', timestamp_utc) as hour_utc,
    zone_name,
    AVG(generation_mw) as avg_generation_mw,
    AVG(demand_mw) as avg_demand_mw,
    AVG(price_nzd_mwh) as avg_price_nzd_mwh,
    AVG(renewable_percentage) as avg_renewable_percentage,
    COUNT(*) as interval_count
FROM electricity_zone_data_5min
WHERE timestamp_utc IS NOT NULL
GROUP BY DATE_TRUNC('HOUR', timestamp_utc), zone_name
ORDER BY hour_utc DESC, zone_name;

-- Sample queries for participants to get started
/*
-- SAMPLE QUERIES FOR HACKATHON PARTICIPANTS

-- 1. Daily renewable energy trends
SELECT 
    date_nz,
    SUM(daily_renewable_generation_mw) as total_renewable_mw,
    AVG(renewable_percentage) as avg_renewable_percentage
FROM electricity_daily_summary
GROUP BY date_nz
ORDER BY date_nz;

-- 2. Peak demand analysis by zone
SELECT 
    zone_name,
    DATE(timestamp_utc) as date_nz,
    MAX(demand_mw) as peak_demand_mw,
    AVG(price_nzd_mwh) as avg_price
FROM electricity_zone_data_5min
WHERE timestamp_utc >= CURRENT_DATE() - 30
GROUP BY zone_name, DATE(timestamp_utc)
ORDER BY peak_demand_mw DESC;

-- 3. Price correlation with renewable percentage
SELECT 
    ROUND(renewable_percentage, -1) as renewable_percentage_bucket,
    AVG(price_nzd_mwh) as avg_price,
    COUNT(*) as sample_count
FROM electricity_zone_data_5min
WHERE renewable_percentage IS NOT NULL 
  AND price_nzd_mwh IS NOT NULL
GROUP BY ROUND(renewable_percentage, -1)
ORDER BY renewable_percentage_bucket;
*/

SHOW TABLES IN SCHEMA URU_RANGI;