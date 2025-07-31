-- WAIPUNA RANGI (Rain & Water) - Climate Data Setup
-- Theme: Rain, Water, Climate Patterns, and Weather Analysis
-- Data Source: NIWA Climate Station Statistics

USE DATABASE nz_partner_hackathon;
USE SCHEMA WAIPUNA_RANGI;
USE WAREHOUSE COMPUTE_WH;

-- =============================================
-- 1. FILE FORMAT AND STAGE SETUP
-- =============================================

-- Create file format for NIWA CSV files
CREATE OR REPLACE FILE FORMAT niwa_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    TRIM_SPACE = TRUE
    NULL_IF = ('NULL', 'null', '', 'NA', 'N/A');

-- Create internal stage for climate data
CREATE OR REPLACE STAGE climate_data_stage
    FILE_FORMAT = niwa_csv_format;

-- =============================================
-- 2. RAINFALL DATA TABLES
-- =============================================

-- Annual rainfall data
CREATE OR REPLACE TABLE rainfall_annual (
    station_id NUMBER(4,0),
    station_name STRING,
    year NUMBER(4,0),
    total_rainfall_mm NUMBER(10,2),
    rain_days_count NUMBER(3,0),
    total_runoff_mm NUMBER(10,2),
    total_deficit_mm NUMBER(10,2),
    load_timestamp TIMESTAMP
) COMMENT = 'Annual rainfall statistics by NIWA weather station';

-- Monthly rainfall data
CREATE OR REPLACE TABLE rainfall_monthly (
    station_id NUMBER(4,0),
    station_name STRING,
    year NUMBER(4,0),
    month_name STRING,
    month_number NUMBER(2,0),
    total_rainfall_mm NUMBER(10,2),
    rain_days_count NUMBER(3,0),
    total_runoff_mm NUMBER(10,2),
    total_deficit_mm NUMBER(10,2),
    load_timestamp TIMESTAMP
) COMMENT = 'Monthly rainfall statistics by NIWA weather station';

-- =============================================
-- 3. TEMPERATURE DATA TABLES
-- =============================================

-- Annual temperature data
CREATE OR REPLACE TABLE temperature_annual (
    station_id NUMBER(4,0),
    station_name STRING,
    year NUMBER(4,0),
    mean_temperature_c NUMBER(5,2),
    mean_max_temperature_c NUMBER(5,2),
    mean_min_temperature_c NUMBER(5,2),
    extreme_grass_min_c NUMBER(5,2),
    earth_temperature_10cm_c NUMBER(5,2),
    temperature_std_dev NUMBER(5,2),
    ground_frost_days NUMBER(3,0),
    load_timestamp TIMESTAMP
) COMMENT = 'Annual temperature statistics by NIWA weather station';

-- Monthly temperature data
CREATE OR REPLACE TABLE temperature_monthly (
    station_id NUMBER(4,0),
    station_name STRING,
    year NUMBER(4,0),
    month_name STRING,
    month_number NUMBER(2,0),
    mean_temperature_c NUMBER(5,2),
    mean_max_temperature_c NUMBER(5,2),
    mean_min_temperature_c NUMBER(5,2),
    extreme_grass_min_c NUMBER(5,2),
    earth_temperature_10cm_c NUMBER(5,2),
    load_timestamp TIMESTAMP
) COMMENT = 'Monthly temperature statistics by NIWA weather station';

-- =============================================
-- 4. COMBINED CLIMATE ANALYSIS VIEWS
-- =============================================

-- Monthly climate summary view
CREATE OR REPLACE VIEW monthly_climate_summary AS
SELECT 
    r.station_id,
    r.station_name,
    r.year,
    r.month_name,
    r.month_number,
    r.total_rainfall_mm,
    r.rain_days_count,
    t.mean_temperature_c,
    t.mean_max_temperature_c,
    t.mean_min_temperature_c,
    -- Climate indicators
    CASE 
        WHEN r.total_rainfall_mm > 150 THEN 'Wet'
        WHEN r.total_rainfall_mm < 50 THEN 'Dry'
        ELSE 'Normal'
    END as rainfall_category,
    CASE 
        WHEN t.mean_temperature_c > 20 THEN 'Warm'
        WHEN t.mean_temperature_c < 10 THEN 'Cool'
        ELSE 'Moderate'
    END as temperature_category
FROM rainfall_monthly r
LEFT JOIN temperature_monthly t
    ON r.station_id = t.station_id 
    AND r.year = t.year 
    AND r.month_number = t.month_number;

-- Climate trends analysis view
CREATE OR REPLACE VIEW climate_trends AS
SELECT 
    station_id,
    station_name,
    year,
    total_rainfall_mm,
    mean_temperature_c,
    -- 5-year moving averages for trend analysis
    AVG(total_rainfall_mm) OVER (
        PARTITION BY station_id 
        ORDER BY year 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ) as rainfall_5yr_avg,
    AVG(mean_temperature_c) OVER (
        PARTITION BY station_id 
        ORDER BY year 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ) as temperature_5yr_avg,
    -- Year-over-year changes
    total_rainfall_mm - LAG(total_rainfall_mm) OVER (
        PARTITION BY station_id ORDER BY year
    ) as rainfall_yoy_change,
    mean_temperature_c - LAG(mean_temperature_c) OVER (
        PARTITION BY station_id ORDER BY year
    ) as temperature_yoy_change
FROM rainfall_annual r
LEFT JOIN temperature_annual t USING (station_id, year);

-- Extreme weather events view
CREATE OR REPLACE VIEW extreme_weather_events AS
SELECT 
    station_id,
    station_name,
    year,
    month_name,
    total_rainfall_mm,
    mean_temperature_c,
    rain_days_count,
    -- Identify extreme events
    CASE 
        WHEN total_rainfall_mm > 
            AVG(total_rainfall_mm) OVER (PARTITION BY station_id, month_number) + 
            2 * STDDEV(total_rainfall_mm) OVER (PARTITION BY station_id, month_number)
        THEN 'Extreme Rainfall'
        WHEN total_rainfall_mm < 
            AVG(total_rainfall_mm) OVER (PARTITION BY station_id, month_number) - 
            2 * STDDEV(total_rainfall_mm) OVER (PARTITION BY station_id, month_number)
        THEN 'Extreme Drought'
        ELSE 'Normal'
    END as rainfall_extreme,
    CASE 
        WHEN mean_temperature_c > 
            AVG(mean_temperature_c) OVER (PARTITION BY station_id, month_number) + 
            2 * STDDEV(mean_temperature_c) OVER (PARTITION BY station_id, month_number)
        THEN 'Heat Wave'
        WHEN mean_temperature_c < 
            AVG(mean_temperature_c) OVER (PARTITION BY station_id, month_number) - 
            2 * STDDEV(mean_temperature_c) OVER (PARTITION BY station_id, month_number)
        THEN 'Cold Snap'
        ELSE 'Normal'
    END as temperature_extreme
FROM monthly_climate_summary;

-- =============================================
-- 5. STATION METADATA
-- =============================================

-- Station information lookup
CREATE OR REPLACE TABLE climate_stations (
    station_id NUMBER(4,0) PRIMARY KEY,
    station_name STRING,
    region STRING,
    latitude NUMBER(10,6),
    longitude NUMBER(10,6),
    elevation_m NUMBER(6,1),
    data_start_year NUMBER(4,0),
    data_end_year NUMBER(4,0),
    active_parameters ARRAY,
    notes STRING
) COMMENT = 'NIWA climate station metadata and geographic information';

-- Insert known station data
INSERT INTO climate_stations VALUES
(1464, 'Historic Station 1464', 'Unknown', NULL, NULL, NULL, 1933, 1984, ['rainfall', 'temperature'], 'Historic data 1933-1984'),
(2109, 'Primary Station 2109', 'Unknown', NULL, NULL, NULL, 1946, 1984, ['rainfall', 'temperature'], 'Primary dataset 1946-1984'),
(4960, 'Modern Station 4960', 'Unknown', NULL, NULL, NULL, 2000, 2024, ['rainfall', 'temperature'], 'Modern data 2000+');

-- =============================================
-- NEXT STEPS:
-- 1. Run processing script to load NIWA CSV data
-- 2. Create sample queries for climate analysis
-- 3. Build data sharing setup for participants
-- =============================================