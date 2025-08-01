-- =============================================
-- WAITA (Ocean & Marine) Schema Setup
-- Tide Predictions from LINZ for major NZ ports
-- =============================================

USE DATABASE nz_partner_hackathon;

-- =============================================
-- 1. SCHEMA CREATION
-- =============================================

CREATE SCHEMA IF NOT EXISTS WAITA 
COMMENT = 'Schema for Ocean & Marine datasets - tide predictions, marine weather, fishing conditions';

USE SCHEMA WAITA;

-- =============================================
-- 2. FILE FORMAT FOR CSV LOADING
-- =============================================

CREATE OR REPLACE FILE FORMAT waita_csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ESCAPE_UNENCLOSED_FIELD = '\\'
NULL_IF = ('NULL', 'null', '', '\\N')
ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
COMMENT = 'CSV format for WAITA ocean and marine data';

-- =============================================
-- 3. STAGE FOR FILE UPLOADS
-- =============================================

CREATE OR REPLACE STAGE marine_data_stage
COMMENT = 'Stage for ocean and marine data files (tide predictions, marine weather, etc.)';

-- =============================================
-- 4. TABLE DEFINITIONS
-- =============================================

-- Main tide predictions table
CREATE OR REPLACE TABLE tide_predictions (
    port_code STRING,
    port_name STRING,
    latitude STRING,
    longitude STRING,
    date DATE,
    day_of_week STRING,
    tide_datetime TIMESTAMP,
    tide_time STRING,
    tide_height_m NUMBER(5,2),
    tide_sequence NUMBER(1,0),
    year NUMBER(4,0),
    month NUMBER(2,0),
    day NUMBER(2,0),
    reference_info STRING,
    timezone_info STRING,
    data_source STRING,
    source_file STRING,
    load_timestamp TIMESTAMP
) COMMENT = 'LINZ tide predictions for major NZ ports (2024-2026) with daily tide times and heights';

-- Port metadata table
CREATE OR REPLACE TABLE tide_ports (
    port_code STRING PRIMARY KEY,
    port_name STRING,
    latitude STRING,
    longitude STRING,
    reference_info STRING,
    timezone_info STRING,
    data_source STRING,
    load_timestamp TIMESTAMP
) COMMENT = 'Metadata for major NZ ports with coordinates and reference information';

-- Tide statistics by port and year
CREATE OR REPLACE TABLE tide_statistics (
    port_code STRING,
    port_name STRING,
    year NUMBER(4,0),
    tide_height_m_count NUMBER(10,0),
    tide_height_m_min NUMBER(5,2),
    tide_height_m_max NUMBER(5,2),
    tide_height_m_mean NUMBER(5,2),
    tide_height_m_std NUMBER(5,2),
    date_min DATE,
    date_max DATE,
    load_timestamp TIMESTAMP
) COMMENT = 'Statistical summary of tide heights by port and year';

-- =============================================
-- 5. ANALYTICAL VIEWS
-- =============================================

-- Daily tide summary view
CREATE OR REPLACE VIEW daily_tide_summary AS
SELECT 
    port_name,
    date,
    day_of_week,
    COUNT(*) as tides_per_day,
    MIN(tide_height_m) as min_tide_height_m,
    MAX(tide_height_m) as max_tide_height_m,
    ROUND(AVG(tide_height_m), 2) as avg_tide_height_m,
    ROUND(MAX(tide_height_m) - MIN(tide_height_m), 2) as tidal_range_m,
    CASE 
        WHEN MAX(tide_height_m) - MIN(tide_height_m) > 3.0 THEN 'Large Tidal Range'
        WHEN MAX(tide_height_m) - MIN(tide_height_m) > 2.0 THEN 'Moderate Tidal Range'
        ELSE 'Small Tidal Range'
    END as tidal_range_category
FROM tide_predictions
GROUP BY port_name, date, day_of_week
ORDER BY port_name, date;

-- Monthly tide patterns view
CREATE OR REPLACE VIEW monthly_tide_patterns AS
SELECT 
    port_name,
    year,
    month,
    MONTHNAME(DATE_FROM_PARTS(year, month, 1)) as month_name,
    COUNT(*) as total_tides,
    ROUND(AVG(tide_height_m), 2) as avg_tide_height_m,
    ROUND(MIN(tide_height_m), 2) as min_tide_height_m,
    ROUND(MAX(tide_height_m), 2) as max_tide_height_m,
    ROUND(STDDEV(tide_height_m), 2) as tide_height_stddev,
    ROUND(MAX(tide_height_m) - MIN(tide_height_m), 2) as monthly_range_m
FROM tide_predictions
GROUP BY port_name, year, month
ORDER BY port_name, year, month;

-- High tide analysis view
CREATE OR REPLACE VIEW high_tide_analysis AS
SELECT 
    port_name,
    date,
    MAX(tide_height_m) as highest_tide_m,
    MIN(tide_height_m) as lowest_tide_m,
    CASE 
        WHEN MAX(tide_height_m) > 3.5 THEN 'Extreme High Tide'
        WHEN MAX(tide_height_m) > 3.0 THEN 'High Tide'
        WHEN MAX(tide_height_m) > 2.5 THEN 'Moderate High Tide'
        ELSE 'Normal Tide'
    END as high_tide_category,
    CASE 
        WHEN MIN(tide_height_m) < 0.2 THEN 'Extreme Low Tide'
        WHEN MIN(tide_height_m) < 0.5 THEN 'Low Tide'
        WHEN MIN(tide_height_m) < 1.0 THEN 'Moderate Low Tide'
        ELSE 'Normal Low Tide'
    END as low_tide_category
FROM tide_predictions
GROUP BY port_name, date
ORDER BY port_name, highest_tide_m DESC;

-- Port comparison view
CREATE OR REPLACE VIEW port_tide_comparison AS
SELECT 
    p.port_name,
    p.latitude,
    p.longitude,
    s.year,
    s.tide_height_m_count as total_tides,
    s.tide_height_m_min as min_height_m,
    s.tide_height_m_max as max_height_m,
    s.tide_height_m_mean as avg_height_m,
    s.tide_height_m_std as height_stddev,
    ROUND(s.tide_height_m_max - s.tide_height_m_min, 2) as annual_range_m,
    CASE 
        WHEN s.tide_height_m_max > 3.5 THEN 'High Tidal Port'
        WHEN s.tide_height_m_max > 2.5 THEN 'Moderate Tidal Port'
        ELSE 'Low Tidal Port'
    END as port_tidal_category
FROM tide_ports p
JOIN tide_statistics s ON p.port_code = s.port_code
ORDER BY s.year, s.tide_height_m_max DESC;

-- =============================================
-- 6. DATA LOADING COMMANDS
-- =============================================

-- Upload processed data files to stage (run after processing script)
PUT file://processed_data/tide_predictions_combined.csv @marine_data_stage;
PUT file://processed_data/tide_ports_metadata.csv @marine_data_stage;
PUT file://processed_data/tide_statistics_by_port.csv @marine_data_stage;

-- Load main tide predictions data
COPY INTO tide_predictions (
    port_code, port_name, latitude, longitude, date, day_of_week, 
    tide_datetime, tide_time, tide_height_m, tide_sequence, 
    year, month, day, reference_info, timezone_info, 
    data_source, source_file, load_timestamp
)
FROM (
    SELECT 
        $1::STRING, $2::STRING, $3::STRING, $4::STRING, $5::DATE, $6::STRING,
        $7::TIMESTAMP, $8::STRING, $9::NUMBER, $10::NUMBER,
        $11::NUMBER, $12::NUMBER, $13::NUMBER, $14::STRING, $15::STRING,
        $16::STRING, $17::STRING, $18::TIMESTAMP
    FROM @marine_data_stage/tide_predictions_combined.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = waita_csv_format);

-- Load port metadata
COPY INTO tide_ports (
    port_code, port_name, latitude, longitude, 
    reference_info, timezone_info, data_source, load_timestamp
)
FROM (
    SELECT 
        $1::STRING, $2::STRING, $3::STRING, $4::STRING,
        $5::STRING, $6::STRING, $7::STRING, $8::TIMESTAMP
    FROM @marine_data_stage/tide_ports_metadata.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = waita_csv_format);

-- Load tide statistics
COPY INTO tide_statistics (
    port_code, port_name, year, tide_height_m_count, tide_height_m_min,
    tide_height_m_max, tide_height_m_mean, tide_height_m_std,
    date_min, date_max, load_timestamp
)
FROM (
    SELECT 
        $1::STRING, $2::STRING, $3::NUMBER, $4::NUMBER, $5::NUMBER,
        $6::NUMBER, $7::NUMBER, $8::NUMBER,
        $9::DATE, $10::DATE, $11::TIMESTAMP
    FROM @marine_data_stage/tide_statistics_by_port.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = waita_csv_format);

-- =============================================
-- 7. DATA VALIDATION QUERIES
-- =============================================

-- Verify data loading
SELECT 'Tide predictions' as table_name, COUNT(*) as record_count FROM tide_predictions
UNION ALL
SELECT 'Tide ports' as table_name, COUNT(*) as record_count FROM tide_ports
UNION ALL
SELECT 'Tide statistics' as table_name, COUNT(*) as record_count FROM tide_statistics;

-- Verify port coverage
SELECT port_name, COUNT(*) as tide_records, MIN(date) as earliest_date, MAX(date) as latest_date
FROM tide_predictions
GROUP BY port_name
ORDER BY tide_records DESC;

-- Verify years and data completeness
SELECT year, COUNT(*) as total_tides, COUNT(DISTINCT port_name) as ports_covered
FROM tide_predictions
GROUP BY year
ORDER BY year;

-- Sample high and low tides by port
SELECT port_name, 
       MAX(tide_height_m) as highest_tide_m, 
       MIN(tide_height_m) as lowest_tide_m,
       ROUND(AVG(tide_height_m), 2) as avg_tide_m
FROM tide_predictions
GROUP BY port_name
ORDER BY highest_tide_m DESC;

-- =============================================
-- 8. SAMPLE ANALYTICS QUERIES
-- =============================================

-- Find extreme tide events
SELECT port_name, date, tide_time, tide_height_m, 
       CASE WHEN tide_height_m > 3.5 THEN 'Extreme High' 
            WHEN tide_height_m < 0.2 THEN 'Extreme Low' 
            ELSE 'Normal' END as tide_event
FROM tide_predictions
WHERE tide_height_m > 3.5 OR tide_height_m < 0.2
ORDER BY tide_height_m DESC;

-- Analyze tidal patterns by day of week
SELECT day_of_week, 
       COUNT(*) as total_tides,
       ROUND(AVG(tide_height_m), 2) as avg_height_m,
       ROUND(MAX(tide_height_m), 2) as max_height_m
FROM tide_predictions
GROUP BY day_of_week
ORDER BY avg_height_m DESC;

-- Monthly tidal range analysis
SELECT port_name, month, MONTHNAME(DATE_FROM_PARTS(2024, month, 1)) as month_name,
       ROUND(AVG(tide_height_m), 2) as avg_tide_height,
       ROUND(MAX(tide_height_m) - MIN(tide_height_m), 2) as avg_tidal_range
FROM tide_predictions
WHERE year = 2024
GROUP BY port_name, month
ORDER BY port_name, month;

COMMENT ON SCHEMA WAITA IS 'WAITA (Ocean & Marine) - Comprehensive tide prediction data from LINZ covering 6 major NZ ports (2024-2026) with 25,411+ tide records for maritime AI applications';