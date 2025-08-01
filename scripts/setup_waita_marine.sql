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
    latitude_dms STRING COMMENT 'Original DMS format (e.g., 36°51''S)',
    longitude_dms STRING COMMENT 'Original DMS format (e.g., 174°46''E)',
    latitude_decimal FLOAT COMMENT 'Decimal degrees latitude (WGS 84, negative for South)',
    longitude_decimal FLOAT COMMENT 'Decimal degrees longitude (WGS 84, negative for West)',
    location_point GEOGRAPHY COMMENT 'Geographic point using Snowflake GEOGRAPHY type (SRID 4326)',
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
) COMMENT = 'LINZ tide predictions for major NZ ports (2024-2026) with WGS 84 coordinates and Snowflake GEOGRAPHY points';

-- Port metadata table
CREATE OR REPLACE TABLE tide_ports (
    port_code STRING PRIMARY KEY,
    port_name STRING,
    latitude_dms STRING COMMENT 'Original DMS format (e.g., 36°51''S)',
    longitude_dms STRING COMMENT 'Original DMS format (e.g., 174°46''E)',
    latitude_decimal FLOAT COMMENT 'Decimal degrees latitude (WGS 84, negative for South)',
    longitude_decimal FLOAT COMMENT 'Decimal degrees longitude (WGS 84, negative for West)', 
    location_point GEOGRAPHY COMMENT 'Geographic point using Snowflake GEOGRAPHY type (SRID 4326)',
    reference_info STRING,
    timezone_info STRING,
    data_source STRING,
    load_timestamp TIMESTAMP
) COMMENT = 'Metadata for major NZ ports with WGS 84 coordinates and Snowflake GEOGRAPHY points from LINZ';

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

-- Maritime incidents and accidents from Maritime NZ
CREATE OR REPLACE TABLE maritime_incidents (
    event_id STRING PRIMARY KEY,
    event_date_original STRING,
    event_date DATE,
    event_year NUMBER(4,0),
    event_month NUMBER(2,0),
    event_quarter NUMBER(1,0),
    brief_description STRING,
    what_happened STRING,
    incident_severity STRING COMMENT 'Critical, Major, Moderate, Minor',
    event_location STRING,
    latitude_decimal FLOAT COMMENT 'Decimal degrees latitude (WGS 84)',
    longitude_decimal FLOAT COMMENT 'Decimal degrees longitude (WGS 84)',
    location_point GEOGRAPHY COMMENT 'Geographic point using Snowflake GEOGRAPHY type (SRID 4326)',
    nz_region STRING,
    where_happened STRING COMMENT 'Offshore, Inshore, In harbour, At berth, etc.',
    sector STRING COMMENT 'Domestic Commercial, International Commercial, Recreational',
    injured_persons NUMBER(5,0),
    vessel_type STRING,
    safety_system STRING,
    country_flag STRING,
    gross_tonnage NUMBER(10,2),
    length_overall NUMBER(8,2),
    year_of_build NUMBER(4,0),
    vessel_age_at_incident NUMBER(3,0),
    data_source STRING,
    source_url STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Maritime NZ accident and incident reports (2018-2024) with safety analysis and vessel details';

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
    p.latitude_decimal,
    p.longitude_decimal,
    p.location_point,
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
-- Load tide predictions with GEOGRAPHY point creation  
COPY INTO tide_predictions (port_code, port_name, latitude_dms, longitude_dms, latitude_decimal, longitude_decimal,
                           date, day_of_week, tide_datetime, tide_time, tide_height_m, tide_sequence,
                           year, month, day, reference_info, timezone_info, data_source, source_file, load_timestamp)
FROM (
    SELECT 
        $1::STRING,      -- port_code
        $2::STRING,      -- port_name
        $3::STRING,      -- latitude_dms  
        $4::STRING,      -- longitude_dms
        $5::FLOAT,       -- latitude_decimal
        $6::FLOAT,       -- longitude_decimal
        $7::DATE,        -- date
        $8::STRING,      -- day_of_week
        $9::TIMESTAMP,   -- tide_datetime
        $10::STRING,     -- tide_time
        $11::FLOAT,      -- tide_height_m
        $12::NUMBER,     -- tide_sequence
        $13::NUMBER,     -- year
        $14::NUMBER,     -- month
        $15::NUMBER,     -- day
        $16::STRING,     -- reference_info
        $17::STRING,     -- timezone_info
        $18::STRING,     -- data_source
        $19::STRING,     -- source_file
        $20::TIMESTAMP   -- load_timestamp
    FROM @marine_data_stage/tide_predictions_combined.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = waita_csv_format);

-- Update location_point using GEOGRAPHY constructor
UPDATE tide_predictions 
SET location_point = TO_GEOGRAPHY('POINT(' || longitude_decimal || ' ' || latitude_decimal || ')')
WHERE latitude_decimal IS NOT NULL AND longitude_decimal IS NOT NULL;

-- Load port metadata with GEOGRAPHY point creation
COPY INTO tide_ports (port_code, port_name, latitude_dms, longitude_dms, latitude_decimal, longitude_decimal, 
                      reference_info, timezone_info, data_source, load_timestamp)
FROM (
    SELECT 
        $1::STRING,   -- port_code
        $2::STRING,   -- port_name  
        $3::STRING,   -- latitude_dms
        $4::STRING,   -- longitude_dms
        $5::FLOAT,    -- latitude_decimal
        $6::FLOAT,    -- longitude_decimal
        $7::STRING,   -- reference_info
        $8::STRING,   -- timezone_info
        $9::STRING,   -- data_source
        $10::TIMESTAMP -- load_timestamp
    FROM @marine_data_stage/tide_ports_metadata.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = waita_csv_format);

-- Update location_point using GEOGRAPHY constructor
UPDATE tide_ports 
SET location_point = TO_GEOGRAPHY('POINT(' || longitude_decimal || ' ' || latitude_decimal || ')')
WHERE latitude_decimal IS NOT NULL AND longitude_decimal IS NOT NULL;

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

-- Load maritime incidents data (run after processing script)
PUT file://processed_data/maritime_incidents_processed.csv @marine_data_stage;

COPY INTO maritime_incidents (
    event_id, event_date_original, event_date, event_year, event_month, event_quarter,
    brief_description, what_happened, incident_severity, event_location,
    latitude_decimal, longitude_decimal, nz_region, where_happened, sector,
    injured_persons, vessel_type, safety_system, country_flag, gross_tonnage,
    length_overall, year_of_build, vessel_age_at_incident, data_source, source_url, load_timestamp
)
FROM (
    SELECT 
        $1::STRING, $2::STRING, $3::DATE, $4::NUMBER, $5::NUMBER, $6::NUMBER,
        $7::STRING, $8::STRING, $9::STRING, $10::STRING,
        $11::FLOAT, $12::FLOAT, $13::STRING, $14::STRING, $15::STRING,
        $16::NUMBER, $17::STRING, $18::STRING, $19::STRING, $20::NUMBER,
        $21::NUMBER, $22::NUMBER, $23::NUMBER, $24::STRING, $25::STRING, $26::TIMESTAMP
    FROM @marine_data_stage/maritime_incidents_processed.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = waita_csv_format);

-- Update location_point using GEOGRAPHY constructor for incidents
UPDATE maritime_incidents 
SET location_point = TO_GEOGRAPHY('POINT(' || longitude_decimal || ' ' || latitude_decimal || ')')
WHERE latitude_decimal IS NOT NULL AND longitude_decimal IS NOT NULL;

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