-- =============================================
-- HIWA_I_TE_RANGI (Travel & Tourism) Schema Setup
-- Events, Tourism Statistics, and Travel Data
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE NZ_PARTNER_HACKATHON;

-- =============================================
-- 1. SCHEMA CREATION
-- =============================================

CREATE SCHEMA IF NOT EXISTS HIWA_I_TE_RANGI 
COMMENT = 'Schema for Travel & Tourism datasets - events, visitor arrivals, accommodation stats';

USE SCHEMA HIWA_I_TE_RANGI;

-- =============================================
-- 2. FILE FORMATS
-- =============================================

-- CSV format for tourism statistics
CREATE OR REPLACE FILE FORMAT tourism_csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
ESCAPE_UNENCLOSED_FIELD = '\\'
NULL_IF = ('NULL', 'null', '', '\\N', 'NA', 'N/A')
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
TRIM_SPACE = TRUE
COMMENT = 'CSV format for HIWA_I_TE_RANGI tourism and event data';

-- =============================================
-- 3. STAGES
-- =============================================

-- Stage for tourism data files
CREATE OR REPLACE STAGE tourism_data_stage
FILE_FORMAT = tourism_csv_format
COMMENT = 'Stage for tourism statistics, event data, and travel information';

-- =============================================
-- 4. EVENTFINDA EVENTS TABLE
-- =============================================

-- Events from EventFinda RSS feed
CREATE OR REPLACE TABLE eventfinda_events (
    event_id STRING,
    title STRING NOT NULL,
    description STRING,
    location_text STRING,
    city STRING,
    region STRING,
    date_info_original STRING COMMENT 'Original date string from RSS',
    start_date DATE,
    end_date DATE,
    is_recurring BOOLEAN DEFAULT FALSE,
    category STRING COMMENT 'Arts & Culture, Sports & Recreation, Music & Performance, etc.',
    event_url STRING,
    publication_date TIMESTAMP,
    fetch_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    data_source STRING DEFAULT 'EventFinda RSS',
    rss_feed_url STRING DEFAULT 'https://www.eventfinda.co.nz/feed/events/new-zealand/whatson/upcoming.rss'
) COMMENT = 'Events from EventFinda RSS feed for tourism event analysis';

-- =============================================
-- 5. TOURISM STATISTICS TABLES
-- =============================================

-- Visitor arrivals total (annual)
CREATE OR REPLACE TABLE visitor_arrivals (
    report_year NUMBER(4,0),
    report_date DATE COMMENT 'Annual period end date',
    period_type STRING DEFAULT 'Annual',
    visitor_arrivals NUMBER(10,0) COMMENT 'Total visitor arrivals for the year',
    data_source STRING DEFAULT 'Stats NZ ITM475712',
    dataset_description STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Visitor arrival totals by year from Stats NZ';

-- Total passenger movements (annual)
CREATE OR REPLACE TABLE passenger_movements (
    report_year NUMBER(4,0),
    report_date DATE COMMENT 'Annual period end date',
    period_type STRING DEFAULT 'Annual',
    arrivals_actual NUMBER(10,0) COMMENT 'Actual arrival counts',
    arrivals_sample NUMBER(10,0) COMMENT 'Sample-derived arrival counts',
    departures_actual NUMBER(10,0) COMMENT 'Actual departure counts',
    departures_sample NUMBER(10,0) COMMENT 'Sample-derived departure counts',
    total_actual NUMBER(10,0) COMMENT 'Total actual movements',
    total_sample NUMBER(10,0) COMMENT 'Total sample-derived movements',
    data_source STRING DEFAULT 'Stats NZ ITM332206',
    dataset_description STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Total passenger movements by year from Stats NZ';

-- Guest nights by region (monthly)
CREATE OR REPLACE TABLE guest_nights_by_region (
    report_date DATE COMMENT 'Monthly reporting period',
    period_type STRING DEFAULT 'Monthly',
    region STRING,
    guest_nights NUMBER(15,2) COMMENT 'Total guest nights in thousands',
    domestic_guest_nights NUMBER(15,2) COMMENT 'Domestic guest nights',
    domestic_guest_nights_seasonally_adjusted NUMBER(15,2),
    domestic_guest_nights_trend NUMBER(15,2),
    international_guest_nights NUMBER(15,2) COMMENT 'International guest nights',
    international_guest_nights_seasonally_adjusted NUMBER(15,2),
    international_guest_nights_trend NUMBER(15,2),
    data_source STRING DEFAULT 'Stats NZ ACS348801',
    dataset_description STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Guest nights by region and type from Stats NZ Accommodation Survey';

-- Occupancy rates by region (monthly)
CREATE OR REPLACE TABLE occupancy_rates_by_region (
    report_date DATE COMMENT 'Monthly reporting period',
    period_type STRING DEFAULT 'Monthly',
    region STRING,
    occupancy_rate NUMBER(5,2) COMMENT 'Overall occupancy rate percentage',
    occupancy_rate_excluding_holiday_parks NUMBER(5,2),
    capacity_stay_units_excluding_holiday_parks NUMBER(10,0) COMMENT 'Available accommodation units',
    data_source STRING DEFAULT 'Stats NZ ACS348401',
    dataset_description STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Accommodation occupancy rates by region from Stats NZ';

-- Migrant arrivals (annual)
CREATE OR REPLACE TABLE migrant_arrivals (
    report_year NUMBER(4,0),
    report_date DATE COMMENT 'Annual period end date (April)',
    period_type STRING DEFAULT 'Annual',
    total_migrant_arrivals NUMBER(10,0) COMMENT 'Total estimated migrant arrivals',
    data_source STRING DEFAULT 'Stats NZ ITM553006',
    dataset_description STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Estimated migrant arrivals by year from Stats NZ';

-- =============================================
-- 6. ANALYTICAL VIEWS
-- =============================================

-- Events by month and category
CREATE OR REPLACE VIEW events_monthly_summary AS
SELECT 
    DATE_TRUNC('month', start_date) as event_month,
    region,
    category,
    COUNT(*) as event_count,
    COUNT(DISTINCT city) as cities_with_events,
    COUNT(CASE WHEN is_recurring THEN 1 END) as recurring_events,
    COUNT(CASE WHEN NOT is_recurring THEN 1 END) as one_time_events
FROM eventfinda_events 
WHERE start_date IS NOT NULL
GROUP BY event_month, region, category
ORDER BY event_month, region, category;

-- Tourism demand vs events correlation
CREATE OR REPLACE VIEW tourism_events_correlation AS
SELECT 
    g.report_date,
    g.region,
    g.guest_nights,
    g.domestic_guest_nights,
    g.international_guest_nights,
    o.occupancy_rate,
    o.occupancy_rate_excluding_holiday_parks,
    e.event_count,
    e.event_categories,
    CASE 
        WHEN e.event_count > 50 THEN 'High Event Activity'
        WHEN e.event_count > 20 THEN 'Medium Event Activity' 
        WHEN e.event_count > 0 THEN 'Low Event Activity'
        ELSE 'No Events'
    END as event_activity_level
FROM guest_nights_by_region g
LEFT JOIN occupancy_rates_by_region o 
    ON g.report_date = o.report_date 
    AND g.region = o.region
LEFT JOIN (
    SELECT 
        DATE_TRUNC('month', start_date) as event_month,
        region,
        COUNT(*) as event_count,
        LISTAGG(DISTINCT category, ', ') as event_categories
    FROM eventfinda_events
    WHERE start_date IS NOT NULL
    GROUP BY event_month, region
) e ON g.report_date = e.event_month AND g.region = e.region;

-- Regional tourism performance
CREATE OR REPLACE VIEW regional_tourism_performance AS
SELECT 
    region,
    COUNT(DISTINCT report_date) as months_of_data,
    AVG(guest_nights) as avg_monthly_guest_nights,
    AVG(domestic_guest_nights) as avg_domestic_guest_nights,
    AVG(international_guest_nights) as avg_international_guest_nights,
    AVG(occupancy_rate) as avg_occupancy_rate,
    MAX(guest_nights) as peak_guest_nights,
    MAX(occupancy_rate) as peak_occupancy,
    STDDEV(guest_nights) as guest_nights_volatility,
    STDDEV(occupancy_rate) as occupancy_volatility
FROM tourism_events_correlation
WHERE report_date >= '2020-01-01'
GROUP BY region
ORDER BY avg_monthly_guest_nights DESC;

-- =============================================
-- 7. EVENT CATEGORY ANALYSIS FUNCTIONS
-- =============================================

-- Function to calculate tourism impact score
CREATE OR REPLACE FUNCTION calculate_tourism_impact_score(
    guest_nights NUMBER,
    occupancy_rate NUMBER,
    event_count NUMBER
)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
    CASE 
        WHEN guest_nights IS NULL OR occupancy_rate IS NULL THEN NULL
        ELSE 
            (guest_nights / 1000) * 0.4 +  -- Guest nights weighted 40%
            (occupancy_rate / 100) * 0.4 + -- Occupancy rate weighted 40% 
            (COALESCE(event_count, 0) / 10) * 0.2  -- Event activity weighted 20%
    END
$$;

-- =============================================
-- 8. SAMPLE DATA LOADING COMMANDS
-- =============================================

/*
-- Load EventFinda events data
COPY INTO eventfinda_events
FROM @tourism_data_stage/eventfinda_events.csv
FILE_FORMAT = tourism_csv_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- Load Stats NZ tourism data
COPY INTO visitor_arrivals
FROM @tourism_data_stage/visitor_arrivals.csv
FILE_FORMAT = tourism_csv_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO passenger_movements  
FROM @tourism_data_stage/passenger_movements.csv
FILE_FORMAT = tourism_csv_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO guest_nights_by_region
FROM @tourism_data_stage/guest_nights_by_region.csv
FILE_FORMAT = tourism_csv_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO occupancy_rates_by_region
FROM @tourism_data_stage/occupancy_rates_by_region.csv  
FILE_FORMAT = tourism_csv_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO migrant_arrivals
FROM @tourism_data_stage/migrant_arrivals.csv
FILE_FORMAT = tourism_csv_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
*/

-- =============================================
-- 9. DATA QUALITY CHECKS
-- =============================================

-- Event data quality check
CREATE OR REPLACE VIEW event_data_quality AS
SELECT 
    'EventFinda Events' as dataset,
    COUNT(*) as total_records,
    COUNT(CASE WHEN title IS NULL OR title = '' THEN 1 END) as missing_titles,
    COUNT(CASE WHEN start_date IS NULL THEN 1 END) as missing_dates,
    COUNT(CASE WHEN region IS NULL OR region = '' THEN 1 END) as missing_regions,
    COUNT(DISTINCT region) as unique_regions,
    COUNT(DISTINCT category) as unique_categories,
    MIN(start_date) as earliest_event,
    MAX(start_date) as latest_event
FROM eventfinda_events;

-- =============================================
-- 8. AIRFARES TABLE FOR TOURISM COST ANALYSIS
-- =============================================

CREATE OR REPLACE TABLE nz_airfares (
    travel_date DATE COMMENT 'Date of travel',
    dep_airport STRING COMMENT 'Departure airport code (e.g., AKL, CHC)',
    dep_time TIME COMMENT 'Departure time',
    arr_airport STRING COMMENT 'Arrival airport code', 
    arr_time TIME COMMENT 'Arrival time',
    duration STRING COMMENT 'Flight duration',
    direct STRING COMMENT 'Direct flight indicator',
    transit STRING COMMENT 'Transit information if not direct',
    baggage STRING COMMENT 'Baggage information',
    airline STRING COMMENT 'Airline name',
    airfare_nz NUMBER(10,2) COMMENT 'Airfare in New Zealand dollars',
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    data_source STRING DEFAULT 'Kaggle - shashwatwork/airfares-in-new-zealand',
    dataset_description STRING,
    data_freshness STRING
) COMMENT = 'New Zealand domestic airfares data for tourism cost analysis';

-- =============================================
-- 9. ENHANCED VIEWS WITH AIRFARES INTEGRATION
-- =============================================

-- Tourism cost analysis view combining events and airfares
CREATE OR REPLACE VIEW tourism_cost_analysis AS
SELECT 
    e.region,
    e.category as event_category,
    COUNT(e.event_id) as events_count,
    -- Airfare analysis for major airports in regions
    CASE e.region
        WHEN 'Auckland' THEN 'AKL'
        WHEN 'Wellington' THEN 'WLG' 
        WHEN 'Canterbury' THEN 'CHC'
        WHEN 'Otago' THEN 'DUD'
        WHEN 'Taranaki' THEN 'NPL'
        ELSE 'OTHER'
    END as regional_airport,
    -- Get average airfares for the region
    (SELECT AVG(airfare_nz) 
     FROM nz_airfares af 
     WHERE af.arr_airport = CASE e.region
         WHEN 'Auckland' THEN 'AKL'
         WHEN 'Wellington' THEN 'WLG'
         WHEN 'Canterbury' THEN 'CHC'
         WHEN 'Otago' THEN 'DUD'
         WHEN 'Taranaki' THEN 'NPL'
         ELSE NULL
     END) as avg_airfare_to_region,
    -- Tourism accessibility score (events per dollar)
    CASE 
        WHEN (SELECT AVG(airfare_nz) FROM nz_airfares af WHERE af.arr_airport = CASE e.region WHEN 'Auckland' THEN 'AKL' WHEN 'Wellington' THEN 'WLG' WHEN 'Canterbury' THEN 'CHC' WHEN 'Otago' THEN 'DUD' WHEN 'Taranaki' THEN 'NPL' ELSE NULL END) > 0
        THEN ROUND(COUNT(e.event_id) * 1000.0 / (SELECT AVG(airfare_nz) FROM nz_airfares af WHERE af.arr_airport = CASE e.region WHEN 'Auckland' THEN 'AKL' WHEN 'Wellington' THEN 'WLG' WHEN 'Canterbury' THEN 'CHC' WHEN 'Otago' THEN 'DUD' WHEN 'Taranaki' THEN 'NPL' ELSE NULL END), 2)
        ELSE NULL
    END as tourism_value_score
FROM eventfinda_events e
WHERE e.region != 'Other'
GROUP BY e.region, e.category;

-- Regional airfare competitiveness view
CREATE OR REPLACE VIEW regional_airfare_analysis AS
SELECT 
    arr_airport,
    CASE arr_airport
        WHEN 'AKL' THEN 'Auckland'
        WHEN 'WLG' THEN 'Wellington'
        WHEN 'CHC' THEN 'Christchurch'
        WHEN 'DUD' THEN 'Dunedin'
        WHEN 'NPL' THEN 'New Plymouth'
        ELSE arr_airport
    END as region_name,
    COUNT(*) as total_flights,
    AVG(airfare_nz) as avg_airfare,
    MIN(airfare_nz) as min_airfare,
    MAX(airfare_nz) as max_airfare,
    STDDEV(airfare_nz) as price_volatility,
    COUNT(DISTINCT airline) as airlines_serving,
    -- Price competitiveness ranking
    ROW_NUMBER() OVER (ORDER BY AVG(airfare_nz)) as affordability_rank,
    -- Flight frequency ranking  
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as accessibility_rank
FROM nz_airfares
WHERE arr_airport IN ('AKL', 'WLG', 'CHC', 'DUD', 'NPL')
GROUP BY arr_airport;

-- =============================================
-- 10. SAMPLE QUERIES FOR TESTING
-- =============================================

-- Test basic event data
-- SELECT * FROM eventfinda_events LIMIT 10;

-- Test monthly event summary
-- SELECT * FROM events_monthly_summary WHERE event_month >= '2025-07-01' LIMIT 20;

-- Test regional performance (when tourism data is loaded)
-- SELECT * FROM regional_tourism_performance LIMIT 10;

-- Test airfares integration
-- SELECT * FROM tourism_cost_analysis ORDER BY tourism_value_score DESC LIMIT 10;

-- Test regional airfare analysis  
-- SELECT * FROM regional_airfare_analysis ORDER BY affordability_rank LIMIT 10;

SELECT 'HIWA_I_TE_RANGI schema with airfares integration completed successfully' as status;