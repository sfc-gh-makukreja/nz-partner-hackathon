-- =============================================
-- WAITA (Ocean & Marine) - Tide Prediction Analytics
-- Sample queries for exploring LINZ tide data across major NZ ports
-- Data Source: Land Information New Zealand (LINZ) - https://static.charts.linz.govt.nz/tide-tables/maj-ports/
-- =============================================

USE DATABASE nz_partner_hackathon;
USE SCHEMA WAITA;

-- =============================================
-- 1. DATA EXPLORATION QUERIES
-- =============================================

-- Check available ports and data coverage with geospatial coordinates
SELECT 
    p.port_name,
    p.port_code,
    p.latitude_decimal,
    p.longitude_decimal,
    COUNT(*) as total_predictions,
    MIN(t.date) as earliest_date,
    MAX(t.date) as latest_date,
    COUNT(DISTINCT t.date) as days_covered
FROM tide_ports p
LEFT JOIN tide_predictions t ON p.port_code = t.port_code
GROUP BY p.port_name, p.port_code, p.latitude_decimal, p.longitude_decimal
ORDER BY p.port_name;

-- Overview of tide data by year and port
SELECT 
    EXTRACT(YEAR FROM date) as year,
    port_code,
    COUNT(*) as total_predictions,
    ROUND(AVG(tide_height_m), 2) as avg_tide_height_m,
    ROUND(MAX(tide_height_m), 2) as max_tide_height_m,
    ROUND(MIN(tide_height_m), 2) as min_tide_height_m
FROM tide_predictions 
GROUP BY EXTRACT(YEAR FROM date), port_code
ORDER BY year, port_code;

-- =============================================
-- 2. TIDE PATTERN ANALYSIS
-- =============================================

-- Daily tide patterns - hourly timing analysis
SELECT 
    port_code,
    EXTRACT(HOUR FROM TRY_TO_TIME(tide_time)) as hour_of_day,
    tide_sequence,
    COUNT(*) as frequency,
    ROUND(AVG(tide_height_m), 2) as avg_height_m
FROM tide_predictions 
WHERE date >= '2024-01-01'
GROUP BY port_code, EXTRACT(HOUR FROM TRY_TO_TIME(tide_time)), tide_sequence
ORDER BY port_code, hour_of_day, tide_sequence;

-- Monthly extreme tide analysis
SELECT 
    port_code,
    EXTRACT(YEAR FROM date) as year,
    EXTRACT(MONTH FROM date) as month,
    MONTHNAME(date) as month_name,
    ROUND(MAX(tide_height_m), 2) as highest_tide_m,
    ROUND(MIN(tide_height_m), 2) as lowest_tide_m,
    ROUND(MAX(tide_height_m) - MIN(tide_height_m), 2) as max_tide_range_m
FROM tide_predictions 
GROUP BY port_code, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date), MONTHNAME(date)
ORDER BY port_code, year, month;

-- =============================================
-- 3. MARINE PLANNING & NAVIGATION QUERIES
-- =============================================

-- High tide windows for vessel navigation (tides > 2.5m)
SELECT 
    port_code,
    date,
    tide_time,
    tide_height_m,
    LAG(tide_time) OVER (PARTITION BY port_code, date ORDER BY tide_time) as prev_tide_time,
    DATEDIFF('minute', 
        LAG(TRY_TO_TIME(tide_time)) OVER (PARTITION BY port_code, date ORDER BY tide_time), 
        TRY_TO_TIME(tide_time)) as minutes_since_last_tide
FROM tide_predictions 
WHERE tide_height_m > 2.5
  AND date BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY port_code, date, tide_time;

-- Best fishing times (2 hours around high tide)
SELECT 
    port_code,
    date,
    tide_time as high_tide_time,
    tide_height_m,
    DATEADD('hour', -2, TRY_TO_TIME(tide_time)) as fishing_window_start,
    DATEADD('hour', 2, TRY_TO_TIME(tide_time)) as fishing_window_end,
    CASE 
        WHEN EXTRACT(HOUR FROM TRY_TO_TIME(tide_time)) BETWEEN 5 AND 8 THEN 'Morning Prime Time'
        WHEN EXTRACT(HOUR FROM TRY_TO_TIME(tide_time)) BETWEEN 17 AND 20 THEN 'Evening Prime Time'
        ELSE 'Standard Time'
    END as fishing_quality
FROM tide_predictions 
WHERE tide_height_m > 2.0  -- Focus on higher tides for better fishing
  AND date BETWEEN CURRENT_DATE() AND DATEADD('day', 7, CURRENT_DATE())
ORDER BY port_code, date, tide_time;

-- =============================================
-- 4. SAFETY & RISK ANALYSIS
-- =============================================

-- Extreme tide events (King Tides - top 5% highest)
WITH tide_percentiles AS (
    SELECT 
        port_code,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY tide_height_m) as p95_height
    FROM tide_predictions 
    GROUP BY port_code
)
SELECT 
    t.port_code,
    t.date,
    t.tide_time,
    t.tide_height_m,
    tp.p95_height,
    ROUND(((t.tide_height_m - tp.p95_height) / tp.p95_height) * 100, 1) as pct_above_p95,
    'King Tide Event' as event_type
FROM tide_predictions t
JOIN tide_percentiles tp ON t.port_code = tp.port_code
WHERE t.tide_height_m > tp.p95_height
ORDER BY t.port_code, t.date;

-- Low tide exposure analysis (for coastal infrastructure)
SELECT 
    port_code,
    date,
    tide_time,
    tide_height_m,
    CASE 
        WHEN tide_height_m < 0.5 THEN 'Extreme Low Exposure'
        WHEN tide_height_m < 1.0 THEN 'High Exposure'
        WHEN tide_height_m < 1.5 THEN 'Moderate Exposure'
        ELSE 'Low Exposure'
    END as exposure_level,
    LAG(tide_height_m) OVER (PARTITION BY port_code ORDER BY date, tide_time) as prev_height,
    LEAD(tide_height_m) OVER (PARTITION BY port_code ORDER BY date, tide_time) as next_height
FROM tide_predictions 
WHERE tide_height_m < 1.5  -- Focus on low tide conditions
ORDER BY port_code, date, tide_time;

-- =============================================
-- 5. COMPARATIVE PORT ANALYSIS
-- =============================================

-- Port comparison - Average tide characteristics
SELECT 
    port_code,
    COUNT(*) as total_observations,
    ROUND(MIN(tide_height_m), 2) as min_tide_m,
    ROUND(MAX(tide_height_m), 2) as max_tide_m,
    ROUND(AVG(tide_height_m), 2) as avg_tide_m,
    ROUND(MAX(tide_height_m) - MIN(tide_height_m), 2) as tidal_range_m,
    ROUND(STDDEV(tide_height_m), 2) as tide_variability
FROM tide_predictions 
GROUP BY port_code
ORDER BY tidal_range_m DESC;

-- Seasonal tide comparison across ports
SELECT 
    port_code,
    CASE 
        WHEN EXTRACT(MONTH FROM tide_datetime) IN (12, 1, 2) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM tide_datetime) IN (3, 4, 5) THEN 'Autumn'
        WHEN EXTRACT(MONTH FROM tide_datetime) IN (6, 7, 8) THEN 'Winter'
        ELSE 'Spring'
    END as season,
    ROUND(AVG(tide_height_m), 2) as avg_tide_height_m,
    ROUND(MAX(tide_height_m), 2) as max_tide_height_m,
    ROUND(MIN(tide_height_m), 2) as min_tide_height_m,
    COUNT(*) as observations
FROM tide_predictions 
GROUP BY port_code, 
    CASE 
        WHEN EXTRACT(MONTH FROM tide_datetime) IN (12, 1, 2) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM tide_datetime) IN (3, 4, 5) THEN 'Autumn'
        WHEN EXTRACT(MONTH FROM tide_datetime) IN (6, 7, 8) THEN 'Winter'
        ELSE 'Spring'
    END
ORDER BY port_code, season;

-- =============================================
-- 6. USING VIEWS FOR COMPLEX ANALYSIS
-- =============================================

-- Query the daily_tide_summary view
SELECT 
    port_name,
    date,
    tides_per_day,
    min_tide_height_m,
    max_tide_height_m,
    tidal_range_m,
    tidal_range_category
FROM daily_tide_summary 
WHERE date >= '2024-01-01'
  AND tidal_range_category IN ('Large Tidal Range', 'Moderate Tidal Range')
ORDER BY tidal_range_m DESC
LIMIT 20;

-- Query the high_tide_analysis view for navigation
SELECT 
    port_name,
    date,
    highest_tide_m,
    lowest_tide_m,
    high_tide_category,
    low_tide_category
FROM high_tide_analysis 
WHERE port_name = 'Auckland'
  AND date >= CURRENT_DATE()
  AND high_tide_category IN ('High Tide', 'Extreme High Tide')
ORDER BY highest_tide_m DESC
LIMIT 10;

-- =============================================
-- 7. AI-READY FEATURES & ADVANCED ANALYTICS
-- =============================================

-- Time series features for ML models
SELECT 
    port_code,
    date,
    tide_time,
    tide_height_m,
    tide_sequence,
    -- Time-based features
    EXTRACT(HOUR FROM TRY_TO_TIME(tide_time)) as hour,
    EXTRACT(DAYOFWEEK FROM date) as day_of_week,
    EXTRACT(DAYOFYEAR FROM date) as day_of_year,
    EXTRACT(WEEK FROM date) as week_of_year,
    -- Lag features
    LAG(tide_height_m, 1) OVER (PARTITION BY port_code ORDER BY date, tide_time) as prev_tide_height,
    LAG(tide_height_m, 2) OVER (PARTITION BY port_code ORDER BY date, tide_time) as prev_2_tide_height,
    -- Rolling statistics
    AVG(tide_height_m) OVER (
        PARTITION BY port_code 
        ORDER BY date, tide_time 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7_avg_height
FROM tide_predictions 
WHERE port_code LIKE '%070%'  -- Auckland example
ORDER BY date, tide_time;

-- Tide prediction accuracy analysis (compare actual patterns)
WITH tide_cycles AS (
    SELECT 
        port_code,
        date,
        COUNT(*) as daily_total_tides,
        MAX(tide_height_m) - MIN(tide_height_m) as daily_range,
        EXTRACT(HOUR FROM TRY_TO_TIME(MIN(tide_time))) as first_tide_hour,
        ROUND(AVG(tide_height_m), 2) as avg_daily_height
    FROM tide_predictions 
    GROUP BY port_code, date
)
SELECT 
    port_code,
    daily_total_tides,
    ROUND(AVG(daily_range), 2) as avg_daily_range_m,
    ROUND(STDDEV(daily_range), 2) as range_variability,
    ROUND(AVG(avg_daily_height), 2) as avg_tide_height_m,
    COUNT(*) as days_observed,
    -- Typical tide cycle analysis
    CASE 
        WHEN daily_total_tides BETWEEN 6 AND 8 THEN 'Semi-diurnal (typical - 3-4 tides per day)'
        WHEN daily_total_tides BETWEEN 2 AND 4 THEN 'Diurnal (2 tides per day)'
        ELSE 'Mixed/Irregular'
    END as tide_pattern_type
FROM tide_cycles
GROUP BY port_code, daily_total_tides,
    CASE 
        WHEN daily_total_tides BETWEEN 6 AND 8 THEN 'Semi-diurnal (typical - 3-4 tides per day)'
        WHEN daily_total_tides BETWEEN 2 AND 4 THEN 'Diurnal (2 tides per day)'
        ELSE 'Mixed/Irregular'
    END
ORDER BY port_code, daily_total_tides DESC;

-- =============================================
-- 8. GEOSPATIAL ANALYSIS QUERIES
-- =============================================

-- Port distances using Snowflake GEOGRAPHY functions
SELECT 
    p1.port_name as from_port,
    p2.port_name as to_port,
    ROUND(ST_DISTANCE(p1.location_point, p2.location_point) / 1000, 2) as distance_km
FROM tide_ports p1
CROSS JOIN tide_ports p2
WHERE p1.port_name < p2.port_name  -- Avoid duplicate pairs
ORDER BY distance_km;

-- Nearest port to a specific coordinate (example: Wellington region)
SELECT 
    port_name,
    latitude_decimal,
    longitude_decimal,
    ROUND(ST_DISTANCE(location_point, TO_GEOGRAPHY('POINT(174.78 -41.29)')) / 1000, 2) as distance_from_wellington_km
FROM tide_ports
ORDER BY distance_from_wellington_km;

-- Coastal clustering analysis (group ports by proximity)
SELECT 
    port_name,
    latitude_decimal,
    longitude_decimal,
    CASE 
        WHEN latitude_decimal > -38 THEN 'North Island - Upper'
        WHEN latitude_decimal > -42 THEN 'North Island - Lower' 
        WHEN latitude_decimal > -44 THEN 'South Island - Upper'
        ELSE 'South Island - Lower'
    END as coastal_region,
    CASE
        WHEN longitude_decimal < 172 THEN 'West Coast'
        WHEN longitude_decimal < 175 THEN 'Central'
        ELSE 'East Coast'
    END as coast_orientation
FROM tide_ports
ORDER BY latitude_decimal DESC;

-- Geospatial bounding box for NZ ports
SELECT 
    'NZ Ports Coverage' as description,
    MIN(latitude_decimal) as south_boundary,
    MAX(latitude_decimal) as north_boundary,
    MIN(longitude_decimal) as west_boundary,
    MAX(longitude_decimal) as east_boundary,
    ROUND(ST_DISTANCE(
        TO_GEOGRAPHY('POINT(' || MIN(longitude_decimal) || ' ' || MIN(latitude_decimal) || ')'),
        TO_GEOGRAPHY('POINT(' || MAX(longitude_decimal) || ' ' || MAX(latitude_decimal) || ')')
    ) / 1000, 2) as diagonal_distance_km
FROM tide_ports;

-- =============================================
-- 9. MARITIME SAFETY & INCIDENT ANALYSIS
-- =============================================

-- Maritime incidents overview by severity and region
SELECT 
    incident_severity,
    nz_region,
    COUNT(*) as total_incidents,
    SUM(injured_persons) as total_injuries,
    ROUND(AVG(injured_persons), 2) as avg_injuries_per_incident,
    COUNT(CASE WHEN injured_persons > 0 THEN 1 END) as incidents_with_injuries
FROM maritime_incidents
GROUP BY incident_severity, nz_region
ORDER BY total_incidents DESC;

-- Critical incidents analysis (fatalities and founderings)
SELECT 
    event_date,
    event_location,
    nz_region,
    what_happened,
    vessel_type,
    sector,
    injured_persons,
    brief_description
FROM maritime_incidents
WHERE incident_severity = 'Critical'
ORDER BY event_date DESC
LIMIT 20;

-- Seasonal incident patterns
SELECT 
    event_year,
    event_quarter,
    CASE event_quarter
        WHEN 1 THEN 'Q1 (Summer)'
        WHEN 2 THEN 'Q2 (Autumn)'
        WHEN 3 THEN 'Q3 (Winter)'
        WHEN 4 THEN 'Q4 (Spring)'
    END as season,
    COUNT(*) as total_incidents,
    COUNT(CASE WHEN incident_severity = 'Critical' THEN 1 END) as critical_incidents,
    AVG(injured_persons) as avg_injuries_per_incident
FROM maritime_incidents
WHERE event_year >= 2020
GROUP BY event_year, event_quarter
ORDER BY event_year, event_quarter;

-- Port safety analysis - incidents near major ports
SELECT 
    p.port_name,
    COUNT(m.event_id) as nearby_incidents,
    COUNT(CASE WHEN m.incident_severity = 'Critical' THEN 1 END) as critical_incidents_nearby,
    SUM(m.injured_persons) as total_injuries_nearby,
    ROUND(AVG(ST_DISTANCE(p.location_point, m.location_point) / 1000), 2) as avg_distance_from_port_km
FROM tide_ports p
LEFT JOIN maritime_incidents m ON ST_DWITHIN(p.location_point, m.location_point, 50000) -- 50km radius
WHERE m.location_point IS NOT NULL
GROUP BY p.port_name
ORDER BY nearby_incidents DESC;

-- Vessel age vs incident severity correlation
SELECT 
    CASE 
        WHEN vessel_age_at_incident IS NULL THEN 'Unknown Age'
        WHEN vessel_age_at_incident < 10 THEN '0-9 years'
        WHEN vessel_age_at_incident < 20 THEN '10-19 years'
        WHEN vessel_age_at_incident < 30 THEN '20-29 years'
        ELSE '30+ years'
    END as vessel_age_group,
    incident_severity,
    COUNT(*) as incident_count,
    ROUND(AVG(injured_persons), 2) as avg_injuries
FROM maritime_incidents
WHERE vessel_age_at_incident IS NOT NULL
GROUP BY vessel_age_group, incident_severity
ORDER BY vessel_age_group, incident_severity;

-- High-risk incident types by location
SELECT 
    what_happened,
    where_happened,
    COUNT(*) as frequency,
    COUNT(CASE WHEN injured_persons > 0 THEN 1 END) as incidents_with_injuries,
    SUM(injured_persons) as total_injuries,
    ROUND((COUNT(CASE WHEN injured_persons > 0 THEN 1 END) * 100.0) / COUNT(*), 1) as injury_rate_percent
FROM maritime_incidents
GROUP BY what_happened, where_happened
HAVING COUNT(*) >= 5  -- Only show incident types with 5+ occurrences
ORDER BY injury_rate_percent DESC, total_injuries DESC;

-- Recreational vs Commercial safety comparison
SELECT 
    sector,
    COUNT(*) as total_incidents,
    COUNT(CASE WHEN incident_severity = 'Critical' THEN 1 END) as critical_incidents,
    ROUND((COUNT(CASE WHEN incident_severity = 'Critical' THEN 1 END) * 100.0) / COUNT(*), 2) as critical_rate_percent,
    SUM(injured_persons) as total_injuries,
    ROUND(AVG(injured_persons), 2) as avg_injuries_per_incident,
    COUNT(CASE WHEN injured_persons > 0 THEN 1 END) as incidents_with_injuries
FROM maritime_incidents
GROUP BY sector
ORDER BY critical_rate_percent DESC;

-- Geospatial hotspots - incident density analysis
WITH incident_grid AS (
    SELECT 
        FLOOR(latitude_decimal * 10) / 10 as lat_grid,
        FLOOR(longitude_decimal * 10) / 10 as lng_grid,
        COUNT(*) as incident_count,
        COUNT(CASE WHEN incident_severity IN ('Critical', 'Major') THEN 1 END) as serious_incidents,
        SUM(injured_persons) as total_injuries
    FROM maritime_incidents
    WHERE location_point IS NOT NULL
    GROUP BY lat_grid, lng_grid
    HAVING COUNT(*) >= 5  -- Only grid cells with 5+ incidents
)
SELECT 
    lat_grid,
    lng_grid,
    incident_count,
    serious_incidents,
    total_injuries,
    ROUND((serious_incidents * 100.0) / incident_count, 1) as serious_incident_rate_percent,
    CASE 
        WHEN incident_count >= 50 THEN 'High Risk Hotspot'
        WHEN incident_count >= 20 THEN 'Moderate Risk Area'
        ELSE 'Lower Risk Area'
    END as risk_classification
FROM incident_grid
ORDER BY incident_count DESC;

-- Incident trends over time with maritime safety insights
SELECT 
    event_year,
    COUNT(*) as total_incidents,
    COUNT(CASE WHEN incident_severity = 'Critical' THEN 1 END) as critical_incidents,
    COUNT(CASE WHEN incident_severity = 'Major' THEN 1 END) as major_incidents,
    SUM(injured_persons) as total_injuries,
    ROUND(AVG(injured_persons), 2) as avg_injuries_per_incident,
    COUNT(DISTINCT vessel_type) as unique_vessel_types_involved,
    COUNT(CASE WHEN sector = 'Recreational' THEN 1 END) as recreational_incidents,
    COUNT(CASE WHEN sector LIKE '%Commercial%' THEN 1 END) as commercial_incidents
FROM maritime_incidents
GROUP BY event_year
ORDER BY event_year;

-- =============================================
-- 10. TIDE & INCIDENT CORRELATION ANALYSIS
-- =============================================

-- Incidents during extreme tide conditions
SELECT 
    m.event_date,
    m.event_location,
    m.what_happened,
    m.incident_severity,
    t.tide_height_m,
    t.port_name as nearest_port,
    ROUND(ST_DISTANCE(t.location_point, m.location_point) / 1000, 2) as distance_from_port_km,
    CASE 
        WHEN t.tide_height_m > 3.0 THEN 'High Tide'
        WHEN t.tide_height_m < 0.5 THEN 'Low Tide'
        ELSE 'Normal Tide'
    END as tide_condition
FROM maritime_incidents m
JOIN tide_predictions t ON DATE(m.event_date) = t.date
    AND ST_DWITHIN(m.location_point, t.location_point, 100000) -- 100km radius
WHERE m.location_point IS NOT NULL
    AND (t.tide_height_m > 3.0 OR t.tide_height_m < 0.5)  -- Extreme tides only
ORDER BY m.event_date DESC, distance_from_port_km
LIMIT 20;

-- =============================================
-- 11. EXPORT QUERIES FOR EXTERNAL TOOLS
-- =============================================

-- Export for marine weather integration
SELECT 
    port_code,
    date,
    tide_time,
    tide_height_m,
    tide_sequence,
    CONCAT(EXTRACT(YEAR FROM date), '-', 
           LPAD(EXTRACT(MONTH FROM date), 2, '0'), '-',
           LPAD(EXTRACT(DAY FROM date), 2, '0'), 'T',
           LPAD(EXTRACT(HOUR FROM TRY_TO_TIME(tide_time)), 2, '0'), ':',
           LPAD(EXTRACT(MINUTE FROM TRY_TO_TIME(tide_time)), 2, '0'), ':00Z') as iso_datetime
FROM tide_predictions 
WHERE date BETWEEN '2024-01-01' AND '2024-12-31'
ORDER BY port_code, date, tide_time;

-- =============================================
-- 9. INTELLIGENT DOCUMENT Q&A WITH CORTEX SEARCH  
-- =============================================

-- Prerequisites: 
-- 1. Add fishing regulation PDFs to data/fish-pdf/ folder
-- 2. Run: snow sql --connection admin --filename scripts/setup_cortex_fishing_documents.sql
-- 3. Or execute: ./scripts/run_cortex_setup.sh
-- 
-- This automatically creates a production-ready fishing_regulations_search Cortex Search Service

-- Ask natural language questions about fishing regulations
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'fishing_regulations_search',
        '{
            "query": "What are the daily bag limits for snapper in Auckland?",
            "columns": ["file_name", "chunk_text", "document_section", "nz_region"],
            "limit": 3
        }'
    )
)['results'] as fishing_rules_results;

-- Find size restrictions for specific fish species
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'fishing_regulations_search',
        '{
            "query": "minimum size limits for kahawai and john dory",
            "columns": ["file_name", "chunk_text", "document_section", "nz_region"],
            "filter": {"@eq": {"document_section": "Size Restrictions"}},
            "limit": 5
        }'
    )
)['results'] as size_limit_results;

-- =============================================
-- 🎣 COMPLETE FISHING TRIP PLANNER - RAG + MULTI-DATA INTEGRATION
-- Complex Question: "I want to catch snapper this weekend near Auckland. 
-- What are the regulations, when are the best tide times, and is it safe?"
-- =============================================

WITH user_fishing_query AS (
    -- Define the fishing trip parameters
    SELECT 
        'Auckland' as target_location,
        'snapper' as target_species,
        CURRENT_DATE() as trip_start_date,
        CURRENT_DATE() + 2 as trip_end_date,
        'I want to catch snapper this weekend near Auckland. What are the regulations, when are the best tide times, and is it safe based on recent incidents?' as user_question
),

regulation_search AS (
    -- RAG: Search fishing regulations for snapper in Auckland
    SELECT 
        ufq.user_question,
        ufq.target_location,
        ufq.target_species,
        PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'fishing_regulations_search',
                '{
                    "query": "snapper bag limit minimum size Auckland recreational fishing",
                    "columns": ["file_name", "chunk_text", "document_section", "nz_region"],
                    "filter": {"@eq": {"nz_region": "Auckland"}},
                    "limit": 3
                }'
            )
        )['results'] as regulation_results
    FROM user_fishing_query ufq
),

optimal_tides AS (
    -- Get best tide times for the fishing trip
    SELECT 
        ufq.target_location,
        tp.date,
        tp.tide_time,
        tp.tide_height_m,
        CASE 
            WHEN tp.tide_height_m BETWEEN 1.5 AND 2.5 THEN 'Excellent for Snapper (Mid-tide)'
            WHEN tp.tide_height_m > 3.0 THEN 'Good for Deep Water Fishing'
            WHEN tp.tide_height_m < 1.0 THEN 'Best for Shallow Water Species'
            ELSE 'Moderate Fishing Conditions'
        END as fishing_quality,
        ROW_NUMBER() OVER (PARTITION BY tp.date ORDER BY 
            CASE WHEN tp.tide_height_m BETWEEN 1.5 AND 2.5 THEN 1 ELSE 2 END,
            tp.tide_height_m DESC
        ) as daily_rank
    FROM user_fishing_query ufq
    JOIN tide_predictions tp ON tp.port_name = ufq.target_location
    WHERE tp.date BETWEEN ufq.trip_start_date AND ufq.trip_end_date
    AND tp.tide_height_m > 0.5  -- Filter out extreme low tides
),

safety_assessment AS (
    -- Analyze maritime safety incidents for the last 365 days
    SELECT 
        ufq.target_location,
        COUNT(*) as total_incidents_last_365_days,
        COUNT(CASE WHEN mi.incident_severity = 'Critical' THEN 1 END) as critical_incidents,
        COUNT(CASE WHEN mi.incident_severity = 'Major' THEN 1 END) as major_incidents,
        LISTAGG(DISTINCT mi.what_happened, '; ') as recent_incident_types,
        CASE 
            WHEN COUNT(CASE WHEN mi.incident_severity IN ('Critical', 'Major') THEN 1 END) = 0 THEN 'Low Risk'
            WHEN COUNT(CASE WHEN mi.incident_severity IN ('Critical', 'Major') THEN 1 END) <= 2 THEN 'Moderate Risk'
            ELSE 'High Risk - Extra Caution Required'
        END as safety_rating
    FROM user_fishing_query ufq
    LEFT JOIN maritime_incidents mi ON mi.nz_region = ufq.target_location
    WHERE mi.event_date >= CURRENT_DATE() - 365
    GROUP BY ufq.target_location
),

q1_regulations AS (
    SELECT 
        rs.user_question,
        rs.target_location,
        rs.target_species,
        ARRAY_TO_STRING(
            ARRAY_AGG(r.value:chunk_text::STRING) WITHIN GROUP (ORDER BY r.index),
            '\n\n---REGULATION---\n\n'
        ) as regulations_text
    FROM regulation_search rs,
    LATERAL FLATTEN(input => rs.regulation_results) r
    GROUP BY rs.user_question, rs.target_location, rs.target_species
),

q2_tides AS (
    SELECT 
        q1.*,
        ARRAY_TO_STRING(
            ARRAY_AGG(
                ot.date::STRING || ' at ' || ot.tide_time || ' (' || ot.tide_height_m || 'm) - ' || ot.fishing_quality
            ) WITHIN GROUP (ORDER BY ot.date, ot.daily_rank),
            '\n'
        ) as tides_text
    FROM q1_regulations q1
    CROSS JOIN (SELECT * FROM optimal_tides WHERE daily_rank <= 3) ot
    GROUP BY q1.user_question, q1.target_location, q1.target_species, q1.regulations_text
),

q3_safety AS (
    SELECT 
        q2.*,
        sa.safety_rating,
        sa.total_incidents_last_365_days,
        COALESCE(sa.recent_incident_types, 'No major incidents reported') as safety_details
    FROM q2_tides q2
    CROSS JOIN safety_assessment sa
),

comprehensive_context AS (
    SELECT 
        q3.*,
        'Question: ' || user_question || 
        '\n\n🎣 FISHING REGULATIONS:\n' || regulations_text ||
        '\n\n🌊 OPTIMAL TIDE TIMES (Next 3 Days):\n' || tides_text ||
        '\n\n⚠️ SAFETY ASSESSMENT:\nRisk Level: ' || safety_rating ||
        '\nRecent Incidents (365 days): ' || total_incidents_last_365_days ||
        '\nDetails: ' || safety_details ||
        '\n\nPlease provide:\n1. REGULATIONS SUMMARY: Key rules for ' || target_species || ' in ' ||
         target_location || ' (bag limits, size requirements)'||
        '\n2. OPTIMAL TIMING: Best 3 tide times with specific recommendations'||
        '\n3. SAFETY ADVICE: Current risk assessment and safety precautions'||
        '\n4. TRIP PLAN: Day-by-day fishing strategy combining all factors'||
        '\n5. COMPLIANCE CHECKLIST: What to remember for legal, safe fishing'||
        '\n\nFormat as a practical, actionable fishing guide.' as full_context
    FROM q3_safety q3
)

SELECT 
    'Fishing Trip Planner' as assistant_type,
    target_location,
    target_species,
    user_question,
    -- AI synthesis using string concatenated context
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        full_context
    ) as comprehensive_fishing_plan,
    
    -- Raw data for app developers
    regulations_text as raw_regulations_data,
    tides_text as raw_tide_data,
    safety_rating as raw_safety_rating,
    full_context as debug_prompt
FROM comprehensive_context;

-- =============================================
-- 🚀 HACKATHON INSPIRATION: What You Can Build!
-- =============================================
/*
💡 This single query demonstrates:

✅ RAG-POWERED REGULATIONS: Semantic search through PDF fishing rules
✅ REAL-TIME TIDE DATA: Optimal fishing times with tidal predictions  
✅ SAFETY INTELLIGENCE: Maritime incident analysis for risk assessment
✅ GEOSPATIAL AWARENESS: Location-specific recommendations
✅ AI SYNTHESIS: Complex multi-source data into actionable advice

🎯 Streamlit App Ideas:
- Interactive fishing trip planner with maps
- Species-specific regulation lookup
- Real-time safety alerts for marine areas
- Tide prediction calendar with fishing recommendations
- Compliance checker for your catch
- Social fishing platform with shared trip reports

🔥 Advanced Features You Can Add:
- Weather API integration (MetService)
- Moon phase calculations for optimal feeding
- Fish species AI identification from photos
- Community-driven fishing spot ratings
- Push notifications for optimal fishing windows
- Integration with fishing license validation

The power of Snowflake: Complex questions → Intelligent answers in ONE QUERY! 🎣
*/

-- =============================================
-- 10. NEXT STEPS FOR PARTICIPANTS
-- =============================================

/*
POTENTIAL AI/ML PROJECT IDEAS:

🎣 FEATURED: "IS THIS THE RIGHT TIME TO GO FISHING?" STREAMLIT APP
   =====================================================
   Build an intelligent fishing assistant that combines:
   - Real-time tide predictions (WAITA data)
   - AI-powered fishing regulation Q&A (Cortex Search on PDF docs)
   - Maritime safety incident analysis
   - Weather API integration (MetService)
   - Species-specific recommendations
   - Lunar cycle fishing predictions
   - Interactive maps with port locations (Snowflake GEOGRAPHY)
   - Mobile-responsive Streamlit interface
   
   Key Features:
   * Ask questions like "Can I catch snapper today?" (Cortex Search)
   * Get tide recommendations: "Best tide times for surf casting"
   * Safety alerts: "Any recent incidents near this location?"
   * Compliance checking: "Is my catch within legal limits?"
   * Trip planning: "Plan a 3-day fishing trip to Bay of Islands"

1. 🌊 TIDE PREDICTION MODELS
   - Train ML models to predict tide heights using historical patterns
   - Compare LSTM vs. Traditional harmonic analysis
   - Account for weather impact on tidal predictions

2. 🚢 MARINE NAVIGATION OPTIMIZATION
   - Optimal route planning considering tide windows
   - Port scheduling optimization for commercial vessels
   - Real-time navigation assistance systems

3. 🎣 SMART FISHING APPLICATIONS
   - Predict best fishing times combining tides + weather
   - Fish species migration pattern analysis with tidal cycles
   - Commercial fishing fleet optimization

4. 🏗️ COASTAL INFRASTRUCTURE PLANNING
   - Sea level rise impact modeling on existing infrastructure
   - Optimal timing for coastal construction projects
   - Flood risk assessment for coastal communities

5. 🔗 MULTI-MODAL INTEGRATION
   - Combine with weather data (wind, waves, barometric pressure)
   - Integration with vessel tracking (AIS) data
   - Marine protected area monitoring

6. 📊 REAL-TIME DASHBOARDS
   - Live tide monitoring with alerts
   - Commercial port efficiency dashboards
   - Recreational boating safety applications

ADDITIONAL DATA SOURCES TO CONSIDER:
- MetService marine weather data
- Vessel tracking (AIS) data from Maritime NZ
- Water quality monitoring data
- Coastal webcam imagery
- Wave height and period data
*/