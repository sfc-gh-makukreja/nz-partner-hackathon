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

-- Check available ports and data coverage
SELECT 
    p.port_name,
    p.port_code,
    p.latitude,
    p.longitude,
    COUNT(*) as total_predictions,
    MIN(t.date) as earliest_date,
    MAX(t.date) as latest_date,
    COUNT(DISTINCT t.date) as days_covered
FROM tide_ports p
LEFT JOIN tide_predictions t ON p.port_code = t.port_code
GROUP BY p.port_name, p.port_code, p.latitude, p.longitude
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
        LAG(tide_time) OVER (PARTITION BY port_code, date ORDER BY tide_time), 
        tide_time) as minutes_since_last_tide
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
    DATEADD('hour', -2, TIME(tide_time)) as fishing_window_start,
    DATEADD('hour', 2, TIME(tide_time)) as fishing_window_end,
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
    WHERE tide_type = 'High'
    GROUP BY port_code
)
SELECT 
    t.port_code,
    t.prediction_date,
    t.tide_time,
    t.tide_height_m,
    tp.p95_height,
    ROUND(((t.tide_height_m - tp.p95_height) / tp.p95_height) * 100, 1) as pct_above_p95,
    'King Tide Event' as event_type
FROM tide_predictions t
JOIN tide_percentiles tp ON t.port_code = tp.port_code
WHERE t.tide_type = 'High' 
  AND t.tide_height_m > tp.p95_height
ORDER BY t.port_code, t.prediction_date;

-- Low tide exposure analysis (for coastal infrastructure)
SELECT 
    port_code,
    prediction_date,
    tide_time,
    tide_height_m,
    CASE 
        WHEN tide_height_m < 0.5 THEN 'Extreme Low Exposure'
        WHEN tide_height_m < 1.0 THEN 'High Exposure'
        WHEN tide_height_m < 1.5 THEN 'Moderate Exposure'
        ELSE 'Low Exposure'
    END as exposure_level,
    LAG(tide_height_m) OVER (PARTITION BY port_code ORDER BY prediction_date, tide_time) as prev_height,
    LEAD(tide_height_m) OVER (PARTITION BY port_code ORDER BY prediction_date, tide_time) as next_height
FROM tide_predictions 
WHERE tide_type = 'Low'
ORDER BY port_code, prediction_date, tide_time;

-- =============================================
-- 5. COMPARATIVE PORT ANALYSIS
-- =============================================

-- Port comparison - Average tide characteristics
SELECT 
    port_code,
    COUNT(*) as total_observations,
    ROUND(AVG(CASE WHEN tide_type = 'High' THEN tide_height_m END), 2) as avg_high_tide_m,
    ROUND(AVG(CASE WHEN tide_type = 'Low' THEN tide_height_m END), 2) as avg_low_tide_m,
    ROUND(AVG(CASE WHEN tide_type = 'High' THEN tide_height_m END) - 
          AVG(CASE WHEN tide_type = 'Low' THEN tide_height_m END), 2) as avg_tidal_range_m,
    ROUND(STDDEV(tide_height_m), 2) as tide_variability
FROM tide_predictions 
GROUP BY port_code
ORDER BY avg_tidal_range_m DESC;

-- Seasonal tide comparison across ports
SELECT 
    port_code,
    CASE 
        WHEN EXTRACT(MONTH FROM prediction_date) IN (12, 1, 2) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM prediction_date) IN (3, 4, 5) THEN 'Autumn'
        WHEN EXTRACT(MONTH FROM prediction_date) IN (6, 7, 8) THEN 'Winter'
        ELSE 'Spring'
    END as season,
    ROUND(AVG(tide_height_m), 2) as avg_tide_height_m,
    ROUND(MAX(tide_height_m), 2) as max_tide_height_m,
    ROUND(MIN(tide_height_m), 2) as min_tide_height_m,
    COUNT(*) as observations
FROM tide_predictions 
GROUP BY port_code, 
    CASE 
        WHEN EXTRACT(MONTH FROM prediction_date) IN (12, 1, 2) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM prediction_date) IN (3, 4, 5) THEN 'Autumn'
        WHEN EXTRACT(MONTH FROM prediction_date) IN (6, 7, 8) THEN 'Winter'
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
-- 8. EXPORT QUERIES FOR EXTERNAL TOOLS
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
-- 9. NEXT STEPS FOR PARTICIPANTS
-- =============================================

/*
POTENTIAL AI/ML PROJECT IDEAS:

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