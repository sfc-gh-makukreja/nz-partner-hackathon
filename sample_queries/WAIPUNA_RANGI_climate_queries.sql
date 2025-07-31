-- WAIPUNA RANGI (Rain & Water) - Sample Queries
-- Theme: Rain, Water, Climate Patterns, and Weather Analysis  
-- Data Available: NIWA climate station rainfall data (103 annual records, 1933-2022)

USE DATABASE nz_partner_hackathon;
USE SCHEMA WAIPUNA_RANGI;

-- =============================================
-- 1. EXPLORATORY QUERIES
-- =============================================

-- Overview of available climate data
SELECT 'Annual rainfall data' as dataset, COUNT(*) as records FROM rainfall_annual
UNION ALL
SELECT 'Monthly rainfall data' as dataset, COUNT(*) as records FROM rainfall_monthly
UNION ALL  
SELECT 'Annual temperature data' as dataset, COUNT(*) as records FROM temperature_annual
UNION ALL
SELECT 'Monthly temperature data' as dataset, COUNT(*) as records FROM temperature_monthly;

-- Station coverage and data range
SELECT 
    station_id,
    station_name,
    MIN(year) as data_start,
    MAX(year) as data_end,
    COUNT(*) as years_of_data,
    AVG(total_rainfall_mm) as avg_annual_rainfall_mm,
    MIN(total_rainfall_mm) as driest_year_mm,
    MAX(total_rainfall_mm) as wettest_year_mm
FROM rainfall_annual
GROUP BY station_id, station_name
ORDER BY avg_annual_rainfall_mm DESC;

-- =============================================
-- 2. RAINFALL TREND ANALYSIS
-- =============================================

-- Long-term rainfall trends by station
SELECT 
    station_id,
    station_name,
    year,
    total_rainfall_mm,
    rain_days_count,
    -- 5-year moving average
    AVG(total_rainfall_mm) OVER (
        PARTITION BY station_id 
        ORDER BY year 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    ) as rainfall_5yr_avg,
    -- Year-over-year change
    total_rainfall_mm - LAG(total_rainfall_mm) OVER (
        PARTITION BY station_id ORDER BY year
    ) as rainfall_yoy_change_mm,
    -- Rainfall intensity (mm per rain day)
    CASE WHEN rain_days_count > 0 
         THEN total_rainfall_mm / rain_days_count 
         ELSE NULL END as rainfall_intensity_mm_per_day
FROM rainfall_annual
ORDER BY station_id, year;

-- Wettest and driest years by station
SELECT 
    station_id,
    station_name,
    'Wettest' as record_type,
    year,
    total_rainfall_mm,
    rain_days_count
FROM rainfall_annual 
WHERE (station_id, total_rainfall_mm) IN (
    SELECT station_id, MAX(total_rainfall_mm) 
    FROM rainfall_annual 
    GROUP BY station_id
)
UNION ALL
SELECT 
    station_id,
    station_name,
    'Driest' as record_type,
    year,
    total_rainfall_mm,
    rain_days_count
FROM rainfall_annual 
WHERE (station_id, total_rainfall_mm) IN (
    SELECT station_id, MIN(total_rainfall_mm) 
    FROM rainfall_annual 
    GROUP BY station_id
)
ORDER BY station_id, record_type;

-- =============================================
-- 3. DROUGHT AND FLOOD ANALYSIS
-- =============================================

-- Identify drought years (bottom 10% rainfall for each station)
SELECT 
    station_id,
    station_name,
    year,
    total_rainfall_mm,
    rain_days_count,
    ROUND(
        (total_rainfall_mm - AVG(total_rainfall_mm) OVER (PARTITION BY station_id)) / 
        STDDEV(total_rainfall_mm) OVER (PARTITION BY station_id), 2
    ) as rainfall_z_score,
    CASE 
        WHEN total_rainfall_mm <= PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY total_rainfall_mm) 
             OVER (PARTITION BY station_id) THEN 'Severe Drought'
        WHEN total_rainfall_mm <= PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_rainfall_mm) 
             OVER (PARTITION BY station_id) THEN 'Moderate Drought'
        WHEN total_rainfall_mm >= PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_rainfall_mm) 
             OVER (PARTITION BY station_id) THEN 'Very Wet'
        WHEN total_rainfall_mm >= PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_rainfall_mm) 
             OVER (PARTITION BY station_id) THEN 'Wet'
        ELSE 'Normal'
    END as rainfall_category
FROM rainfall_annual
ORDER BY station_id, year;

-- Consecutive dry years analysis
WITH drought_years AS (
    SELECT 
        station_id,
        station_name,
        year,
        total_rainfall_mm,
        CASE WHEN total_rainfall_mm <= PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_rainfall_mm) 
                  OVER (PARTITION BY station_id) THEN 1 ELSE 0 END as is_drought_year
    FROM rainfall_annual
),
drought_sequences AS (
    SELECT *,
        SUM(CASE WHEN is_drought_year = 0 THEN 1 ELSE 0 END) 
        OVER (PARTITION BY station_id ORDER BY year) as drought_group
    FROM drought_years
    WHERE is_drought_year = 1
)
SELECT 
    station_id,
    station_name,
    MIN(year) as drought_start,
    MAX(year) as drought_end,
    COUNT(*) as consecutive_drought_years,
    AVG(total_rainfall_mm) as avg_drought_rainfall
FROM drought_sequences
GROUP BY station_id, station_name, drought_group
HAVING COUNT(*) >= 2
ORDER BY consecutive_drought_years DESC, station_id;

-- =============================================
-- 4. CLIMATE VARIABILITY ANALYSIS
-- =============================================

-- Rainfall variability by decade
SELECT 
    station_id,
    station_name,
    FLOOR(year / 10) * 10 as decade,
    COUNT(*) as years_in_decade,
    AVG(total_rainfall_mm) as avg_rainfall,
    STDDEV(total_rainfall_mm) as rainfall_variability,
    MIN(total_rainfall_mm) as min_rainfall,
    MAX(total_rainfall_mm) as max_rainfall,
    MAX(total_rainfall_mm) - MIN(total_rainfall_mm) as rainfall_range
FROM rainfall_annual
GROUP BY station_id, station_name, FLOOR(year / 10) * 10
HAVING COUNT(*) >= 5  -- Only decades with 5+ years of data
ORDER BY station_id, decade;

-- Rainfall efficiency analysis (mm per rain day)
SELECT 
    station_id,
    station_name,
    year,
    total_rainfall_mm,
    rain_days_count,
    CASE WHEN rain_days_count > 0 
         THEN ROUND(total_rainfall_mm / rain_days_count, 2) 
         ELSE NULL END as mm_per_rain_day,
    -- Categorize rainfall patterns
    CASE 
        WHEN rain_days_count > 200 AND total_rainfall_mm / rain_days_count < 8 
        THEN 'Frequent Light Rain'
        WHEN rain_days_count < 150 AND total_rainfall_mm / rain_days_count > 12 
        THEN 'Infrequent Heavy Rain'
        WHEN rain_days_count BETWEEN 150 AND 200 
        THEN 'Moderate Pattern'
        ELSE 'Variable Pattern'
    END as rainfall_pattern
FROM rainfall_annual
WHERE rain_days_count > 0
ORDER BY station_id, year;

-- =============================================
-- 5. REGIONAL COMPARISON
-- =============================================

-- Compare rainfall between stations by time period
WITH station_comparison AS (
    SELECT 
        year,
        SUM(CASE WHEN station_id = 1464 THEN total_rainfall_mm END) as station_1464_mm,
        SUM(CASE WHEN station_id = 2109 THEN total_rainfall_mm END) as station_2109_mm,
        SUM(CASE WHEN station_id = 4960 THEN total_rainfall_mm END) as station_4960_mm
    FROM rainfall_annual
    GROUP BY year
    HAVING COUNT(DISTINCT station_id) >= 2
)
SELECT 
    year,
    station_1464_mm,
    station_2109_mm,
    station_4960_mm,
    -- Calculate regional differences
    ABS(station_1464_mm - station_2109_mm) as diff_1464_2109,
    CASE 
        WHEN station_1464_mm > station_2109_mm THEN 'Station 1464 Wetter'
        WHEN station_2109_mm > station_1464_mm THEN 'Station 2109 Wetter'
        ELSE 'Similar'
    END as regional_pattern
FROM station_comparison
WHERE station_1464_mm IS NOT NULL AND station_2109_mm IS NOT NULL
ORDER BY year;

-- =============================================
-- 6. AI/ML FEATURE ENGINEERING QUERIES
-- =============================================

-- Create features for rainfall prediction model
SELECT 
    station_id,
    year,
    total_rainfall_mm as target_rainfall,
    -- Lag features (previous years)
    LAG(total_rainfall_mm, 1) OVER (PARTITION BY station_id ORDER BY year) as rainfall_lag_1yr,
    LAG(total_rainfall_mm, 2) OVER (PARTITION BY station_id ORDER BY year) as rainfall_lag_2yr,
    LAG(total_rainfall_mm, 3) OVER (PARTITION BY station_id ORDER BY year) as rainfall_lag_3yr,
    -- Rolling averages
    AVG(total_rainfall_mm) OVER (
        PARTITION BY station_id ORDER BY year 
        ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
    ) as rainfall_3yr_avg,
    AVG(total_rainfall_mm) OVER (
        PARTITION BY station_id ORDER BY year 
        ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
    ) as rainfall_5yr_avg,
    -- Trend indicators
    CASE 
        WHEN year - MIN(year) OVER (PARTITION BY station_id) < 10 THEN 'Early Period'
        WHEN year - MIN(year) OVER (PARTITION BY station_id) >= 40 THEN 'Recent Period'
        ELSE 'Middle Period'
    END as data_period,
    -- Cyclical features (basic)
    CASE 
        WHEN year % 7 IN (0, 1) THEN 'El Niño Pattern'
        WHEN year % 7 IN (3, 4) THEN 'La Niña Pattern'
        ELSE 'Neutral Pattern'
    END as potential_climate_cycle
FROM rainfall_annual
ORDER BY station_id, year;

-- Anomaly detection features
SELECT 
    station_id,
    station_name,
    year,
    total_rainfall_mm,
    rain_days_count,
    -- Z-score for anomaly detection
    (total_rainfall_mm - AVG(total_rainfall_mm) OVER (PARTITION BY station_id)) / 
    STDDEV(total_rainfall_mm) OVER (PARTITION BY station_id) as rainfall_zscore,
    -- Rolling standard deviation
    STDDEV(total_rainfall_mm) OVER (
        PARTITION BY station_id ORDER BY year 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) as rolling_5yr_std,
    -- Change point detection
    ABS(total_rainfall_mm - AVG(total_rainfall_mm) OVER (
        PARTITION BY station_id ORDER BY year 
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    )) > 1.5 * STDDEV(total_rainfall_mm) OVER (
        PARTITION BY station_id ORDER BY year 
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) as potential_change_point
FROM rainfall_annual
ORDER BY ABS((total_rainfall_mm - AVG(total_rainfall_mm) OVER (PARTITION BY station_id)) / 
             STDDEV(total_rainfall_mm) OVER (PARTITION BY station_id)) DESC
LIMIT 20;

-- =============================================
-- 7. BUSINESS INSIGHTS QUERIES  
-- =============================================

-- Water resource planning indicators
SELECT 
    station_id,
    station_name,
    FLOOR(year / 10) * 10 as decade,
    AVG(total_rainfall_mm) as avg_decade_rainfall,
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY total_rainfall_mm) as p10_drought_level,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_rainfall_mm) as p90_flood_level,
    -- Infrastructure planning metrics
    ROUND(AVG(total_rainfall_mm) * 0.7, 0) as conservative_planning_mm,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_rainfall_mm), 0) as flood_protection_mm,
    -- Agricultural season length estimate
    AVG(rain_days_count) as avg_rain_days,
    CASE 
        WHEN AVG(rain_days_count) > 180 THEN 'Long Growing Season'
        WHEN AVG(rain_days_count) > 120 THEN 'Medium Growing Season'
        ELSE 'Short Growing Season'
    END as growing_season_estimate
FROM rainfall_annual
GROUP BY station_id, station_name, FLOOR(year / 10) * 10
HAVING COUNT(*) >= 5
ORDER BY station_id, decade;

-- Risk assessment matrix
SELECT 
    station_id,
    station_name,
    COUNT(*) as total_years,
    -- Drought risk
    ROUND(100.0 * SUM(CASE WHEN total_rainfall_mm <= 
        PERCENTILE_CONT(0.2) WITHIN GROUP (ORDER BY total_rainfall_mm) OVER (PARTITION BY station_id) 
        THEN 1 ELSE 0 END) / COUNT(*), 1) as drought_risk_percent,
    -- Flood risk  
    ROUND(100.0 * SUM(CASE WHEN total_rainfall_mm >= 
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY total_rainfall_mm) OVER (PARTITION BY station_id) 
        THEN 1 ELSE 0 END) / COUNT(*), 1) as flood_risk_percent,
    -- Climate stability
    ROUND(STDDEV(total_rainfall_mm) / AVG(total_rainfall_mm) * 100, 1) as variability_coefficient,
    CASE 
        WHEN STDDEV(total_rainfall_mm) / AVG(total_rainfall_mm) < 0.25 THEN 'Low Variability'
        WHEN STDDEV(total_rainfall_mm) / AVG(total_rainfall_mm) < 0.35 THEN 'Moderate Variability'
        ELSE 'High Variability'
    END as climate_stability
FROM rainfall_annual
GROUP BY station_id, station_name
ORDER BY drought_risk_percent DESC;

-- =============================================
-- NEXT STEPS FOR PARTICIPANTS:
-- =============================================

/*
AI/ML PROJECT IDEAS:

1. RAINFALL PREDICTION
   - Predict next year's rainfall using historical patterns
   - Use lag features and climate cycle indicators
   - Implement drought early warning system

2. WATER RESOURCE OPTIMIZATION
   - Reservoir capacity planning using rainfall variability
   - Irrigation scheduling based on rainfall patterns
   - Flood management system design

3. CLIMATE CHANGE ANALYSIS
   - Detect long-term rainfall trend changes
   - Analyze rainfall pattern shifts over decades
   - Project future water availability scenarios

4. AGRICULTURE PLANNING
   - Crop selection based on rainfall patterns
   - Growing season optimization
   - Risk assessment for different farming regions

5. INFRASTRUCTURE PLANNING
   - Storm water system capacity planning
   - Bridge and road design flood levels
   - Urban planning for climate resilience

CORTEX AI INTEGRATION:
- Use COMPLETE() for climate report generation
- Use CLASSIFY() for drought/flood categorization
- Use SIMILARITY() for pattern matching across years
*/