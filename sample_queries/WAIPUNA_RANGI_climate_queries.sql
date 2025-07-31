-- WAIPUNA RANGI (Rain & Water) - Complete Water Risk Intelligence Platform
-- Theme: Comprehensive water risk analysis combining climate, flood, and financial data
-- Data Sources:
-- • NIWA Climate Data (rainfall/temperature, 1933-2022)
-- • Waikato Flood Zones (13 zones, current mapping)  
-- • ICNZ Disaster Costs (141 events, 1968-2025, $1,955M water-related)

USE DATABASE nz_partner_hackathon;
USE SCHEMA WAIPUNA_RANGI;

-- =============================================
-- 1. EXPLORATORY QUERIES
-- =============================================

-- Overview of all WAIPUNA_RANGI datasets
SELECT 'Annual rainfall data' as dataset, COUNT(*) as records FROM rainfall_annual
UNION ALL
SELECT 'Monthly rainfall data' as dataset, COUNT(*) as records FROM rainfall_monthly
UNION ALL  
SELECT 'Annual temperature data' as dataset, COUNT(*) as records FROM temperature_annual
UNION ALL
SELECT 'Monthly temperature data' as dataset, COUNT(*) as records FROM temperature_monthly
UNION ALL
SELECT 'Flood zones' as dataset, COUNT(*) as records FROM waipa_flood_zones
UNION ALL
SELECT 'Flood boundaries' as dataset, COUNT(*) as records FROM waipa_flood_boundaries
UNION ALL
SELECT 'Disaster costs (all)' as dataset, COUNT(*) as records FROM icnz_disaster_costs
UNION ALL
SELECT 'Disaster costs (water-related)' as dataset, COUNT(*) as records FROM icnz_disaster_costs WHERE is_water_related = TRUE;

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
-- 5. FLOOD RISK ANALYSIS (WAIKATO REGIONAL HAZARDS PORTAL)
-- =============================================

-- Flood zone overview and risk classification
SELECT 
    reference,
    feature,
    COUNT(*) as zone_count,
    ROUND(SUM(shape_area_sqm) / 1000000, 2) as total_area_km2,
    ROUND(AVG(shape_area_sqm), 2) as avg_zone_size_sqm,
    ROUND(SUM(shape_length_m) / 1000, 2) as total_perimeter_km
FROM waipa_flood_zones
GROUP BY reference, feature
ORDER BY total_area_km2 DESC;

-- Major flood areas by watercourse
SELECT 
    CASE 
        WHEN comments LIKE '%WAIPA RIVER%' THEN 'Waipa River'
        WHEN comments LIKE '%PUNIU RIVER%' THEN 'Puniu River'
        WHEN comments LIKE '%MANGAPIKO%' THEN 'Mangapiko Stream'
        WHEN comments LIKE '%FLOOD DETENTION%' THEN 'Flood Control Infrastructure'
        WHEN comments LIKE '%SECONDARY FLOW%' THEN 'Secondary Flow Paths'
        ELSE 'General Flood Hazard Areas'
    END as watercourse_type,
    COUNT(*) as zone_count,
    ROUND(SUM(shape_area_sqm) / 1000000, 3) as total_flood_area_km2,
    ROUND(AVG(shape_area_sqm), 0) as avg_zone_area_sqm
FROM waipa_flood_zones
GROUP BY watercourse_type
ORDER BY total_flood_area_km2 DESC;

-- Flood zone complexity analysis (polygon detail)
SELECT 
    f.reference,
    f.comments,
    b.geometry_type,
    b.coordinate_count,
    f.shape_area_sqm,
    CASE 
        WHEN b.coordinate_count > 0 THEN ROUND(f.shape_area_sqm / b.coordinate_count, 2)
        ELSE NULL 
    END as area_per_coordinate,
    CASE 
        WHEN b.coordinate_count > 100 THEN 'Highly Complex'
        WHEN b.coordinate_count > 50 THEN 'Complex'
        WHEN b.coordinate_count > 20 THEN 'Moderate'
        WHEN b.coordinate_count = 0 THEN 'No Coordinates'
        ELSE 'Simple'
    END as complexity_level
FROM waipa_flood_zones f
JOIN waipa_flood_boundaries b ON f.fid = b.fid
ORDER BY b.coordinate_count DESC;

-- =============================================
-- 6. DISASTER COST ANALYSIS (ICNZ DATA)
-- =============================================

-- Water-related disaster trends over time
SELECT 
    event_year,
    COUNT(*) as total_events,
    SUM(CASE WHEN is_water_related THEN 1 ELSE 0 END) as water_related_events,
    ROUND(SUM(inflation_adjusted_cost_millions_nzd), 1) as total_cost_millions_nzd,
    ROUND(SUM(CASE WHEN is_water_related THEN inflation_adjusted_cost_millions_nzd ELSE 0 END), 1) as water_cost_millions_nzd,
    ROUND(100.0 * SUM(CASE WHEN is_water_related THEN inflation_adjusted_cost_millions_nzd ELSE 0 END) / SUM(inflation_adjusted_cost_millions_nzd), 1) as water_cost_percentage
FROM icnz_disaster_costs
WHERE event_year >= 2000  -- Focus on recent decades
GROUP BY event_year
ORDER BY event_year DESC;

-- Major water-related disasters (top 20 costliest)
SELECT 
    event_date,
    event_year,
    event,
    primary_category,
    ROUND(inflation_adjusted_cost_millions_nzd, 1) as cost_millions_nzd,
    CASE 
        WHEN inflation_adjusted_cost_millions_nzd > 1000 THEN 'Catastrophic (>$1B)'
        WHEN inflation_adjusted_cost_millions_nzd > 100 THEN 'Major ($100M-$1B)'
        WHEN inflation_adjusted_cost_millions_nzd > 10 THEN 'Significant ($10M-$100M)'
        ELSE 'Minor (<$10M)'
    END as disaster_magnitude
FROM icnz_disaster_costs
WHERE is_water_related = TRUE
ORDER BY inflation_adjusted_cost_millions_nzd DESC
LIMIT 20;

-- Disaster frequency and cost by decade
SELECT 
    FLOOR(event_year / 10) * 10 as decade,
    COUNT(*) as total_disasters,
    SUM(CASE WHEN is_water_related THEN 1 ELSE 0 END) as water_disasters,
    ROUND(AVG(inflation_adjusted_cost_millions_nzd), 1) as avg_cost_per_event,
    ROUND(SUM(inflation_adjusted_cost_millions_nzd), 1) as decade_total_cost,
    ROUND(SUM(CASE WHEN is_water_related THEN inflation_adjusted_cost_millions_nzd ELSE 0 END), 1) as water_disaster_cost
FROM icnz_disaster_costs
WHERE event_year >= 1970
GROUP BY decade
ORDER BY decade;

-- Seasonal disaster patterns (water-related only)
SELECT 
    EXTRACT(MONTH FROM event_date) as event_month,
    MONTHNAME(event_date) as month_name,
    COUNT(*) as disaster_count,
    ROUND(AVG(inflation_adjusted_cost_millions_nzd), 1) as avg_cost_millions,
    ROUND(SUM(inflation_adjusted_cost_millions_nzd), 1) as total_cost_millions,
    CASE 
        WHEN EXTRACT(MONTH FROM event_date) IN (12, 1, 2) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM event_date) IN (3, 4, 5) THEN 'Autumn'  
        WHEN EXTRACT(MONTH FROM event_date) IN (6, 7, 8) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM event_date) IN (9, 10, 11) THEN 'Spring'
    END as season
FROM icnz_disaster_costs
WHERE is_water_related = TRUE 
    AND event_date IS NOT NULL
GROUP BY event_month, month_name, season
ORDER BY event_month;

-- =============================================
-- 7. INTEGRATED WATER RISK ANALYSIS
-- =============================================

-- Climate-disaster correlation analysis
SELECT 
    r.year,
    r.station_name,
    r.total_rainfall_mm,
    CASE 
        WHEN r.total_rainfall_mm > 1200 THEN 'High Rainfall Year'
        WHEN r.total_rainfall_mm > 800 THEN 'Normal Rainfall Year'  
        ELSE 'Low Rainfall Year'
    END as rainfall_category,
    d.disaster_count,
    d.total_disaster_cost
FROM rainfall_annual r
LEFT JOIN (
    SELECT 
        event_year,
        COUNT(*) as disaster_count,
        SUM(inflation_adjusted_cost_millions_nzd) as total_disaster_cost
    FROM icnz_disaster_costs
    WHERE is_water_related = TRUE
    GROUP BY event_year
) d ON r.year = d.event_year
WHERE r.year >= 2000  -- Focus on years with both datasets
ORDER BY r.year DESC;

-- Water risk summary by data source
SELECT 
    'Climate Patterns' as risk_factor,
    'Historical rainfall shows high variability with drought/flood cycles' as risk_description,
    COUNT(DISTINCT year) as years_analyzed,
    NULL as financial_impact_millions
FROM rainfall_annual
WHERE total_rainfall_mm < 600 OR total_rainfall_mm > 1400  -- Extreme years

UNION ALL

SELECT 
    'Flood Zones' as risk_factor,
    'Physical flood risk mapped across ' || COUNT(*) || ' zones covering ' || 
    ROUND(SUM(shape_area_sqm)/1000000, 1) || ' km²' as risk_description,
    COUNT(*) as areas_at_risk,
    NULL as financial_impact_millions
FROM waipa_flood_zones

UNION ALL

SELECT 
    'Historical Disasters' as risk_factor,
    'Water-related disasters cost $' || ROUND(SUM(inflation_adjusted_cost_millions_nzd), 0) || 
    'M over ' || COUNT(*) || ' events since 1968' as risk_description,
    COUNT(*) as disaster_events,
    ROUND(SUM(inflation_adjusted_cost_millions_nzd), 1) as financial_impact_millions
FROM icnz_disaster_costs
WHERE is_water_related = TRUE;

-- =============================================
-- 8. ADVANCED ANALYTICS & AI OPPORTUNITIES
-- =============================================

-- Extreme event correlation with costs
WITH extreme_rainfall_years AS (
    SELECT DISTINCT year
    FROM monthly_climate_summary 
    WHERE rainfall_category = 'Wet'
    GROUP BY year
    HAVING COUNT(*) >= 3  -- 3+ wet months = extreme rainfall year
),
disaster_costs_by_year AS (
    SELECT 
        event_year,
        COUNT(*) as events,
        SUM(inflation_adjusted_cost_millions_nzd) as total_cost
    FROM icnz_disaster_costs 
    WHERE is_water_related = TRUE
    GROUP BY event_year
)
SELECT 
    e.year,
    'Extreme Rainfall Year' as event_type,
    COALESCE(d.events, 0) as disasters_that_year,
    COALESCE(d.total_cost, 0) as disaster_cost_millions
FROM extreme_rainfall_years e
LEFT JOIN disaster_costs_by_year d ON e.year = d.event_year
ORDER BY e.year DESC;

-- AI-Powered Risk Assessment Examples (using available Cortex AISQL functions)

-- Example 1: AI_CLASSIFY disaster events by severity
SELECT 
    event,
    inflation_adjusted_cost_millions_nzd,
    AI_CLASSIFY(event, ['Minor Incident', 'Moderate Event', 'Major Disaster', 'Catastrophic Event']) as severity_classification
FROM icnz_disaster_costs 
WHERE is_water_related = TRUE
ORDER BY inflation_adjusted_cost_millions_nzd DESC
LIMIT 10;

-- Example 2: AI_FILTER to identify extreme weather events  
SELECT 
    station_name,
    year,
    total_rainfall_mm,
    AI_FILTER(
        'Annual rainfall of ' || total_rainfall_mm || 'mm in ' || year, 
        'Is this an extreme rainfall year that could cause flooding?'
    ) as extreme_weather_flag
FROM rainfall_annual 
WHERE year >= 2020
ORDER BY total_rainfall_mm DESC;

-- Example 3: AI_AGG to summarize flood risks by region
SELECT 
    AI_AGG(
        comments, 
        'Summarize the main flood risks and affected waterways in this region'
    ) as flood_risk_summary
FROM waipa_flood_zones;

-- Example 4: AI_SENTIMENT analysis of disaster event descriptions
SELECT 
    event,
    event_year,
    primary_category,
    AI_SENTIMENT(event) as event_impact_sentiment,
    inflation_adjusted_cost_millions_nzd
FROM icnz_disaster_costs 
WHERE is_water_related = TRUE 
    AND event_year >= 2020
ORDER BY inflation_adjusted_cost_millions_nzd DESC
LIMIT 5;

-- Example 5: AI_COMPLETE for automated flood risk reports
SELECT 
    AI_COMPLETE(
        'llama3.1-70b',
        'Create a flood risk assessment for: ' || comments || 
        '. Area size: ' || ROUND(shape_area_sqm/1000000, 2) || ' km². ' ||
        'Provide risk level and recommended actions.'
    ) as automated_risk_report
FROM waipa_flood_zones 
WHERE comments LIKE '%RIVER%'
LIMIT 3;

-- =============================================
-- 9. NEXT STEPS FOR PARTICIPANTS:
-- =============================================

/*
COMPREHENSIVE WATER RISK AI/ML PROJECT IDEAS:

🌊 FLOOD PREDICTION & EARLY WARNING
   - Combine rainfall patterns + flood zone mapping + historical disaster costs
   - Predict flood likelihood and estimated financial impact  
   - Real-time early warning system using weather data
   - Use AI_COMPLETE('llama3.1-70b', prompt) for automated flood risk reports

💰 DISASTER COST MODELING  
   - Predict insurance costs based on weather patterns and flood exposure
   - Correlate extreme rainfall events with historical disaster costs
   - Risk-based pricing models for flood insurance
   - Use AI_CLASSIFY(event, ['Low Risk', 'Medium Risk', 'High Risk', 'Extreme Risk']) 

🗺️ SPATIAL RISK ASSESSMENT
   - Combine flood zone boundaries with rainfall station data
   - Create risk heat maps overlaying climate + flood + cost data
   - Optimize flood zone updates using recent climate trends
   - Use AI_SIMILARITY(area1_description, area2_description) to find comparable risk areas

📊 INTEGRATED WATER INTELLIGENCE PLATFORM
   - Real-time dashboard combining all 3 data sources
   - Climate-flood-cost correlation analysis engine
   - Automated risk scoring for any location
   - Use AI_AGG(risk_factors, 'Summarize overall water risk') for comprehensive analysis

🌿 CLIMATE ADAPTATION PLANNING
   - Infrastructure investment prioritization using integrated risk data
   - Climate resilience planning for communities
   - Water resource management optimization
   - Long-term urban planning scenarios

🚨 EMERGENCY RESPONSE OPTIMIZATION
   - Resource allocation during extreme weather events
   - Evacuation planning based on flood zones + weather forecasts
   - Post-disaster cost estimation and recovery planning
   - Use CLASSIFY() for emergency response priority levels

DATA SOURCES AVAILABLE:
✅ NIWA Climate Data: 277 annual + 4,245 monthly rainfall records (1933-2022)
✅ Waikato Flood Zones: 13 zones with precise polygon boundaries  
✅ ICNZ Disaster Costs: 141 events, 97 water-related ($1,955M total impact)

CORTEX AI INTEGRATION OPPORTUNITIES (Verified Available in Asia Pacific):

🎯 AVAILABLE MODELS: llama3.1-8b, llama3.1-70b, mistral-large2, mixtral-8x7b, mistral-7b
📍 REGIONS: AWS AP Southeast 2 (Sydney), AWS AP Northeast 1 (Tokyo)

- AI_COMPLETE('llama3.1-70b', prompt) → Automated risk reports, natural language explanations
- AI_CLASSIFY(text, ['Low Risk', 'Medium Risk', 'High Risk']) → Risk level categorization  
- AI_FILTER(text_column, 'Is this water-related?') → Filter water-related events
- AI_SIMILARITY(text1, text2) → Pattern matching across years, comparable areas
- AI_AGG(text_column, 'Summarize key flood risks') → Aggregate insights across events  
- AI_SENTIMENT(event_description) → Sentiment analysis of disaster reports
- EXTRACT_ANSWER(text, question) → Extract specific information from documents
- TRANSLATE(text, source_lang, target_lang) → Multi-language disaster reporting

SAMPLE HACKATHON CHALLENGES:
1. "Predict the next major flood event and its potential cost"
2. "Create an AI-powered flood insurance pricing model"
3. "Build a climate resilience planning tool for local councils"
4. "Design an early warning system combining all data sources"
*/