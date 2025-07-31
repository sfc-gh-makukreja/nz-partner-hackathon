-- URU RANGI (Wind & Atmosphere) - Sample Queries
-- Theme: Wind, Energy, Climate, and Renewable Power
-- Data Available: Electricity zone demand (5-min intervals), renewable generation, historical trends

USE DATABASE nz_partner_hackathon;
USE SCHEMA URU_RANGI;

-- =============================================
-- 1. EXPLORATORY QUERIES
-- =============================================

-- Overview of available data
SELECT 'Zone data (5-min intervals)' as dataset, COUNT(*) as records FROM electricity_zone_data_5min
UNION ALL
SELECT 'Fuel type data (annual)' as dataset, COUNT(*) as records FROM electricity_generation_by_fuel
UNION ALL
SELECT 'Quarterly trends' as dataset, COUNT(*) as records FROM electricity_quarterly_generation;

-- Data date range and coverage
SELECT 
    MIN(timestamp_nz) as earliest_data,
    MAX(timestamp_nz) as latest_data,
    COUNT(*) as total_intervals,
    COUNT(*) * 5 / 60 as total_hours_covered
FROM electricity_zone_data_5min;

-- =============================================
-- 2. PEAK DEMAND ANALYSIS
-- =============================================

-- Daily peak demand patterns
SELECT 
    EXTRACT(HOUR FROM timestamp_nz) as hour_of_day,
    AVG(nz_total_mw) as avg_demand_mw,
    MAX(nz_total_mw) as peak_demand_mw,
    STDDEV(nz_total_mw) as demand_variability
FROM electricity_zone_data_5min
GROUP BY EXTRACT(HOUR FROM timestamp_nz)
ORDER BY hour_of_day;

-- Weekend vs weekday demand patterns
SELECT 
    CASE WHEN DAYOFWEEK(timestamp_nz) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END as day_type,
    EXTRACT(HOUR FROM timestamp_nz) as hour_of_day,
    AVG(nz_total_mw) as avg_demand_mw,
    COUNT(*) as sample_count
FROM electricity_zone_data_5min
GROUP BY day_type, hour_of_day
ORDER BY day_type, hour_of_day;

-- Top 10 peak demand moments
SELECT 
    timestamp_nz,
    nz_total_mw,
    auckland_gz2_mw,
    wellington_gz8_mw,
    christchurch_gz10_mw,
    DAYNAME(timestamp_nz) as day_of_week
FROM electricity_zone_data_5min
ORDER BY nz_total_mw DESC
LIMIT 10;

-- =============================================
-- 3. REGIONAL ANALYSIS
-- =============================================

-- Regional demand comparison (major cities)
SELECT 
    'Auckland' as region,
    AVG(auckland_gz2_mw) as avg_demand_mw,
    MAX(auckland_gz2_mw) as peak_demand_mw,
    MIN(auckland_gz2_mw) as min_demand_mw
FROM electricity_zone_data_5min
UNION ALL
SELECT 
    'Wellington' as region,
    AVG(wellington_gz8_mw),
    MAX(wellington_gz8_mw),
    MIN(wellington_gz8_mw)
FROM electricity_zone_data_5min
UNION ALL
SELECT 
    'Christchurch' as region,
    AVG(christchurch_gz10_mw),
    MAX(christchurch_gz10_mw),
    MIN(christchurch_gz10_mw)
FROM electricity_zone_data_5min
ORDER BY avg_demand_mw DESC;

-- North Island vs South Island demand split
SELECT 
    DATE(timestamp_nz) as date_nz,
    AVG(ni_total_mw) as avg_north_island_mw,
    AVG(si_total_mw) as avg_south_island_mw,
    AVG(ni_total_mw) / AVG(nz_total_mw) * 100 as north_island_percentage
FROM electricity_zone_data_5min
GROUP BY DATE(timestamp_nz)
ORDER BY date_nz;

-- Regional demand correlation analysis
SELECT 
    CORR(auckland_gz2_mw, wellington_gz8_mw) as auckland_wellington_correlation,
    CORR(auckland_gz2_mw, christchurch_gz10_mw) as auckland_christchurch_correlation,
    CORR(wellington_gz8_mw, christchurch_gz10_mw) as wellington_christchurch_correlation
FROM electricity_zone_data_5min;

-- =============================================
-- 4. WIND ENERGY POTENTIAL ANALYSIS
-- =============================================

-- Identify wind-favorable regions by demand variability
-- (Higher variability may indicate wind generation impact)
SELECT 
    'Taranaki' as region,
    AVG(taranaki_gz6_mw) as avg_demand,
    STDDEV(taranaki_gz6_mw) as demand_variability,
    STDDEV(taranaki_gz6_mw) / AVG(taranaki_gz6_mw) * 100 as variability_coefficient
FROM electricity_zone_data_5min
UNION ALL
SELECT 
    'Wellington',
    AVG(wellington_gz8_mw),
    STDDEV(wellington_gz8_mw),
    STDDEV(wellington_gz8_mw) / AVG(wellington_gz8_mw) * 100
FROM electricity_zone_data_5min
UNION ALL
SELECT 
    'West Coast',
    AVG(west_coast_gz12_mw),
    STDDEV(west_coast_gz12_mw),
    STDDEV(west_coast_gz12_mw) / AVG(west_coast_gz12_mw) * 100
FROM electricity_zone_data_5min
UNION ALL
SELECT 
    'Canterbury',
    AVG(canterbury_gz11_mw),
    STDDEV(canterbury_gz11_mw),
    STDDEV(canterbury_gz11_mw) / AVG(canterbury_gz11_mw) * 100
FROM electricity_zone_data_5min
ORDER BY variability_coefficient DESC;

-- Hourly wind generation patterns (using demand drops as proxy)
SELECT 
    EXTRACT(HOUR FROM timestamp_nz) as hour,
    AVG(CASE WHEN LAG(nz_total_mw) OVER (ORDER BY timestamp_nz) - nz_total_mw > 100 
             THEN 1 ELSE 0 END) as wind_surge_probability
FROM electricity_zone_data_5min
GROUP BY EXTRACT(HOUR FROM timestamp_nz)
ORDER BY hour;

-- =============================================
-- 5. RENEWABLE ENERGY TRENDS
-- =============================================

-- Historical renewable percentage growth
SELECT 
    calendar_year,
    renewable_percentage,
    fossil_fuel_percentage,
    renewable_percentage - LAG(renewable_percentage) OVER (ORDER BY calendar_year) as yearly_improvement
FROM electricity_generation_by_fuel
WHERE calendar_year >= 2015
ORDER BY calendar_year;

-- Renewable energy breakdown by source
SELECT 
    calendar_year,
    hydro_gwh,
    geothermal_gwh,
    wind_gwh,
    solar_pv_gwh,
    wind_gwh / total_generation_gwh * 100 as wind_percentage
FROM electricity_generation_by_fuel
WHERE calendar_year >= 2020
ORDER BY calendar_year;

-- =============================================
-- 6. AI/ML FEATURE ENGINEERING QUERIES
-- =============================================

-- Create features for demand prediction model
SELECT 
    timestamp_nz,
    nz_total_mw as target_demand,
    -- Time features
    EXTRACT(HOUR FROM timestamp_nz) as hour,
    EXTRACT(DAYOFWEEK FROM timestamp_nz) as day_of_week,
    EXTRACT(MONTH FROM timestamp_nz) as month,
    -- Lag features (previous intervals)
    LAG(nz_total_mw, 1) OVER (ORDER BY timestamp_nz) as demand_lag_5min,
    LAG(nz_total_mw, 12) OVER (ORDER BY timestamp_nz) as demand_lag_1hr,
    LAG(nz_total_mw, 288) OVER (ORDER BY timestamp_nz) as demand_lag_24hr,
    -- Rolling averages
    AVG(nz_total_mw) OVER (ORDER BY timestamp_nz ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as demand_avg_1hr,
    -- Regional features
    ni_total_mw,
    si_total_mw,
    auckland_gz2_mw / nz_total_mw * 100 as auckland_demand_share
FROM electricity_zone_data_5min
ORDER BY timestamp_nz;

-- Anomaly detection features
SELECT 
    timestamp_nz,
    nz_total_mw,
    -- Z-score for anomaly detection
    (nz_total_mw - AVG(nz_total_mw) OVER ()) / STDDEV(nz_total_mw) OVER () as demand_zscore,
    -- Hour-specific z-score
    (nz_total_mw - AVG(nz_total_mw) OVER (PARTITION BY EXTRACT(HOUR FROM timestamp_nz))) / 
    STDDEV(nz_total_mw) OVER (PARTITION BY EXTRACT(HOUR FROM timestamp_nz)) as hourly_zscore,
    -- Regional imbalance indicators
    ABS(ni_total_mw - AVG(ni_total_mw) OVER ()) / STDDEV(ni_total_mw) OVER () as ni_anomaly_score
FROM electricity_zone_data_5min
ORDER BY ABS((nz_total_mw - AVG(nz_total_mw) OVER ()) / STDDEV(nz_total_mw) OVER ()) DESC
LIMIT 20;

-- =============================================
-- 7. SNOWFLAKE CORTEX AI EXAMPLES
-- =============================================

-- Generate insights using Cortex AI
SELECT 
    SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        CONCAT('Analyze this electricity demand pattern and suggest energy optimization strategies: ',
               'Peak demand: ', MAX(nz_total_mw)::STRING, ' MW, ',
               'Average demand: ', AVG(nz_total_mw)::STRING, ' MW, ',
               'North Island share: ', AVG(ni_total_mw)/AVG(nz_total_mw)*100::STRING, '%')
    ) as ai_insights
FROM electricity_zone_data_5min;

-- Classify demand periods using Cortex
SELECT 
    timestamp_nz,
    nz_total_mw,
    SNOWFLAKE.CORTEX.CLASSIFY(
        CASE 
            WHEN nz_total_mw > 6000 THEN 'High Demand'
            WHEN nz_total_mw > 4500 THEN 'Medium Demand'
            ELSE 'Low Demand'
        END,
        CONCAT('Time: ', timestamp_nz::STRING, ', Demand: ', nz_total_mw::STRING, ' MW')
    ) as demand_classification
FROM electricity_zone_data_5min
LIMIT 10;

-- =============================================
-- 8. BUSINESS INSIGHTS QUERIES
-- =============================================

-- Grid stability analysis
SELECT 
    DATE(timestamp_nz) as date_nz,
    MAX(nz_total_mw) - MIN(nz_total_mw) as daily_demand_swing,
    STDDEV(nz_total_mw) as daily_volatility,
    COUNT(CASE WHEN ABS(nz_total_mw - LAG(nz_total_mw) OVER (ORDER BY timestamp_nz)) > 200 
               THEN 1 END) as sudden_changes
FROM electricity_zone_data_5min
GROUP BY DATE(timestamp_nz)
ORDER BY daily_volatility DESC;

-- Economic load forecasting
SELECT 
    EXTRACT(HOUR FROM timestamp_nz) as hour,
    EXTRACT(DAYOFWEEK FROM timestamp_nz) as day_of_week,
    AVG(nz_total_mw) as expected_demand,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY nz_total_mw) as peak_capacity_needed
FROM electricity_zone_data_5min
GROUP BY hour, day_of_week
ORDER BY day_of_week, hour;

-- Infrastructure utilization
SELECT 
    'National Grid' as infrastructure,
    AVG(nz_total_mw) as avg_utilization_mw,
    MAX(nz_total_mw) as peak_utilization_mw,
    MAX(nz_total_mw) / 12000 * 100 as estimated_capacity_usage -- Assuming 12GW total capacity
FROM electricity_zone_data_5min;

-- =============================================
-- NEXT STEPS FOR PARTICIPANTS:
-- =============================================

/*
AI/ML PROJECT IDEAS:

1. DEMAND FORECASTING
   - Predict next-hour electricity demand using time series
   - Use weather data correlation for better accuracy
   - Implement real-time anomaly detection

2. RENEWABLE OPTIMIZATION
   - Identify optimal wind farm locations using demand patterns
   - Predict wind generation potential by region
   - Plan grid storage requirements

3. GRID MANAGEMENT
   - Detect equipment failures through demand anomalies
   - Optimize load balancing between North/South Islands
   - Predict maintenance windows

4. ENERGY TRADING
   - Price prediction models using demand patterns
   - Regional arbitrage opportunities
   - Peak demand charge optimization

5. SUSTAINABILITY REPORTING
   - Carbon footprint calculation by region
   - Renewable energy adoption tracking
   - Environmental impact visualization

CORTEX AI INTEGRATION:
- Use COMPLETE() for natural language insights
- Use CLASSIFY() for demand pattern categorization
- Use SIMILARITY() for pattern matching across regions
*/