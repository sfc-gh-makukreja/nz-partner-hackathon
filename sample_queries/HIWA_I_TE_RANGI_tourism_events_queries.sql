-- =============================================
-- HIWA_I_TE_RANGI (Travel & Tourism) - Sample Queries
-- Theme: Events, Tourism Statistics, and Travel Analysis
-- =============================================

-- Table of Contents:
-- 1. Basic Event Data Exploration
-- 2. Event Tourism Impact Analysis  
-- 3. Regional Event Distribution
-- 4. Seasonal Event Patterns
-- 5. AI-Powered Tourism Insights
-- 6. Cross-Theme Integration Examples
-- 7. POTENTIAL PROJECT IDEAS

USE ROLE ACCOUNTADMIN;
USE DATABASE nz_partner_hackathon;
USE SCHEMA HIWA_I_TE_RANGI;

-- =============================================
-- 1. BASIC EVENT DATA EXPLORATION
-- =============================================

-- Overview of EventFinda data
SELECT 
    'EventFinda Events' as dataset,
    COUNT(*) as total_events,
    COUNT(DISTINCT region) as unique_regions,
    COUNT(DISTINCT category) as unique_categories,
    MIN(start_date) as earliest_event,
    MAX(start_date) as latest_event,
    COUNT(CASE WHEN is_recurring THEN 1 END) as recurring_events,
    COUNT(CASE WHEN NOT is_recurring THEN 1 END) as one_time_events
FROM eventfinda_events;

-- Event categories breakdown
SELECT 
    category,
    COUNT(*) as event_count,
    COUNT(DISTINCT region) as regions_covered,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage_of_total,
    -- Sample events in each category
    LISTAGG(DISTINCT LEFT(title, 30), ' | ') WITHIN GROUP (ORDER BY LEFT(title, 30)) as sample_titles
FROM eventfinda_events
GROUP BY category
ORDER BY event_count DESC;

-- Regional event distribution
SELECT 
    region,
    COUNT(*) as total_events,
    COUNT(DISTINCT category) as unique_categories,
    COUNT(CASE WHEN is_recurring THEN 1 END) as recurring_events,
    -- Top categories by region
    LISTAGG(DISTINCT category, ', ') WITHIN GROUP (ORDER BY category) as categories_offered,
    -- Date range of events
    MIN(start_date) as first_event,
    MAX(start_date) as last_event
FROM eventfinda_events
WHERE region != 'Other'
GROUP BY region
ORDER BY total_events DESC;

-- =============================================
-- 2. EVENT TOURISM IMPACT ANALYSIS
-- =============================================

-- Events that could drive tourism (multi-day, unique categories)
SELECT 
    title,
    region,
    category,
    start_date,
    end_date,
    DATEDIFF('day', start_date, end_date) + 1 as event_duration_days,
    is_recurring,
    event_url,
    CASE 
        WHEN DATEDIFF('day', start_date, end_date) >= 2 THEN 'Multi-day Event'
        WHEN category IN ('Music & Performance', 'Arts & Culture', 'Comedy & Entertainment') THEN 'Cultural Attraction'
        WHEN is_recurring THEN 'Regular Attraction'
        ELSE 'Single-day Event'
    END as tourism_potential
FROM eventfinda_events
WHERE start_date >= CURRENT_DATE()
ORDER BY 
    CASE 
        WHEN DATEDIFF('day', start_date, end_date) >= 2 THEN 1
        WHEN category IN ('Music & Performance', 'Arts & Culture') THEN 2
        ELSE 3
    END,
    start_date;

-- Regional event density (events per month by region)
WITH monthly_events AS (
    SELECT 
        region,
        DATE_TRUNC('month', start_date) as event_month,
        COUNT(*) as events_in_month,
        COUNT(DISTINCT category) as categories_in_month
    FROM eventfinda_events
    WHERE region != 'Other' AND start_date IS NOT NULL
    GROUP BY region, event_month
)
SELECT 
    region,
    COUNT(DISTINCT event_month) as months_with_events,
    AVG(events_in_month) as avg_events_per_month,
    MAX(events_in_month) as peak_events_in_month,
    AVG(categories_in_month) as avg_categories_per_month,
    STDDEV(events_in_month) as event_volatility
FROM monthly_events
GROUP BY region
ORDER BY avg_events_per_month DESC;

-- =============================================
-- 3. SEASONAL EVENT PATTERNS
-- =============================================

-- Events by day of week and time of year
SELECT 
    DAYNAME(start_date) as day_of_week,
    MONTH(start_date) as month_number,
    MONTHNAME(start_date) as month_name,
    COUNT(*) as event_count,
    COUNT(DISTINCT region) as regions_active,
    -- Most common categories by day/month
    MODE(category) as most_common_category
FROM eventfinda_events
WHERE start_date IS NOT NULL
GROUP BY DAYNAME(start_date), MONTH(start_date), MONTHNAME(start_date)
ORDER BY month_number, 
    CASE DAYNAME(start_date)
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2 
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END;

-- Weekend vs weekday event patterns
SELECT 
    CASE 
        WHEN DAYOFWEEK(start_date) IN (1, 7) THEN 'Weekend'  -- Sunday=1, Saturday=7
        ELSE 'Weekday'
    END as day_type,
    category,
    COUNT(*) as event_count,
    AVG(DATEDIFF('day', start_date, end_date) + 1) as avg_duration_days,
    COUNT(CASE WHEN is_recurring THEN 1 END) as recurring_events
FROM eventfinda_events
WHERE start_date IS NOT NULL
GROUP BY day_type, category
ORDER BY day_type, event_count DESC;

-- =============================================
-- 4. AI-POWERED TOURISM INSIGHTS
-- =============================================

-- AI-generated event summaries and tourism recommendations
SELECT 
    region,
    COUNT(*) as total_events,
    LISTAGG(DISTINCT category, ', ') as event_categories,
    MIN(start_date) as next_event_date,
    -- AI-powered regional tourism summary
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT(
            'Tourism Analysis for ', region, ': ',
            'Events: ', COUNT(*), ' events across categories: ', 
            LISTAGG(DISTINCT category, ', '),
            '. Next event: ', MIN(start_date),
            '. Provide tourism recommendations and visitor appeal analysis.'
        )
    ) as ai_tourism_analysis
FROM eventfinda_events
WHERE region != 'Other' AND start_date >= CURRENT_DATE()
GROUP BY region
ORDER BY total_events DESC;

-- AI event categorization and tourism appeal scoring
WITH event_analysis AS (
    SELECT 
        event_id,
        title,
        description,
        region,
        category,
        start_date,
        DATEDIFF('day', start_date, end_date) + 1 as duration_days,
        -- AI-powered tourism appeal assessment
        SNOWFLAKE.CORTEX.COMPLETE(
            'llama3.1-8b',
            CONCAT(
                'Event: "', title, '" in ', region, 
                '. Category: ', category,
                '. Description: ', LEFT(description, 200),
                '. Rate tourism appeal (1-10) and explain why visitors would travel for this event.'
            )
        ) as ai_appeal_analysis
    FROM eventfinda_events
    WHERE start_date >= CURRENT_DATE()
    LIMIT 10  -- Limit for demo purposes
)
SELECT 
    title,
    region,
    category,
    duration_days,
    ai_appeal_analysis,
    -- Extract numeric score from AI response
    TRY_CAST(
        REGEXP_SUBSTR(ai_appeal_analysis, '[0-9]+', 1, 1)
        AS INTEGER
    ) as ai_tourism_score
FROM event_analysis
ORDER BY ai_tourism_score DESC;

-- =============================================
-- 5. CROSS-THEME INTEGRATION EXAMPLES
-- =============================================

-- Example: Events + Weather (WAIPUNA_RANGI) - Outdoor event risk analysis
SELECT 
    e.title,
    e.region,
    e.start_date,
    e.category,
    -- This would join with climate data if regions matched
    CASE e.region
        WHEN 'Auckland' THEN 'Consider Auckland climate patterns for outdoor events'
        WHEN 'Wellington' THEN 'Wellington wind patterns may affect outdoor events'
        WHEN 'Canterbury' THEN 'Canterbury weather variability for event planning'
        ELSE 'General NZ climate considerations'
    END as climate_considerations,
    -- AI recommendation combining events and weather
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT(
            'Event planning for "', e.title, '" in ', e.region, 
            ' on ', e.start_date, '. Category: ', e.category,
            '. Provide weather risk assessment and backup planning recommendations for New Zealand climate.'
        )
    ) as ai_weather_planning
FROM eventfinda_events e
WHERE e.category IN ('Sports & Recreation', 'Arts & Culture')
    AND e.start_date >= CURRENT_DATE()
ORDER BY e.start_date
LIMIT 5;

-- Example: Events + Marine Data (WAITA) - Coastal events and tide planning  
SELECT 
    e.title,
    e.region,
    e.start_date,
    e.category,
    CASE 
        WHEN e.region IN ('Auckland', 'Wellington', 'Canterbury') THEN 'Major port city - consider tide schedules'
        WHEN e.region = 'Taranaki' THEN 'Coastal region - tide awareness important'
        ELSE 'Inland region - marine factors less relevant'
    END as marine_considerations,
    -- AI recommendation for coastal event planning
    CASE 
        WHEN e.region IN ('Auckland', 'Wellington', 'Canterbury', 'Taranaki') THEN
            SNOWFLAKE.CORTEX.COMPLETE(
                'llama3.1-8b',
                CONCAT(
                    'Coastal event planning for "', e.title, '" in ', e.region,
                    '. Consider tide times, marine weather, and ferry schedules. Provide logistics recommendations.'
                )
            )
        ELSE 'N/A - Inland event'
    END as ai_marine_planning
FROM eventfinda_events e
WHERE e.start_date >= CURRENT_DATE()
ORDER BY 
    CASE WHEN e.region IN ('Auckland', 'Wellington', 'Canterbury', 'Taranaki') THEN 1 ELSE 2 END,
    e.start_date
LIMIT 8;

-- =============================================
-- 6. ADVANCED ANALYTICS & FORECASTING
-- =============================================

-- Event clustering analysis - similar events and patterns
WITH event_features AS (
    SELECT 
        event_id,
        title,
        region,
        category,
        MONTH(start_date) as event_month,
        DATEDIFF('day', start_date, end_date) + 1 as duration,
        is_recurring,
        -- Text features for similarity
        LENGTH(description) as desc_length,
        CASE WHEN title ILIKE '%festival%' THEN 1 ELSE 0 END as is_festival,
        CASE WHEN title ILIKE '%workshop%' OR title ILIKE '%class%' THEN 1 ELSE 0 END as is_educational,
        CASE WHEN title ILIKE '%free%' THEN 1 ELSE 0 END as is_free
    FROM eventfinda_events
    WHERE start_date IS NOT NULL
)
SELECT 
    category,
    region,
    COUNT(*) as similar_events,
    AVG(duration) as avg_duration,
    AVG(desc_length) as avg_description_length,
    SUM(is_festival) as festival_count,
    SUM(is_educational) as educational_count,
    SUM(is_free) as free_events_count,
    -- Seasonality pattern
    LISTAGG(DISTINCT event_month, ',') as months_active
FROM event_features
GROUP BY category, region
HAVING COUNT(*) > 1  -- Only show categories/regions with multiple events
ORDER BY similar_events DESC;

-- Tourism demand forecasting template (ready for tourism stats data)
WITH forecast_base AS (
    SELECT 
        DATE_TRUNC('month', start_date) as event_month,
        region,
        COUNT(*) as monthly_events,
        COUNT(DISTINCT category) as category_diversity,
        -- Event impact score based on type and duration
        SUM(
            CASE category
                WHEN 'Music & Performance' THEN 3
                WHEN 'Arts & Culture' THEN 2.5
                WHEN 'Sports & Recreation' THEN 2
                WHEN 'Comedy & Entertainment' THEN 2
                ELSE 1
            END * 
            (DATEDIFF('day', start_date, end_date) + 1)
        ) as event_impact_score
    FROM eventfinda_events
    WHERE start_date IS NOT NULL AND region != 'Other'
    GROUP BY event_month, region
)
SELECT 
    region,
    event_month,
    monthly_events,
    category_diversity,
    event_impact_score,
    -- Tourism demand prediction template
    CASE 
        WHEN event_impact_score > 20 THEN 'High tourism demand expected'
        WHEN event_impact_score > 10 THEN 'Moderate tourism demand expected'
        WHEN event_impact_score > 5 THEN 'Low-moderate tourism demand expected'
        ELSE 'Minimal event-driven tourism expected'
    END as predicted_tourism_impact,
    -- AI-powered demand forecast
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT(
            'Tourism forecast for ', region, ' in ', event_month, ': ',
            monthly_events, ' events with impact score ', event_impact_score,
            '. Predict accommodation demand, visitor numbers, and business impact.'
        )
    ) as ai_demand_forecast
FROM forecast_base
ORDER BY event_month, event_impact_score DESC;

-- =============================================
-- 7. DATA QUALITY & MONITORING
-- =============================================

-- Event data quality monitoring
SELECT 
    'Data Quality Report' as report_type,
    COUNT(*) as total_events,
    COUNT(CASE WHEN title IS NULL OR title = '' THEN 1 END) as missing_titles,
    COUNT(CASE WHEN start_date IS NULL THEN 1 END) as missing_dates,
    COUNT(CASE WHEN region IS NULL OR region = 'Other' THEN 1 END) as missing_regions,
    COUNT(CASE WHEN category = 'Other' THEN 1 END) as uncategorized_events,
    ROUND(COUNT(CASE WHEN title IS NOT NULL AND start_date IS NOT NULL AND region != 'Other' 
                     THEN 1 END) * 100.0 / COUNT(*), 2) as data_completeness_percent,
    MAX(fetch_timestamp) as last_updated
FROM eventfinda_events;

-- =============================================
-- 8. ADVANCED: EVENT-TOURISM INTELLIGENCE
-- Combines EventFinda events with tourism data + AI insights
-- Complexity: Advanced
-- =============================================

WITH current_event_landscape AS (
    -- Analyze current event activity by region
    SELECT 
        region,
        COUNT(*) as total_events,
        COUNT(DISTINCT category) as event_diversity,
        LISTAGG(DISTINCT category, ' + ') as event_mix,
        COUNT(CASE WHEN is_recurring THEN 1 END) as recurring_events,
        AVG(DATEDIFF('day', CURRENT_DATE(), start_date)) as avg_days_until_event,
        COUNT(CASE WHEN start_date BETWEEN CURRENT_DATE() AND CURRENT_DATE() + 30 THEN 1 END) as events_next_30_days
    FROM eventfinda_events
    WHERE region != 'Other' AND start_date >= CURRENT_DATE()
    GROUP BY region
),
tourism_recovery_context AS (
    -- Get tourism recovery metrics
    SELECT 
        MAX(CASE WHEN report_year = 2019 THEN visitor_arrivals END) as pre_covid_peak,
        MAX(CASE WHEN report_year = 2024 THEN visitor_arrivals END) as current_visitors,
        ROUND(
            MAX(CASE WHEN report_year = 2024 THEN visitor_arrivals END) * 100.0 / 
            MAX(CASE WHEN report_year = 2019 THEN visitor_arrivals END), 1
        ) as tourism_recovery_rate
    FROM visitor_arrivals
    WHERE report_year IN (2019, 2024)
),
regional_tourism_intelligence AS (
    SELECT 
        cel.region,
        cel.total_events,
        cel.event_diversity,
        cel.event_mix,
        cel.events_next_30_days,
        trc.tourism_recovery_rate,
        trc.current_visitors as national_visitors_2024,
        
        -- Calculate regional event density score
        ROUND(cel.total_events * cel.event_diversity * 
              CASE WHEN cel.events_next_30_days > 0 THEN 1.5 ELSE 1.0 END, 2) as event_tourism_score,
              
        -- AI-powered tourism strategy analysis
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            CONCAT(
                'REGIONAL TOURISM INTELLIGENCE REPORT\n\n',
                'Region: ', cel.region, '\n',
                'Event Profile:\n',
                '• Total Events: ', cel.total_events, '\n',
                '• Event Categories: ', cel.event_mix, '\n',
                '• Upcoming Events (30 days): ', cel.events_next_30_days, '\n\n',
                'NZ Tourism Context (2024):\n',
                '• National Tourism Recovery: ', trc.tourism_recovery_rate, '% of pre-COVID levels\n',
                '• Annual Visitors: ', trc.current_visitors, '\n\n',
                'ANALYSIS REQUEST: Provide strategic tourism insights:\n',
                '1. How can this region leverage its event profile to capture more of the ', trc.current_visitors, ' annual visitors?\n',
                '2. What specific tourism opportunities does the event mix suggest?\n',
                '3. Given NZ''s ', trc.tourism_recovery_rate, '% recovery rate, what marketing strategies would be most effective?\n',
                '4. Rate this region''s tourism potential (1-10) based on event diversity and upcoming activity.\n\n',
                'Provide actionable recommendations:'
            )
        ) as ai_tourism_strategy
        
    FROM current_event_landscape cel
    CROSS JOIN tourism_recovery_context trc
)
SELECT 
    region,
    total_events,
    event_mix,
    events_next_30_days,
    tourism_recovery_rate || '% tourism recovery' as recovery_status,
    event_tourism_score,
    ai_tourism_strategy
FROM regional_tourism_intelligence
ORDER BY event_tourism_score DESC, total_events DESC;

-- Cross-theme integration example: Events + Tourism demand prediction
WITH event_tourism_correlation AS (
    SELECT 
        e.region,
        e.category,
        e.title,
        e.start_date,
        
        -- Tourism demand prediction based on event type and timing
        CASE 
            WHEN e.category IN ('Sports & Recreation', 'Music & Performance') 
                 AND MONTH(e.start_date) IN (12, 1, 2) 
                THEN 'HIGH - Summer outdoor events attract peak tourism'
            WHEN e.category = 'Arts & Culture' 
                 AND MONTH(e.start_date) IN (3, 4, 5, 9, 10, 11)
                THEN 'MODERATE - Cultural events in shoulder seasons'
            WHEN e.category IN ('Comedy & Entertainment', 'Education & Workshops')
                THEN 'STEADY - Indoor events less weather dependent'
            ELSE 'VARIABLE - Depends on specific event and timing'
        END as seasonal_tourism_impact,
        
        -- AI-powered event marketing analysis
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-7b',
            CONCAT(
                'Event: "', e.title, '" (', e.category, ') in ', e.region, ' on ', e.start_date, 
                '. Write a compelling 40-word tourism marketing hook that would attract visitors to this event and region.'
            )
        ) as ai_marketing_hook
    FROM eventfinda_events e
    WHERE e.region != 'Other' 
      AND e.start_date BETWEEN CURRENT_DATE() AND CURRENT_DATE() + 60
)
SELECT 
    region,
    category,
    title,
    start_date,
    seasonal_tourism_impact,
    ai_marketing_hook
FROM event_tourism_correlation
ORDER BY start_date;

-- =============================================
-- 9. EVENT-AIRFARE ANALYSIS & TOURISM VALUE SCORING
-- Combines EventFinda events with airfare data to assess regional tourism value
-- Complexity: Advanced
-- =============================================

WITH event_regions AS (
    SELECT 
        region,
        category,
        COUNT(*) as event_count,
        LISTAGG(DISTINCT title, ' | ') WITHIN GROUP (ORDER BY title) as sample_events
    FROM eventfinda_events 
    WHERE region != 'Other'
    GROUP BY region, category
),
regional_flights AS (
    SELECT 
        CASE arr_airport
            WHEN 'AKL' THEN 'Auckland'
            WHEN 'WLG' THEN 'Wellington' 
            WHEN 'CHC' THEN 'Canterbury'
            WHEN 'DUD' THEN 'Otago'
            WHEN 'NPL' THEN 'Taranaki'
            ELSE arr_airport
        END as region,
        COUNT(*) as total_flights,
        AVG(airfare_nz) as avg_airfare,
        MIN(airfare_nz) as min_airfare
    FROM nz_airfares
    WHERE arr_airport IN ('AKL', 'WLG', 'CHC', 'DUD', 'NPL')
    GROUP BY arr_airport
)
SELECT 
    er.region,
    er.category,
    er.event_count,
    rf.total_flights,
    ROUND(rf.avg_airfare, 2) as avg_airfare_nzd,
    ROUND(rf.min_airfare, 2) as budget_airfare_nzd,

    ROUND((er.event_count * 1000.0) / rf.avg_airfare, 2) as tourism_value_score,

    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-7b',
        CONCAT(
            'Tourism Analysis: ', er.region, ' has ', er.event_count, ' ', er.category, ' events. ',
            'Average airfare: $', rf.avg_airfare, ' (budget: $', rf.min_airfare, '). ',
            'Value score: ', ROUND((er.event_count * 1000.0) / rf.avg_airfare, 2), '. ',
            'Provide a 40-word tourism recommendation highlighting the value proposition.'
        )
    ) as ai_tourism_recommendation
FROM event_regions er
JOIN regional_flights rf ON er.region = rf.region
ORDER BY tourism_value_score DESC
LIMIT 8;

/*
=============================================================================
POTENTIAL AI/ML PROJECT IDEAS FOR HIWA_I_TE_RANGI (TRAVEL & TOURISM):
=============================================================================

🎯 FEATURED: EVENT-DRIVEN TOURISM DEMAND PREDICTOR
   =============================================
   Build an AI system that predicts tourism demand spikes based on event data:
   
   Key Features:
   * Real-time event monitoring from EventFinda RSS
   * Cross-reference with accommodation availability (Stats NZ data)
   * AI-powered demand forecasting using event impact scores
   * Dynamic pricing recommendations for accommodation providers
   * Tourist flow optimization between regions
   
   Technical Approach:
   * Cortex ML for time series forecasting
   * Event categorization with tourism appeal scoring
   * Integration with accommodation occupancy data
   * Streamlit dashboard for real-time insights

📊 TOURISM INTELLIGENCE DASHBOARD IDEAS:
   - Regional Event Calendar with Tourism Impact Scoring
   - "Smart Tourism Route" planner based on event schedules
   - Event Conflict Detection (oversaturation warnings)
   - Economic Impact Calculator (events → accommodation → local business)

🤖 AI/ML OPPORTUNITIES:
   - Event Classification: Automatic tourism appeal scoring
   - Demand Forecasting: Visitor numbers based on event types
   - Recommendation Engine: Best events for different tourist profiles
   - Sentiment Analysis: Event reviews and tourism satisfaction
   - Anomaly Detection: Unusual tourism patterns or event impacts

🔗 INTEGRATION POSSIBILITIES:
   - External APIs: Weather forecasts for outdoor events
   - Social Media: Event buzz and social sentiment tracking
   - Transport APIs: Ferry, bus, flight integration for event access
   - Real-time Data: Traffic, parking, accommodation availability

💡 INNOVATION CHALLENGES:
   - Multi-modal AI: Combine text, images, location data for event analysis
   - Carbon Footprint Tourism: Sustainable event travel recommendations
   - Cultural Tourism AI: Match international visitors with cultural events
   - Accessibility Tourism: Event accessibility assessment and recommendations

🚀 STREAMLIT APP CONCEPTS:
   1. "EventNZ Tourism Planner" - AI-powered event discovery and trip planning
   2. "Regional Tourism Command Center" - Real-time dashboard for tourism operators
   3. "Smart Event Calendar" - Conflict detection and optimization for event organizers
   4. "Tourism Impact Analyzer" - Economic impact modeling for events
   5. "Visit New Zealand AI" - Personalized tourism recommendations

🎪 SPECIFIC USE CASES:
   - Festival Season Planning: Predict accommodation needs for major festivals
   - Business Event Optimization: Best regions/dates for corporate events
   - Tourist Route Optimization: Multi-city event tours
   - Local Business Intelligence: Event impact on restaurants, retail, transport
   - International Tourism: Match visitor interests with NZ event calendar

=============================================================================
NEXT STEPS FOR IMPLEMENTATION:
=============================================================================

1. Load Stats NZ tourism statistics data to enable full analysis
2. Set up automated RSS feed updates for real-time event monitoring  
3. Create Cortex Search service for intelligent event discovery
4. Build Streamlit prototypes using these query patterns
5. Integrate with other themes (WAITA for coastal events, WAIPUNA_RANGI for weather)

*/