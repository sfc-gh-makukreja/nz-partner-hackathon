#!/bin/bash
# Complete HIWA_I_TE_RANGI (Tourism & Events) Setup Script
# Processes data, creates schema, loads data, and tests queries

set -e

echo "🎯 HIWA_I_TE_RANGI COMPLETE SETUP"
echo "=================================="
echo ""

# 1. Process EventFinda data (already done, but refresh if needed)
echo "📡 1. Refreshing EventFinda RSS data..."
uv run --with requests --with pandas scripts/fetch_eventfinda_data.py

# 2. Process tourism statistics data  
echo ""
echo "📊 2. Processing Stats NZ tourism data..."
uv run --with pandas --with requests scripts/process_tourism_data.py

# 3. Create Snowflake schema and tables
echo ""
echo "❄️ 3. Setting up Snowflake schema..."
snow sql -f scripts/setup_hiwa_i_te_rangi.sql --connection default

# 4. Load EventFinda events data
echo ""
echo "📤 4. Loading EventFinda events data..."
snow sql --connection default -q "
USE DATABASE nz_partner_hackathon;
USE SCHEMA HIWA_I_TE_RANGI;

PUT file://processed_data/eventfinda_events.csv @tourism_data_stage;

COPY INTO eventfinda_events
FROM @tourism_data_stage/eventfinda_events.csv
FILE_FORMAT = tourism_csv_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

SELECT 'EventFinda Events Loaded: ' || COUNT(*) as status FROM eventfinda_events;
"

# 5. Load tourism statistics (start with the simple ones)
echo ""
echo "📤 5. Loading tourism statistics..."
snow sql --connection default -q "
USE DATABASE nz_partner_hackathon;
USE SCHEMA HIWA_I_TE_RANGI;

-- Upload files
PUT file://processed_data/visitor_arrivals.csv @tourism_data_stage;
PUT file://processed_data/passenger_movements.csv @tourism_data_stage;
PUT file://processed_data/migrant_arrivals.csv @tourism_data_stage;

-- Load visitor arrivals
COPY INTO visitor_arrivals
FROM @tourism_data_stage/visitor_arrivals.csv
FILE_FORMAT = tourism_csv_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- Load passenger movements
COPY INTO passenger_movements
FROM @tourism_data_stage/passenger_movements.csv
FILE_FORMAT = tourism_csv_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- Load migrant arrivals
COPY INTO migrant_arrivals
FROM @tourism_data_stage/migrant_arrivals.csv
FILE_FORMAT = tourism_csv_format
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

-- Status report
SELECT 
    'visitor_arrivals' as table_name, 
    COUNT(*) as records, 
    MIN(report_year) as earliest_year, 
    MAX(report_year) as latest_year
FROM visitor_arrivals
UNION ALL
SELECT 
    'passenger_movements' as table_name, 
    COUNT(*) as records, 
    MIN(report_year) as earliest_year, 
    MAX(report_year) as latest_year
FROM passenger_movements
UNION ALL
SELECT 
    'migrant_arrivals' as table_name, 
    COUNT(*) as records, 
    MIN(report_year) as earliest_year, 
    MAX(report_year) as latest_year
FROM migrant_arrivals
UNION ALL
SELECT 
    'eventfinda_events' as table_name, 
    COUNT(*) as records, 
    YEAR(MIN(start_date)) as earliest_year, 
    YEAR(MAX(start_date)) as latest_year
FROM eventfinda_events;
"

# 6. Test basic queries
echo ""
echo "🧪 6. Testing basic queries..."
snow sql --connection default -q "
USE DATABASE nz_partner_hackathon;
USE SCHEMA HIWA_I_TE_RANGI;

-- Test EventFinda events
SELECT 
    'EVENTS BY CATEGORY' as query_type,
    category,
    COUNT(*) as event_count,
    COUNT(DISTINCT region) as regions_covered
FROM eventfinda_events
GROUP BY category
ORDER BY event_count DESC;
"

snow sql --connection default -q "
USE DATABASE nz_partner_hackathon;
USE SCHEMA HIWA_I_TE_RANGI;

-- Test visitor trends
SELECT 
    'VISITOR ARRIVALS TREND' as query_type,
    report_year,
    visitor_arrivals,
    LAG(visitor_arrivals) OVER (ORDER BY report_year) as previous_year,
    visitor_arrivals - LAG(visitor_arrivals) OVER (ORDER BY report_year) as year_over_year_change
FROM visitor_arrivals
WHERE report_year >= 2019
ORDER BY report_year DESC;
"

# 7. Final status
echo ""
echo "✅ HIWA_I_TE_RANGI SETUP COMPLETE!"
echo "=================================="
echo ""
echo "📊 Available Data:"
echo "  • EventFinda Events: Real-time event data from RSS feed"
echo "  • Visitor Arrivals: 100+ years of NZ visitor statistics (1923-2024)"  
echo "  • Passenger Movements: 160+ years of arrival/departure data (1861-2024)"
echo "  • Migrant Arrivals: Recent migration statistics (2003-2025)"
echo ""
echo "🎯 Next Steps:"
echo "  1. Run sample queries: snow sql -f sample_queries/HIWA_I_TE_RANGI_tourism_events_queries.sql"
echo "  2. Build Streamlit apps using the tourism data"
echo "  3. Create AI-powered tourism demand forecasting"
echo "  4. Integrate with other themes (events + weather, events + marine data)"
echo ""
echo "🚀 Ready for hackathon participants!"