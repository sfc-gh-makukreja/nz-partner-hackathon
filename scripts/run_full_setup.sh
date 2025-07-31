#!/bin/bash
# Complete setup script for NZ Partner Hackathon - Electricity Data
# This script processes data and loads it into Snowflake

set -e

echo "🚀 Starting NZ Partner Hackathon - Electricity Data Setup"
echo "========================================================"

# Step 1: Process raw data files
echo "📊 Step 1: Processing raw data files..."
uv run --with pandas python scripts/simple_electricity_processing.py

echo ""
echo "📁 Processed files created:"
ls -la processed_data/*_final.csv

# Step 2: Test Snowflake connection
echo ""
echo "🔗 Step 2: Testing Snowflake connection..."
snow connection test --connection admin

# Step 3: Create database structure
echo ""
echo "🏗️  Step 3: Creating database tables and views..."
snow sql -f scripts/complete_setup.sql --connection admin

# Step 4: Upload and load data
echo ""
echo "📤 Step 4: Uploading data to Snowflake..."

# Upload files to Snowflake stage
snow sql -q "USE ROLE ACCOUNTADMIN; USE DATABASE NZ_PARTNER_HACKATHON; USE SCHEMA URU_RANGI; USE WAREHOUSE COMPUTE_WH;" --connection admin

snow sql -q "PUT file://processed_data/electricity_zone_data_5min_final.csv @electricity_data_stage AUTO_COMPRESS=TRUE;" --connection admin

snow sql -q "PUT file://processed_data/electricity_generation_by_fuel_final.csv @electricity_data_stage AUTO_COMPRESS=TRUE;" --connection admin

snow sql -q "PUT file://processed_data/electricity_quarterly_generation_final.csv @electricity_data_stage AUTO_COMPRESS=TRUE;" --connection admin

# Load data into tables
echo ""
echo "📥 Step 5: Loading data into tables..."

snow sql -q "COPY INTO electricity_zone_data_5min FROM @electricity_data_stage/electricity_zone_data_5min_final.csv.gz FILE_FORMAT = electricity_csv_format ON_ERROR = 'CONTINUE';" --connection admin

snow sql -q "COPY INTO electricity_generation_by_fuel FROM @electricity_data_stage/electricity_generation_by_fuel_final.csv.gz FILE_FORMAT = electricity_csv_format ON_ERROR = 'CONTINUE';" --connection admin

snow sql -q "COPY INTO electricity_quarterly_generation FROM @electricity_data_stage/electricity_quarterly_generation_final.csv.gz FILE_FORMAT = electricity_csv_format ON_ERROR = 'CONTINUE';" --connection admin

# Step 6: Verify data loading
echo ""
echo "✅ Step 6: Verifying data loading..."
snow sql -q "SELECT 'Zone data (5-min)' as dataset, COUNT(*) as record_count FROM electricity_zone_data_5min UNION ALL SELECT 'Fuel type data' as dataset, COUNT(*) as record_count FROM electricity_generation_by_fuel UNION ALL SELECT 'Quarterly data' as dataset, COUNT(*) as record_count FROM electricity_quarterly_generation;" --connection admin

echo ""
echo "🎉 Setup Complete!"
echo "==================="
echo ""
echo "✅ Database: NZ_PARTNER_HACKATHON"
echo "✅ Schema: URU_RANGI (Wind/Energy theme)"
echo "✅ Tables created:"
echo "   - electricity_zone_data_5min (5-minute demand data)"
echo "   - electricity_generation_by_fuel (renewable vs fossil)"  
echo "   - electricity_quarterly_generation (historical trends)"
echo "✅ Views created for analysis"
echo ""
echo "🔧 Next steps:"
echo "1. Use data_sharing_setup.sql to share with participants"
echo "2. Provide sample queries from complete_setup.sql"
echo "3. Add more datasets to other theme schemas"
echo ""
echo "📊 Sample data sharing command:"
echo "CALL setup_participant_data_share('participant-account.region.cloud', 'Team Name');"