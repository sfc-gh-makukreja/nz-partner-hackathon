-- TEMPLATE: [THEME_NAME] - Sample Queries
-- Theme: [Description of theme]
-- Data Available: [List of datasets and tables]

USE DATABASE NZ_HACKATHON_DATA;
USE SCHEMA [SCHEMA_NAME];

-- =============================================
-- 1. DATA EXPLORATION
-- =============================================

-- Overview of available data
SELECT 'Table Name' as dataset, COUNT(*) as records FROM [table_name];

-- Data quality check
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT [key_column]) as unique_records,
    MIN([date_column]) as earliest_date,
    MAX([date_column]) as latest_date
FROM [table_name];

-- =============================================
-- 2. BASIC ANALYSIS
-- =============================================

-- [Add theme-specific analysis queries]

-- =============================================
-- 3. ADVANCED ANALYTICS
-- =============================================

-- [Add complex analytical queries]

-- =============================================
-- 4. AI/ML FEATURE ENGINEERING
-- =============================================

-- [Add feature engineering queries for ML models]

-- =============================================
-- 5. SNOWFLAKE CORTEX AI EXAMPLES
-- =============================================

-- Generate insights using Cortex AI
SELECT 
    SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        CONCAT('Analyze this [theme] data and provide insights: ', [data_summary])
    ) as ai_insights
FROM [table_name];

-- =============================================
-- AI/ML PROJECT IDEAS:
-- =============================================

/*
[List specific AI/ML project ideas for this theme]

1. PROJECT IDEA 1
   - Description
   - Key datasets needed
   - Expected outcomes

2. PROJECT IDEA 2
   - Description
   - Key datasets needed  
   - Expected outcomes

CORTEX AI APPLICATIONS:
- [List specific Cortex AI use cases]
*/