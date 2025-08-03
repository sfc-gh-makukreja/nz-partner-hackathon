-- =============================================
-- FOUNDATIONAL SAMPLE QUERIES
-- Theme: Socio-Economic Baseline Analysis
-- Data Sources: Stats NZ Income & Productivity Statistics
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE nz_partner_hackathon;
USE SCHEMA FOUNDATIONAL;

-- =============================================
-- 1. BASIC DATA EXPLORATION
-- =============================================

-- Data coverage overview
SELECT * FROM economic_indicators_summary
ORDER BY data_type, dataset;


-- =============================================
-- 2. GENDER PAY GAP ANALYSIS
-- =============================================

-- Current gender pay gap by occupation (latest year)
WITH latest_earnings AS (
    SELECT 
        occupation,
        sex,
        qualification,
        avg_hourly_earnings,
        avg_weekly_income,
        year
    FROM income_gender_analysis
    WHERE year = (SELECT MAX(year) FROM income_gender_analysis)
    AND avg_hourly_earnings IS NOT NULL
),
gender_comparison AS (
    SELECT 
        occupation,
        qualification,
        MAX(CASE WHEN sex = 'Male' THEN avg_hourly_earnings END) as male_hourly_rate,
        MAX(CASE WHEN sex = 'Female' THEN avg_hourly_earnings END) as female_hourly_rate,
        MAX(CASE WHEN sex = 'Male' THEN avg_weekly_income END) as male_weekly_income,
        MAX(CASE WHEN sex = 'Female' THEN avg_weekly_income END) as female_weekly_income
    FROM latest_earnings
    GROUP BY occupation, qualification
    HAVING male_hourly_rate IS NOT NULL AND female_hourly_rate IS NOT NULL
)
SELECT 
    occupation,
    qualification,
    male_hourly_rate,
    female_hourly_rate,
    ROUND((male_hourly_rate - female_hourly_rate) / male_hourly_rate * 100, 1) as hourly_pay_gap_percentage,
    male_weekly_income,
    female_weekly_income,
    ROUND((male_weekly_income - female_weekly_income) / male_weekly_income * 100, 1) as weekly_pay_gap_percentage
FROM gender_comparison
ORDER BY hourly_pay_gap_percentage DESC
LIMIT 15;

-- =============================================
-- 3. REGIONAL ECONOMIC ANALYSIS
-- =============================================

-- Regional income distribution and composition (latest year)
SELECT 
    region,
    household_type,
    avg_total_income,
    avg_wage_salary_income,
    avg_self_employed_income,
    avg_government_transfer_income,
    wage_salary_percentage,
    self_employed_percentage,
    government_transfer_percentage,
    -- Economic resilience indicator (diversification of income sources)
    CASE 
        WHEN wage_salary_percentage > 80 THEN 'Wage Dependent'
        WHEN self_employed_percentage > 30 THEN 'Entrepreneurial'
        WHEN government_transfer_percentage > 25 THEN 'Transfer Dependent'
        ELSE 'Diversified'
    END as income_profile
FROM regional_income_trends
WHERE year = (SELECT MAX(year) FROM regional_income_trends)
AND avg_total_income IS NOT NULL
ORDER BY avg_total_income DESC;

-- Regional income growth trends (5-year comparison)
WITH income_comparison AS (
    SELECT 
        region,
        household_type,
        year,
        avg_total_income,
        LAG(avg_total_income, 20) OVER (PARTITION BY region, household_type ORDER BY year) as income_5_years_ago
    FROM regional_income_trends
    WHERE avg_total_income IS NOT NULL
)
SELECT 
    region,
    household_type,
    year as latest_year,
    avg_total_income as current_income,
    income_5_years_ago,
    ROUND((avg_total_income - income_5_years_ago) / NULLIF(income_5_years_ago, 0) * 100, 1) as five_year_growth_percentage,
    ROUND(POW((avg_total_income / NULLIF(income_5_years_ago, 0)), (1.0/5)) - 1, 3) * 100 as annual_growth_rate
FROM income_comparison
WHERE year = (SELECT MAX(year) FROM regional_income_trends)
AND income_5_years_ago IS NOT NULL
ORDER BY five_year_growth_percentage DESC;

-- =============================================
-- 4. PRODUCTIVITY TRENDS ANALYSIS
-- =============================================

-- Industry productivity performance (latest 10 years)
WITH recent_productivity AS (
    SELECT *
    FROM industry_productivity_trends
    WHERE year >= (SELECT MAX(year) - 10 FROM industry_productivity_trends)
    AND labour_productivity IS NOT NULL
),
productivity_stats AS (
    SELECT 
        industry,
        AVG(labour_productivity) as avg_labour_productivity,
        AVG(multifactor_productivity) as avg_multifactor_productivity,
        AVG(labour_productivity_yoy_change) as avg_annual_growth,
        STDDEV(labour_productivity_yoy_change) as productivity_volatility,
        COUNT(*) as years_of_data
    FROM recent_productivity
    GROUP BY industry
    HAVING years_of_data >= 5
)
SELECT 
    industry,
    ROUND(avg_labour_productivity, 2) as avg_labour_productivity,
    ROUND(avg_multifactor_productivity, 2) as avg_multifactor_productivity,
    ROUND(avg_annual_growth, 2) as avg_annual_growth_rate,
    ROUND(productivity_volatility, 2) as volatility_score,
    CASE 
        WHEN avg_annual_growth > 2 AND productivity_volatility < 3 THEN 'High Growth, Stable'
        WHEN avg_annual_growth > 2 AND productivity_volatility >= 3 THEN 'High Growth, Volatile'
        WHEN avg_annual_growth > 0 AND productivity_volatility < 3 THEN 'Moderate Growth, Stable'
        WHEN avg_annual_growth > 0 AND productivity_volatility >= 3 THEN 'Moderate Growth, Volatile'
        ELSE 'Low/Negative Growth'
    END as productivity_profile
FROM productivity_stats
ORDER BY avg_annual_growth DESC;

-- =============================================
-- 5. AI-POWERED ECONOMIC INSIGHTS
-- =============================================

-- AI-powered regional economic assessment
WITH regional_summary AS (
    SELECT 
        region,
        AVG(avg_total_income) as avg_income,
        AVG(wage_salary_percentage) as avg_wage_dependency,
        AVG(government_transfer_percentage) as avg_transfer_dependency,
        COUNT(DISTINCT household_type) as household_diversity
    FROM regional_income_trends
    WHERE year >= (SELECT MAX(year) - 2 FROM regional_income_trends)
    GROUP BY region
)
SELECT 
    region,
    ROUND(avg_income, 0) as average_income,
    ROUND(avg_wage_dependency, 1) as wage_dependency_pct,
    ROUND(avg_transfer_dependency, 1) as transfer_dependency_pct,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        PROMPT(
            'Analyze this regional economic profile for {0}: Average Income: ${1}, Wage Dependency: {2}%, Transfer Dependency: {3}%, Household Types: {4}. Provide a brief economic assessment including strengths, challenges, and development recommendations. Focus on economic resilience and growth potential.',
            region,
            avg_income,
            avg_wage_dependency,
            avg_transfer_dependency,
            household_diversity
        )
    ) as ai_economic_assessment
FROM regional_summary
ORDER BY avg_income DESC
LIMIT 10;

-- AI-powered occupational equity analysis
WITH occupation_analysis AS (
    SELECT 
        occupation,
        AVG(CASE WHEN sex = 'Male' THEN avg_hourly_earnings END) as male_avg_hourly,
        AVG(CASE WHEN sex = 'Female' THEN avg_hourly_earnings END) as female_avg_hourly,
        COUNT(DISTINCT qualification) as qualification_levels
    FROM income_gender_analysis
    WHERE year >= (SELECT MAX(year) - 1 FROM income_gender_analysis)
    AND avg_hourly_earnings IS NOT NULL
    GROUP BY occupation
    HAVING male_avg_hourly IS NOT NULL AND female_avg_hourly IS NOT NULL
)
SELECT 
    occupation,
    ROUND(male_avg_hourly, 2) as male_hourly_rate,
    ROUND(female_avg_hourly, 2) as female_hourly_rate,
    ROUND((male_avg_hourly - female_avg_hourly) / male_avg_hourly * 100, 1) as pay_gap_percentage,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        PROMPT(
            'Analyze the gender pay gap in {0}: Male rate: ${1}/hour, Female rate: ${2}/hour, Gap: {3}%. Provide insights on potential causes and recommendations for addressing pay equity in this occupation. Consider industry norms, skill requirements, and career progression factors.',
            occupation,
            male_avg_hourly,
            female_avg_hourly,
            ROUND((male_avg_hourly - female_avg_hourly) / male_avg_hourly * 100, 1)
        )
    ) as equity_analysis
FROM occupation_analysis
ORDER BY pay_gap_percentage DESC
LIMIT 8;

-- =============================================
-- 6. CROSS-DATASET INTEGRATION EXAMPLES
-- =============================================

-- Economic resilience indicator
WITH economic_indicators AS (
    SELECT 
        region,
        year,
        AVG(government_transfer_percentage) as transfer_dependency,
        STDDEV(avg_total_income) as income_volatility,
        COUNT(DISTINCT household_type) as household_diversity
    FROM regional_income_trends
    WHERE year >= (SELECT MAX(year) - 5 FROM regional_income_trends)
    GROUP BY region, year
)
SELECT 
    region,
    ROUND(AVG(transfer_dependency), 1) as avg_transfer_dependency,
    ROUND(AVG(income_volatility), 0) as avg_income_volatility,
    AVG(household_diversity) as avg_household_diversity,
    -- Economic resilience score (lower is more resilient)
    ROUND(
        (AVG(transfer_dependency) * 0.4) + 
        (AVG(income_volatility) / 100 * 0.3) + 
        (100 / AVG(household_diversity) * 0.3), 1
    ) as economic_resilience_score,
    CASE 
        WHEN ROUND((AVG(transfer_dependency) * 0.4) + (AVG(income_volatility) / 100 * 0.3) + (100 / AVG(household_diversity) * 0.3), 1) < 20 THEN 'Highly Resilient'
        WHEN ROUND((AVG(transfer_dependency) * 0.4) + (AVG(income_volatility) / 100 * 0.3) + (100 / AVG(household_diversity) * 0.3), 1) < 30 THEN 'Moderately Resilient'
        ELSE 'Vulnerable'
    END as resilience_category
FROM economic_indicators
GROUP BY region
ORDER BY economic_resilience_score ASC;

-- =============================================
-- 7. PROJECT IDEAS FOR PARTICIPANTS
-- =============================================

/*
🎯 POTENTIAL AI/ML PROJECT IDEAS:

📊 ECONOMIC DASHBOARD APPLICATIONS:
   ✅ Regional Economic Health Monitor
   - Real-time regional economic indicators dashboard
   - Early warning system for economic downturns
   - Investment opportunity scoring by region
   
   ✅ Pay Equity Analyzer
   - Gender pay gap tracking across occupations
   - Fair pay recommendations using AI
   - Bias detection in compensation patterns
   
   ✅ Economic Forecasting System
   - Income trend prediction using 25+ years of data
   - Regional growth opportunity identification
   - Industry productivity forecasting

🤖 AI-POWERED INSIGHTS:
   ✅ Economic Policy Impact Simulator
   - Use Cortex AI to assess policy change impacts
   - Regional economic scenario modeling
   - Evidence-based policy recommendations
   
   ✅ Career Guidance Platform
   - Income potential analysis by occupation/education
   - Skills gap identification using productivity data
   - Personalized career progression recommendations
   
   ✅ Regional Development Advisor
   - AI-powered economic development strategies
   - Investment priority recommendations
   - Resource allocation optimization

📈 ADVANCED ANALYTICS:
   ✅ Economic Resilience Scoring
   - Multi-factor economic vulnerability assessment
   - Regional risk profiling for decision makers
   - Crisis preparedness indicators
   
   ✅ Income Inequality Analysis
   - Comprehensive inequality tracking system
   - Social mobility pathway analysis
   - Intervention effectiveness measurement

🔗 CROSS-THEME INTEGRATION:
   - Economic data + Tourism patterns (HIWA_I_TE_RANGI)
   - Income trends + Climate impacts (WAIPUNA_RANGI)
   - Regional economics + Energy consumption (URU_RANGI)
   - Economic development + Food security (TIPUANUKU)
   - Productivity + Maritime industry (WAITA)

TECHNICAL FEATURES:
   ✅ 62,218+ total economic records (1979-2024)
   ✅ Income data: 15,012 earnings records + 42,806 household records
   ✅ Productivity data: 4,400 industry records across 45 years
   ✅ Regional coverage: All NZ regions with household diversity
   ✅ Occupational coverage: All major ANZSCO occupation categories
   ✅ AI-powered insights using Snowflake Cortex
   ✅ Ready for Streamlit dashboard development
*/