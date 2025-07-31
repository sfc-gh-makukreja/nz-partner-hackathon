-- TIPUĀNUKU (Food & The Land) - Sample Queries
-- Theme: Food, Agriculture, Land Use, and Nutrition
-- Template queries for food-related datasets

USE DATABASE NZ_HACKATHON_DATA;
USE SCHEMA TIPUANUKU;

-- =============================================
-- SAMPLE TABLE STRUCTURES (Examples for participants)
-- =============================================

/*
Example tables participants could create:

CREATE TABLE food_production (
    region STRING,
    product_type STRING,
    production_date DATE,
    quantity_tonnes NUMBER(10,2),
    unit_price_nzd NUMBER(8,2),
    farm_type STRING, -- organic, conventional, regenerative
    carbon_footprint_kg_co2 NUMBER(10,2)
);

CREATE TABLE restaurant_reviews (
    review_id STRING,
    restaurant_name STRING,
    location STRING,
    cuisine_type STRING,
    rating NUMBER(2,1),
    review_text STRING,
    review_date DATE,
    price_range STRING,
    dietary_options ARRAY -- vegan, vegetarian, gluten-free
);

CREATE TABLE food_allergies (
    person_id STRING,
    age_group STRING,
    allergies ARRAY,
    severity STRING,
    region STRING,
    diagnosis_date DATE
);

CREATE TABLE supply_chain (
    product_id STRING,
    origin_location STRING,
    destination_location STRING,
    transport_date DATE,
    transport_method STRING,
    distance_km NUMBER(8,2),
    carbon_emissions_kg NUMBER(10,2),
    cost_nzd NUMBER(10,2),
    spoilage_percentage NUMBER(5,2)
);
*/

-- =============================================
-- 1. FOOD PRODUCTION ANALYSIS
-- =============================================

-- Regional production trends
SELECT 
    region,
    product_type,
    SUM(quantity_tonnes) as total_production,
    AVG(unit_price_nzd) as avg_price,
    COUNT(DISTINCT production_date) as production_days
FROM food_production
WHERE production_date >= '2024-01-01'
GROUP BY region, product_type
ORDER BY total_production DESC;

-- Seasonal production patterns
SELECT 
    EXTRACT(MONTH FROM production_date) as month,
    product_type,
    AVG(quantity_tonnes) as avg_monthly_production,
    STDDEV(quantity_tonnes) as production_variability
FROM food_production
GROUP BY month, product_type
ORDER BY month, product_type;

-- Organic vs conventional comparison
SELECT 
    farm_type,
    product_type,
    AVG(unit_price_nzd) as avg_price,
    AVG(carbon_footprint_kg_co2) as avg_carbon_footprint,
    SUM(quantity_tonnes) as total_production
FROM food_production
GROUP BY farm_type, product_type
ORDER BY product_type, farm_type;

-- Price volatility analysis
SELECT 
    product_type,
    STDDEV(unit_price_nzd) / AVG(unit_price_nzd) * 100 as price_volatility_percent,
    MIN(unit_price_nzd) as min_price,
    MAX(unit_price_nzd) as max_price,
    AVG(unit_price_nzd) as avg_price
FROM food_production
GROUP BY product_type
ORDER BY price_volatility_percent DESC;

-- =============================================
-- 2. RESTAURANT & FOOD REVIEW ANALYSIS
-- =============================================

-- Cuisine popularity by region
SELECT 
    location,
    cuisine_type,
    COUNT(*) as restaurant_count,
    AVG(rating) as avg_rating,
    COUNT(CASE WHEN rating >= 4.0 THEN 1 END) / COUNT(*) * 100 as excellent_percentage
FROM restaurant_reviews
GROUP BY location, cuisine_type
ORDER BY location, restaurant_count DESC;

-- Sentiment analysis of reviews using Cortex AI
SELECT 
    restaurant_name,
    rating,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        CONCAT('Analyze the sentiment and key themes in this restaurant review: "', 
               SUBSTR(review_text, 1, 500), '"')
    ) as review_sentiment_analysis
FROM restaurant_reviews
WHERE LENGTH(review_text) > 100
LIMIT 10;

-- Dietary trend analysis
SELECT 
    EXTRACT(YEAR FROM review_date) as year,
    FLATTEN(dietary_options) as dietary_option,
    COUNT(*) as mention_count
FROM restaurant_reviews
GROUP BY year, dietary_option
ORDER BY year, mention_count DESC;

-- Price vs rating correlation
SELECT 
    price_range,
    AVG(rating) as avg_rating,
    COUNT(*) as restaurant_count,
    CORR(rating, CASE 
        WHEN price_range = '$' THEN 1
        WHEN price_range = '$$' THEN 2
        WHEN price_range = '$$$' THEN 3
        WHEN price_range = '$$$$' THEN 4
        ELSE 0 END) OVER () as price_rating_correlation
FROM restaurant_reviews
GROUP BY price_range
ORDER BY price_range;

-- =============================================
-- 3. FOOD ALLERGY & HEALTH ANALYSIS
-- =============================================

-- Allergy prevalence by region
SELECT 
    region,
    FLATTEN(allergies) as allergy_type,
    COUNT(*) as cases,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY region) as prevalence_percentage
FROM food_allergies
GROUP BY region, allergy_type
ORDER BY region, cases DESC;

-- Age group vulnerability analysis
SELECT 
    age_group,
    FLATTEN(allergies) as allergy_type,
    COUNT(*) as cases,
    AVG(CASE WHEN severity = 'Severe' THEN 1 ELSE 0 END) as severe_case_rate
FROM food_allergies
GROUP BY age_group, allergy_type
ORDER BY age_group, cases DESC;

-- Temporal allergy trend analysis
SELECT 
    EXTRACT(YEAR FROM diagnosis_date) as diagnosis_year,
    FLATTEN(allergies) as allergy_type,
    COUNT(*) as new_cases
FROM food_allergies
GROUP BY diagnosis_year, allergy_type
ORDER BY diagnosis_year, allergy_type;

-- =============================================
-- 4. SUPPLY CHAIN & SUSTAINABILITY
-- =============================================

-- Food miles analysis
SELECT 
    product_id,
    origin_location,
    destination_location,
    AVG(distance_km) as avg_distance,
    AVG(carbon_emissions_kg) as avg_emissions,
    SUM(cost_nzd) as total_transport_cost
FROM supply_chain
GROUP BY product_id, origin_location, destination_location
ORDER BY avg_distance DESC;

-- Transport efficiency comparison
SELECT 
    transport_method,
    AVG(distance_km) as avg_distance,
    AVG(carbon_emissions_kg / distance_km) as emissions_per_km,
    AVG(cost_nzd / distance_km) as cost_per_km,
    AVG(spoilage_percentage) as avg_spoilage
FROM supply_chain
GROUP BY transport_method
ORDER BY emissions_per_km;

-- Seasonal spoilage patterns
SELECT 
    EXTRACT(MONTH FROM transport_date) as month,
    transport_method,
    AVG(spoilage_percentage) as avg_spoilage,
    COUNT(*) as shipment_count
FROM supply_chain
GROUP BY month, transport_method
ORDER BY month, avg_spoilage DESC;

-- Carbon footprint optimization
SELECT 
    product_id,
    origin_location,
    destination_location,
    SUM(carbon_emissions_kg) as total_emissions,
    SUM(distance_km) as total_distance,
    MIN(carbon_emissions_kg / distance_km) as best_efficiency,
    MAX(carbon_emissions_kg / distance_km) as worst_efficiency
FROM supply_chain
GROUP BY product_id, origin_location, destination_location
HAVING COUNT(*) > 5  -- Multiple shipments for comparison
ORDER BY total_emissions DESC;

-- =============================================
-- 5. AI/ML FEATURE ENGINEERING
-- =============================================

-- Demand prediction features
SELECT 
    production_date,
    product_type,
    region,
    quantity_tonnes as target_production,
    -- Time features
    EXTRACT(MONTH FROM production_date) as month,
    EXTRACT(QUARTER FROM production_date) as quarter,
    EXTRACT(DAYOFYEAR FROM production_date) as day_of_year,
    -- Lag features
    LAG(quantity_tonnes, 7) OVER (PARTITION BY product_type, region ORDER BY production_date) as production_last_week,
    LAG(quantity_tonnes, 30) OVER (PARTITION BY product_type, region ORDER BY production_date) as production_last_month,
    -- Rolling averages
    AVG(quantity_tonnes) OVER (PARTITION BY product_type, region ORDER BY production_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as avg_weekly_production,
    -- Price correlation
    unit_price_nzd,
    LAG(unit_price_nzd, 1) OVER (PARTITION BY product_type, region ORDER BY production_date) as price_yesterday
FROM food_production
ORDER BY production_date, product_type, region;

-- Restaurant success prediction
SELECT 
    restaurant_name,
    location,
    cuisine_type,
    AVG(rating) as avg_rating,
    COUNT(*) as review_count,
    -- Feature engineering
    COUNT(CASE WHEN rating >= 4.5 THEN 1 END) / COUNT(*) as excellent_ratio,
    STDDEV(rating) as rating_consistency,
    DATEDIFF(DAY, MIN(review_date), MAX(review_date)) as review_span_days,
    -- Text analysis features
    AVG(LENGTH(review_text)) as avg_review_length,
    COUNT(CASE WHEN review_text ILIKE '%amazing%' OR review_text ILIKE '%excellent%' THEN 1 END) as positive_keywords
FROM restaurant_reviews
GROUP BY restaurant_name, location, cuisine_type
HAVING COUNT(*) >= 10  -- Sufficient reviews for analysis
ORDER BY avg_rating DESC;

-- =============================================
-- 6. SNOWFLAKE CORTEX AI APPLICATIONS
-- =============================================

-- Recipe recommendation system
SELECT 
    SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        CONCAT('Create a healthy recipe using these seasonal ingredients from New Zealand: ',
               'Available ingredients: ', STRING_AGG(product_type, ', '), 
               '. Focus on nutrition and local flavors. Keep it under 200 words.')
    ) as seasonal_recipe
FROM food_production
WHERE production_date >= CURRENT_DATE() - 30
  AND region = 'Canterbury'
GROUP BY region;

-- Allergy-safe menu suggestions
SELECT 
    SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        CONCAT('Suggest menu modifications for a restaurant to accommodate these common allergies in the region: ',
               STRING_AGG(DISTINCT allergy_type, ', '),
               '. Provide practical alternatives.')
    ) as allergy_safe_suggestions
FROM (
    SELECT FLATTEN(allergies) as allergy_type
    FROM food_allergies
    WHERE region = 'Auckland'
);

-- Sustainability insights
SELECT 
    transport_method,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        CONCAT('Analyze this food transport data and suggest sustainability improvements: ',
               'Method: ', transport_method, 
               ', Avg emissions per km: ', AVG(carbon_emissions_kg / distance_km)::STRING,
               ', Avg spoilage: ', AVG(spoilage_percentage)::STRING, '%')
    ) as sustainability_recommendations
FROM supply_chain
GROUP BY transport_method;

-- =============================================
-- 7. BUSINESS INTELLIGENCE QUERIES
-- =============================================

-- Market opportunity analysis
SELECT 
    r.location,
    r.cuisine_type,
    COUNT(DISTINCT r.restaurant_name) as existing_restaurants,
    AVG(r.rating) as market_avg_rating,
    -- Local production support
    COUNT(DISTINCT p.product_type) as local_ingredients_available,
    AVG(p.unit_price_nzd) as avg_ingredient_cost
FROM restaurant_reviews r
LEFT JOIN food_production p ON r.location = p.region
GROUP BY r.location, r.cuisine_type
ORDER BY existing_restaurants, market_avg_rating DESC;

-- Food security analysis
SELECT 
    region,
    product_type,
    SUM(quantity_tonnes) as total_production,
    AVG(carbon_footprint_kg_co2) as avg_carbon_footprint,
    -- Calculate food security score (higher production, lower carbon = better)
    (SUM(quantity_tonnes) / NULLIF(AVG(carbon_footprint_kg_co2), 0)) as sustainability_efficiency_score
FROM food_production
GROUP BY region, product_type
ORDER BY sustainability_efficiency_score DESC;

-- Last mile delivery optimization
SELECT 
    destination_location,
    COUNT(*) as delivery_count,
    AVG(distance_km) as avg_delivery_distance,
    AVG(spoilage_percentage) as avg_spoilage,
    SUM(cost_nzd) as total_delivery_cost,
    -- Optimization score
    (100 - AVG(spoilage_percentage)) / AVG(distance_km) as delivery_efficiency_score
FROM supply_chain
WHERE transport_date >= CURRENT_DATE() - 90
GROUP BY destination_location
ORDER BY delivery_efficiency_score DESC;

-- =============================================
-- AI/ML PROJECT IDEAS FOR PARTICIPANTS:
-- =============================================

/*
1. FOOD DEMAND FORECASTING
   - Predict seasonal demand for agricultural products
   - Weather correlation for crop yield prediction
   - Price optimization models

2. RESTAURANT RECOMMENDATION ENGINE
   - Personalized recommendations based on dietary restrictions
   - Sentiment analysis of reviews for quality scoring
   - Location-based cuisine trend analysis

3. SUPPLY CHAIN OPTIMIZATION
   - Route optimization for minimal food waste
   - Carbon footprint reduction strategies
   - Predictive spoilage models

4. NUTRITION & HEALTH INSIGHTS
   - Allergy prevalence mapping and prediction
   - Nutritional gap analysis by region
   - Health trend correlation with food availability

5. SUSTAINABLE AGRICULTURE
   - Organic vs conventional profitability analysis
   - Carbon footprint optimization for farms
   - Regenerative agriculture impact assessment

6. FOOD SAFETY & TRACEABILITY
   - Contamination source tracking
   - Quality prediction along supply chain
   - Real-time food safety monitoring

CORTEX AI APPLICATIONS:
- Recipe generation based on local ingredients
- Dietary advice and meal planning
- Food trend analysis from social media
- Automated food quality assessment from images
*/