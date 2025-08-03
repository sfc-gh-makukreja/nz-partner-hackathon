-- =============================================
-- TIPUĀNUKU (Food & The Land) - Sample Queries
-- Theme: Food, Agriculture, Land Use, and Nutrition
-- REAL DATA: 1.1M+ food products with allergy information from Open Food Facts
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE nz_partner_hackathon;
USE SCHEMA TIPUANUKU;

-- Table of Contents:
-- 1. Dataset Overview and Exploration
-- 2. Allergy Detection and Food Safety
-- 3. Regional Analysis (NZ/Australia Focus)
-- 4. AI-Powered Food Analysis
-- 5. Brand and Category Intelligence
-- 6. Nutritional Analysis
-- 7. Cross-Dataset Integration Examples
-- 8. Template Queries for Additional Data
-- 9. PROJECT IDEAS

-- =============================================
-- AVAILABLE REAL DATASETS IN TIPUANUKU
-- =============================================

-- TABLES:
-- 1. openfoodfacts_raw (3,974,038 products) - Complete Open Food Facts dataset
-- 2. food_products_allergy_focus (1,130,511 products) - Filtered for allergy analysis
-- 3. food_image_table - Food images and metadata

-- VIEWS:
-- 1. products_with_allergens - Products with allergen warnings
-- 2. oceania_food_products - NZ/Australia focused products

-- =============================================
-- 1. DATASET OVERVIEW AND EXPLORATION
-- =============================================

-- Complete dataset overview
SELECT 
    'Open Food Facts Full Dataset' as dataset_name,
    COUNT(*) as total_products,
    COUNT(DISTINCT code) as unique_barcodes,
    COUNT(CASE WHEN allergens IS NOT NULL OR allergens_en IS NOT NULL THEN 1 END) as products_with_allergens,
    COUNT(CASE WHEN traces IS NOT NULL OR traces_en IS NOT NULL THEN 1 END) as products_with_traces,
    COUNT(CASE WHEN ingredients_text IS NOT NULL THEN 1 END) as products_with_ingredients,
    ROUND(AVG(completeness), 2) as avg_data_completeness,
    MIN(created_datetime) as earliest_product,
    MAX(created_datetime) as latest_product
FROM openfoodfacts_raw;

-- Allergy-focused dataset overview
SELECT 
    'Allergy-Focused Dataset' as dataset_name,
    COUNT(*) as total_products,
    COUNT(DISTINCT barcode) as unique_barcodes,
    COUNT(CASE WHEN allergens_en IS NOT NULL OR allergens IS NOT NULL THEN 1 END) as products_with_allergens,
    COUNT(CASE WHEN traces_en IS NOT NULL OR traces IS NOT NULL THEN 1 END) as products_with_traces,
    COUNT(CASE WHEN ingredients_text IS NOT NULL THEN 1 END) as products_with_ingredients,
    ROUND(AVG(completeness), 2) as avg_data_completeness
FROM food_products_allergy_focus;

-- Top food categories globally
SELECT 
    SPLIT_PART(categories, ',', 1) as primary_category,
    COUNT(*) as product_count,
    COUNT(CASE WHEN traces_en IS NOT NULL THEN 1 END) as products_with_traces,
    ROUND(AVG(completeness), 1) as avg_completeness,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage_of_total
FROM food_products_allergy_focus
WHERE categories IS NOT NULL
GROUP BY primary_category
ORDER BY product_count DESC
LIMIT 20;

-- Data quality by completeness level
SELECT 
    CASE 
        WHEN completeness >= 80 THEN 'High Quality (80%+)'
        WHEN completeness >= 60 THEN 'Medium Quality (60-79%)'
        WHEN completeness >= 40 THEN 'Basic Quality (40-59%)'
        ELSE 'Low Quality (<40%)'
    END as quality_tier,
    COUNT(*) as product_count,
    ROUND(AVG(completeness), 1) as avg_completeness,
    COUNT(CASE WHEN ingredients_text IS NOT NULL THEN 1 END) as with_ingredients,
    COUNT(CASE WHEN traces_en IS NOT NULL THEN 1 END) as with_allergen_traces
FROM food_products_allergy_focus
GROUP BY quality_tier
ORDER BY avg_completeness DESC;

-- =============================================
-- 2. ALLERGY DETECTION AND FOOD SAFETY
-- =============================================

-- Most common allergen traces globally
SELECT 
    traces_en as allergen_trace,
    COUNT(*) as product_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage_of_products,
    ROUND(AVG(completeness), 1) as avg_data_quality
FROM food_products_allergy_focus
WHERE traces_en IS NOT NULL 
  AND traces_en != 'None'
  AND traces_en != ''
  AND LENGTH(TRIM(traces_en)) > 0
GROUP BY traces_en
ORDER BY product_count DESC
LIMIT 25;

-- Products safe for specific allergies (nut-free example)
SELECT 
    barcode,
    product_name,
    brands,
    SPLIT_PART(categories, ',', 1) as primary_category,
    traces_en,
    nutriscore_grade,
    completeness
FROM food_products_allergy_focus
WHERE (traces_en IS NULL OR traces_en NOT ILIKE '%nut%')
  AND (allergens_en IS NULL OR allergens_en NOT ILIKE '%nut%')
  AND (ingredients_text IS NULL OR ingredients_text NOT ILIKE '%nut%')
  AND product_name IS NOT NULL
  AND LENGTH(TRIM(product_name)) > 0
ORDER BY nutriscore_grade NULLS LAST, completeness DESC, product_name
LIMIT 20;

-- Multi-allergen risk analysis
SELECT 
    barcode,
    product_name,
    brands,
    traces_en,
    CASE 
        WHEN traces_en ILIKE '%nut%' AND traces_en ILIKE '%milk%' THEN 'High Risk: Nuts + Dairy'
        WHEN traces_en ILIKE '%gluten%' AND traces_en ILIKE '%egg%' THEN 'High Risk: Gluten + Eggs'
        WHEN traces_en ILIKE '%nut%' THEN 'Medium Risk: Nuts'
        WHEN traces_en ILIKE '%milk%' THEN 'Medium Risk: Dairy'
        WHEN traces_en ILIKE '%gluten%' THEN 'Medium Risk: Gluten'
        WHEN traces_en ILIKE '%egg%' THEN 'Medium Risk: Eggs'
        ELSE 'Lower Risk'
    END as risk_assessment,
    nutriscore_grade,
    completeness
FROM food_products_allergy_focus
WHERE traces_en IS NOT NULL 
  AND traces_en != 'None'
  AND product_name IS NOT NULL
ORDER BY risk_assessment, completeness DESC
LIMIT 30;

-- Brand safety analysis
SELECT 
    brands,
    COUNT(*) as total_products,
    COUNT(CASE WHEN traces_en IS NULL OR traces_en = 'None' THEN 1 END) as likely_safe_products,
    ROUND(COUNT(CASE WHEN traces_en IS NULL OR traces_en = 'None' THEN 1 END) * 100.0 / COUNT(*), 1) as safety_percentage,
    ROUND(AVG(completeness), 1) as avg_data_quality,
    LISTAGG(DISTINCT SPLIT_PART(categories, ',', 1), ', ') as common_categories
FROM food_products_allergy_focus
WHERE brands IS NOT NULL
  AND LENGTH(TRIM(brands)) > 0
GROUP BY brands
HAVING COUNT(*) >= 15  -- Brands with at least 15 products
ORDER BY safety_percentage DESC, total_products DESC
LIMIT 20;

-- =============================================
-- 3. REGIONAL ANALYSIS (NZ/AUSTRALIA FOCUS)
-- =============================================

-- Oceania product overview using the view
SELECT 
    'Oceania Food Products' as dataset_name,
    COUNT(*) as total_products,
    COUNT(DISTINCT barcode) as unique_barcodes,
    COUNT(CASE WHEN traces_en IS NOT NULL AND traces_en != 'None' THEN 1 END) as products_with_allergen_warnings,
    ROUND(AVG(completeness), 1) as avg_data_quality
FROM oceania_food_products;

-- Detailed regional breakdown
SELECT 
    CASE 
        WHEN countries ILIKE '%new zealand%' OR countries ILIKE '%new-zealand%' THEN 'New Zealand'
        WHEN countries ILIKE '%australia%' THEN 'Australia'
        ELSE 'Other Oceania'
    END as country_focus,
    COUNT(*) as total_products,
    COUNT(CASE WHEN traces_en IS NOT NULL AND traces_en != 'None' THEN 1 END) as products_with_allergen_warnings,
    ROUND(AVG(completeness), 1) as avg_data_quality,
    COUNT(DISTINCT brands) as unique_brands
FROM food_products_allergy_focus
WHERE countries ILIKE '%new zealand%' 
   OR countries ILIKE '%new-zealand%'
   OR countries ILIKE '%australia%'
GROUP BY country_focus
ORDER BY total_products DESC;

-- Popular food categories in Oceania
SELECT 
    SPLIT_PART(categories, ',', 1) as category,
    COUNT(*) as product_count,
    COUNT(CASE WHEN traces_en IS NOT NULL THEN 1 END) as products_with_warnings,
    ROUND(COUNT(CASE WHEN traces_en IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as warning_percentage,
    LISTAGG(DISTINCT brands, ', ') as sample_brands
FROM oceania_food_products
WHERE categories IS NOT NULL
GROUP BY category
ORDER BY product_count DESC
LIMIT 15;

-- Regional allergen pattern comparison
SELECT 
    CASE 
        WHEN countries ILIKE '%new zealand%' THEN 'NZ'
        WHEN countries ILIKE '%australia%' THEN 'AU'
    END as region,
    traces_en as allergen_trace,
    COUNT(*) as occurrence_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY 
        CASE WHEN countries ILIKE '%new zealand%' THEN 'NZ'
             WHEN countries ILIKE '%australia%' THEN 'AU' END), 2) as regional_percentage
FROM food_products_allergy_focus
WHERE (countries ILIKE '%new zealand%' OR countries ILIKE '%australia%')
  AND traces_en IS NOT NULL
  AND traces_en != 'None'
GROUP BY region, traces_en
ORDER BY region, occurrence_count DESC;

-- =============================================
-- 4. AI-POWERED FOOD ANALYSIS
-- =============================================

-- AI allergen detection from ingredient text (Oceania products)
SELECT 
    barcode,
    product_name,
    brands,
    LEFT(ingredients_text, 150) as ingredient_preview,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT('Analyze these food ingredients for common allergens (nuts, dairy, gluten, eggs, soy, shellfish, fish, sesame). ',
               'List detected allergens and risk level: ', 
               COALESCE(ingredients_text, 'No ingredients listed'))
    ) as ai_allergen_analysis
FROM food_products_allergy_focus
WHERE ingredients_text IS NOT NULL
  AND LENGTH(ingredients_text) > 30
  AND (countries ILIKE '%new zealand%' OR countries ILIKE '%australia%')
ORDER BY RANDOM()
LIMIT 5;

-- AI-powered safety recommendations for specific allergies
SELECT 
    product_name,
    brands,
    traces_en as official_warnings,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT('Based on these allergen warnings: "', COALESCE(traces_en, 'None listed'), 
               '" for the food product "', product_name, 
               '", provide specific safety advice for someone with severe nut allergies. ',
               'Include risk assessment and alternative suggestions.')
    ) as ai_safety_advice
FROM food_products_allergy_focus
WHERE traces_en IS NOT NULL
  AND product_name IS NOT NULL
  AND (traces_en ILIKE '%nut%' OR traces_en ILIKE '%peanut%')
ORDER BY RANDOM()
LIMIT 3;

-- Smart dietary categorization
SELECT 
    product_name,
    SPLIT_PART(categories, ',', 1) as category,
    ingredients_text,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        CONCAT('Categorize this food product for dietary restrictions: "', product_name, '". ',
               'Based on ingredients: ', COALESCE(LEFT(ingredients_text, 200), 'ingredients not available'), 
               '. Determine if it is: vegan-friendly, vegetarian, gluten-free, dairy-free, nut-free, low-sodium. ',
               'Provide yes/no for each category with confidence level.')
    ) as ai_dietary_categorization
FROM food_products_allergy_focus
WHERE product_name IS NOT NULL
  AND ingredients_text IS NOT NULL
  AND LENGTH(ingredients_text) > 50
ORDER BY RANDOM()
LIMIT 3;

-- =============================================
-- 5. BRAND AND CATEGORY INTELLIGENCE
-- =============================================

-- Brand performance analysis
SELECT 
    brands,
    COUNT(*) as product_portfolio,
    COUNT(CASE WHEN nutriscore_grade IN ('A', 'B') THEN 1 END) as high_quality_products,
    ROUND(COUNT(CASE WHEN nutriscore_grade IN ('A', 'B') THEN 1 END) * 100.0 / COUNT(*), 1) as quality_percentage,
    COUNT(CASE WHEN traces_en IS NULL OR traces_en = 'None' THEN 1 END) as likely_allergen_free,
    ROUND(AVG(completeness), 1) as avg_data_completeness,
    LISTAGG(DISTINCT SPLIT_PART(categories, ',', 1), ', ') as category_focus
FROM food_products_allergy_focus
WHERE brands IS NOT NULL
  AND LENGTH(TRIM(brands)) > 0
GROUP BY brands
HAVING COUNT(*) >= 20  -- Substantial product portfolio
ORDER BY quality_percentage DESC, product_portfolio DESC
LIMIT 15;

-- Category risk assessment
SELECT 
    SPLIT_PART(categories, ',', 1) as food_category,
    COUNT(*) as total_products,
    COUNT(CASE WHEN traces_en IS NOT NULL AND traces_en != 'None' THEN 1 END) as products_with_allergen_warnings,
    ROUND(COUNT(CASE WHEN traces_en IS NOT NULL AND traces_en != 'None' THEN 1 END) * 100.0 / COUNT(*), 1) as allergen_risk_percentage,
    LISTAGG(DISTINCT SPLIT_PART(traces_en, ',', 1), ', ') as common_allergens,
    ROUND(AVG(completeness), 1) as avg_data_quality
FROM food_products_allergy_focus
WHERE categories IS NOT NULL
GROUP BY food_category
HAVING COUNT(*) >= 50  -- Categories with substantial data
ORDER BY allergen_risk_percentage DESC
LIMIT 20;

-- =============================================
-- 6. NUTRITIONAL ANALYSIS
-- =============================================

-- Nutriscore distribution analysis
SELECT 
    nutriscore_grade,
    COUNT(*) as product_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    COUNT(CASE WHEN traces_en IS NULL OR traces_en = 'None' THEN 1 END) as allergen_safe_products,
    ROUND(AVG(energy_kcal_100g), 0) as avg_calories_per_100g,
    ROUND(AVG(proteins_100g), 1) as avg_protein_per_100g,
    ROUND(AVG(salt_100g), 2) as avg_salt_per_100g
FROM food_products_allergy_focus
WHERE nutriscore_grade IS NOT NULL
GROUP BY nutriscore_grade
ORDER BY nutriscore_grade;

-- Nutritional quality vs allergen safety correlation
SELECT 
    nutriscore_grade,
    COUNT(*) as total_products,
    COUNT(CASE WHEN traces_en IS NULL OR traces_en = 'None' THEN 1 END) as allergen_safe_products,
    ROUND(COUNT(CASE WHEN traces_en IS NULL OR traces_en = 'None' THEN 1 END) * 100.0 / COUNT(*), 1) as safety_percentage,
    ROUND(AVG(proteins_100g), 1) as avg_protein,
    ROUND(AVG(fiber_100g), 1) as avg_fiber,
    ROUND(AVG(salt_100g), 2) as avg_salt
FROM food_products_allergy_focus
WHERE nutriscore_grade IS NOT NULL
GROUP BY nutriscore_grade
ORDER BY nutriscore_grade;

-- =============================================
-- 7. AI-POWERED FOOD IMAGE ANALYSIS
-- Using Snowflake Cortex Multimodal AI (claude-3-5-sonnet, pixtral-large)
-- =============================================

-- Analyze image, suggest ingredients, and match with Oceania food products
WITH image_ingredient_analysis AS (
    SELECT 
        RELATIVE_PATH,
        SIZE,
        LAST_MODIFIED,
        SNOWFLAKE.CORTEX.COMPLETE(
            'claude-3-5-sonnet',
            'Analyze this food image and identify the main ingredients. List specific ingredient names that could be found in a product database. Respond in JSON format with main_ingredients as an array of ingredient names.',
            IMG
        ) as ingredient_analysis
    FROM food_image_table
    WHERE IMG IS NOT NULL
    ORDER BY RANDOM()
    LIMIT 5
),
parsed_ingredients AS (
    SELECT 
        RELATIVE_PATH,
        SIZE,
        LAST_MODIFIED,
        TRY_PARSE_JSON(ingredient_analysis) as ingredients_json,
        ingredient_analysis
    FROM image_ingredient_analysis
)
SELECT 
    pi.RELATIVE_PATH,
    pi.SIZE,
    pi.LAST_MODIFIED,
    pi.ingredients_json:main_ingredients as suggested_ingredients,
    -- Match with Oceania food products
    op.product_name as matching_product,
    op.brands as product_brand,
    op.categories as product_categories,
    op.ingredients_text as product_ingredients,
    op.allergens_en as product_allergens,
    op.nutriscore_grade as quality_grade,
    op.countries as available_countries
FROM parsed_ingredients pi
LEFT JOIN oceania_food_products op 
    ON (op.ingredients_text ILIKE '%' || TRIM(pi.ingredients_json:main_ingredients[0]::STRING, '"') || '%'
        OR op.product_name ILIKE '%' || TRIM(pi.ingredients_json:main_ingredients[0]::STRING, '"') || '%')
WHERE pi.ingredients_json:main_ingredients IS NOT NULL
ORDER BY pi.RELATIVE_PATH;


-- =============================================
-- 8. CROSS-DATASET INTEGRATION EXAMPLES
-- =============================================

-- Tourism + Food Safety Integration (conceptual)
SELECT 
    'Food Safety for Travelers' as analysis_type,
    COUNT(DISTINCT f.barcode) as safe_food_options,
    COUNT(CASE WHEN f.traces_en IS NULL OR f.traces_en = 'None' THEN 1 END) as minimal_allergen_risk,
    COUNT(CASE WHEN f.nutriscore_grade IN ('a', 'b') THEN 1 END) as high_quality_options,
    LISTAGG(DISTINCT f.brands, ', ') as trusted_brands
FROM food_products_allergy_focus f
WHERE f.countries ILIKE '%new zealand%'
  AND f.nutriscore_grade IN ('a', 'b')  -- Higher quality foods
  AND (f.traces_en IS NULL OR f.traces_en = 'None')
GROUP BY analysis_type;

-- Temporal trends in food safety data
SELECT 
    EXTRACT(YEAR FROM created_datetime) as product_year,
    COUNT(*) as products_added,
    COUNT(CASE WHEN traces_en IS NOT NULL AND traces_en != 'None' THEN 1 END) as products_with_warnings,
    ROUND(COUNT(CASE WHEN traces_en IS NOT NULL AND traces_en != 'None' THEN 1 END) * 100.0 / COUNT(*), 1) as warning_percentage,
    ROUND(AVG(completeness), 1) as avg_data_quality
FROM food_products_allergy_focus
WHERE created_datetime IS NOT NULL
  AND EXTRACT(YEAR FROM created_datetime) BETWEEN 2015 AND 2024
GROUP BY product_year
ORDER BY product_year DESC;

-- =============================================
-- 8. TEMPLATE QUERIES FOR ADDITIONAL DATA
-- =============================================

/*
These are template queries that participants can use when they add their own datasets:

-- Template: Restaurant analysis
SELECT 
    restaurant_name,
    location,
    cuisine_type,
    AVG(rating) as avg_rating,
    COUNT(*) as review_count
FROM restaurant_reviews  -- Table participants would create
GROUP BY restaurant_name, location, cuisine_type
ORDER BY avg_rating DESC;

-- Template: Local production analysis
SELECT 
    region,
    product_type,
    SUM(quantity_tonnes) as total_production,
    AVG(unit_price_nzd) as avg_price
FROM food_production  -- Table participants would create
WHERE production_date >= '2024-01-01'
GROUP BY region, product_type
ORDER BY total_production DESC;

-- Template: Supply chain sustainability
SELECT 
    transport_method,
    AVG(carbon_emissions_kg / distance_km) as emissions_per_km,
    AVG(spoilage_percentage) as avg_spoilage
FROM supply_chain  -- Table participants would create
GROUP BY transport_method
ORDER BY emissions_per_km;
*/

-- =============================================
-- 9. PROJECT IDEAS FOR PARTICIPANTS
-- =============================================

/*
🎯 FEATURED PROJECT: Smart Allergy Scanner App
=========================================
Use the real Open Food Facts data to build a comprehensive food safety app:

AVAILABLE REAL DATA:
* food_products_allergy_focus (1.13M products with allergen data)
* openfoodfacts_raw (3.97M complete product database)  
* food_image_table (food images with AI analysis capabilities)
* products_with_allergens (filtered view for safety analysis)
* oceania_food_products (12,819 NZ/AU products)

Core Features to Build:
✅ Barcode lookup with 1.1M+ product safety database
✅ AI ingredient analysis using Cortex COMPLETE
✅ **NEW: Food image analysis** using multimodal AI (claude-3-5-sonnet, pixtral-large)
✅ **NEW: Visual allergen detection** from packaging and ingredient labels
✅ Personalized risk assessment based on user allergy profile
✅ Alternative product recommendations for safe substitutes
✅ Regional compliance focus (NZ/AU market)
✅ Brand safety scoring and trend analysis

Technical Implementation:
* Streamlit frontend with barcode input and allergy profile selection
* Snowflake backend using food_products_allergy_focus for product lookups
* Cortex AI for intelligent ingredient analysis and safety recommendations
* Real-time safety scoring based on official allergen warnings
* Integration with external barcode scanning APIs for mobile functionality

Sample Streamlit Code Structure:
```python
import streamlit as st
import snowflake.connector

def main():
    st.title("🍎 Smart Allergy Scanner")
    
    # User profile setup
    st.sidebar.header("Your Allergy Profile")
    user_allergies = st.sidebar.multiselect(
        "Select your allergies:",
        ["Nuts", "Peanuts", "Dairy", "Gluten", "Eggs", "Soy", "Shellfish", "Fish"]
    )
    
    # Barcode input
    barcode = st.text_input("Enter or scan product barcode:")
    
    if barcode and user_allergies:
        # Query real Snowflake data
        product_safety = check_product_safety(barcode, user_allergies)
        display_safety_results(product_safety)

def check_product_safety(barcode, allergies):
    # Query food_products_allergy_focus table
    # Use Cortex AI for ingredient analysis
    # Return comprehensive safety assessment
    pass
```

📊 ADDITIONAL PROJECT IDEAS:

1. **AI FOOD VISION SYSTEM** (NEW: Using Multimodal Cortex)
   - Photo-to-nutrition analysis using food_image_table
   - Visual allergen detection from product packaging
   - Real-time meal assessment from camera input
   - Recipe suggestions from ingredient photos
   - Food freshness and quality evaluation
   - Multi-language packaging analysis for international products

2. RESTAURANT SAFETY ADVISOR (Integrate with HIWA_I_TE_RANGI tourism data)
   - Menu safety analysis for restaurants
   - Tourist-focused dining recommendations
   - Real-time allergen warnings for travelers
   - Integration with local event data for food festivals

3. BRAND SAFETY INTELLIGENCE PLATFORM
   - Brand safety scoring based on 1.1M+ product analysis
   - Trend analysis of allergen warnings over time
   - Competitive brand analysis for food companies
   - Supply chain risk assessment using real data

4. AI-POWERED NUTRITION ADVISOR
   - Automated dietary categorization (vegan, gluten-free, etc.)
   - Nutritional quality assessment using nutriscore data
   - Personalized meal planning based on allergy constraints
   - Recipe suggestions using available safe products

5. CROSS-DATASET ANALYTICS HUB
   - Tourism + Food Safety: Safe dining for travelers
   - Climate + Food: Seasonal allergy patterns (integrate with URU_RANGI)
   - Maritime + Food: Seafood safety analysis (integrate with WAITĀ)
   - Demographics + Health: Regional allergy prevalence mapping

6. FOOD SAFETY COMPLIANCE MONITOR
   - Real-time monitoring of product recalls
   - Compliance checking for food manufacturers
   - Allergen labeling accuracy verification
   - Quality assurance dashboards for retailers

SUCCESS METRICS:
- Product safety assessment time: < 2 seconds
- AI allergen detection accuracy: > 92%
- User satisfaction with recommendations: > 88%
- Cross-dataset insights integration: 3+ schemas
- Mobile app performance: Works offline for cached products

INNOVATION OPPORTUNITIES:
- **NEW: Multimodal AI** for real-time food image analysis using Cortex
- **NEW: Visual packaging analysis** with claude-3-5-sonnet and pixtral-large models  
- **NEW: Cross-reference image analysis** with 1.1M product database
- Voice interface for hands-free safety checking
- Social features for community safety reviews and photo sharing
- Integration with grocery store APIs for shopping lists
- Wearable device integration for dining out safety alerts
- **NEW: Meal planning AI** using multi-image analysis capabilities
*/