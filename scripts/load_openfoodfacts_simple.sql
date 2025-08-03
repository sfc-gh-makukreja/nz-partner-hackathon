-- =============================================
-- SIMPLE LOAD OPEN FOOD FACTS DATA
-- Fixed version without validation mode
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE nz_partner_hackathon;
USE SCHEMA TIPUANUKU;
USE WAREHOUSE COMPUTE_WH;

-- =============================================
-- 1. SIMPLE LOAD INTO RAW TABLE
-- =============================================

-- Load essential columns for food allergy detection
COPY INTO openfoodfacts_raw (
    code, url, creator, created_t, created_datetime,
    last_modified_t, last_modified_datetime, last_modified_by,
    last_updated_t, last_updated_datetime,
    product_name, abbreviated_product_name, generic_name,
    quantity, packaging, brands, brands_tags, brands_en,
    categories, categories_tags, categories_en,
    origins, origins_tags, origins_en,
    countries, countries_tags, countries_en,
    -- CRITICAL: Allergy and ingredient data
    ingredients_text, ingredients_tags, ingredients_analysis_tags,
    allergens, allergens_en, traces, traces_tags, traces_en,
    -- Nutritional scoring
    serving_size, serving_quantity, no_nutrition_data,
    additives_n, additives, additives_tags, additives_en,
    nutriscore_score, nutriscore_grade, nova_group,
    pnns_groups_1, pnns_groups_2, food_groups, food_groups_tags, food_groups_en,
    -- Quality metadata
    states, states_tags, states_en, brand_owner,
    completeness, main_category, main_category_en,
    -- Key nutrition values
    energy_kcal_100g, fat_100g, saturated_fat_100g, trans_fat_100g,
    carbohydrates_100g, sugars_100g, fiber_100g, proteins_100g,
    salt_100g, sodium_100g, vitamin_a_100g, vitamin_c_100g, vitamin_d_100g,
    calcium_100g, iron_100g, potassium_100g,
    nutrition_score_fr_100g, nutrition_score_uk_100g
)
FROM (
    SELECT 
        $1, $2, $3, TRY_CAST($4 AS NUMBER), $5,
        TRY_CAST($6 AS NUMBER), $7, $8,
        TRY_CAST($9 AS NUMBER), $10,
        $11, $12, $13,
        $14, $15, $19, $20, $21,
        $22, $23, $24,
        $25, $26, $27,
        $40, $41, $42,
        -- Allergy critical columns (43-50)
        $43, $44, $45,
        $46, $47, $48, $49, $50,
        -- Nutritional metadata (51-65)
        $51, $52, $53,
        TRY_CAST($54 AS NUMBER), $55, $56, $57,
        TRY_CAST($58 AS NUMBER), $59, TRY_CAST($60 AS NUMBER),
        $61, $62, $63, $64, $65,
        -- Quality metadata (66-82)
        $66, $67, $68, $69,
        TRY_CAST($78 AS NUMBER), $81, $82,
        -- Key nutrition columns (90-201)
        TRY_CAST($90 AS NUMBER), TRY_CAST($93 AS NUMBER), 
        TRY_CAST($94 AS NUMBER), TRY_CAST($128 AS NUMBER),
        TRY_CAST($130 AS NUMBER), TRY_CAST($131 AS NUMBER), 
        TRY_CAST($143 AS NUMBER), TRY_CAST($146 AS NUMBER),
        TRY_CAST($150 AS NUMBER), TRY_CAST($152 AS NUMBER),
        TRY_CAST($154 AS NUMBER), TRY_CAST($159 AS NUMBER), TRY_CAST($156 AS NUMBER),
        TRY_CAST($173 AS NUMBER), TRY_CAST($175 AS NUMBER), TRY_CAST($171 AS NUMBER),
        TRY_CAST($198 AS NUMBER), TRY_CAST($199 AS NUMBER)
    FROM @openfoodfacts_stage/en.openfoodfacts.org.products.csv.gz
    (FILE_FORMAT => openfoodfacts_csv_format)
)
ON_ERROR = 'CONTINUE';

-- =============================================
-- 2. DATA QUALITY CHECKS
-- =============================================

-- Basic data quality validation
SELECT 
    'openfoodfacts_raw' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT code) as unique_products,
    COUNT(*) - COUNT(DISTINCT code) as duplicates,
    COUNT(CASE WHEN allergens IS NOT NULL OR allergens_en IS NOT NULL THEN 1 END) as products_with_allergens,
    COUNT(CASE WHEN traces IS NOT NULL OR traces_en IS NOT NULL THEN 1 END) as products_with_traces,
    COUNT(CASE WHEN countries ILIKE '%new zealand%' OR countries ILIKE '%australia%' THEN 1 END) as oceania_products,
    MIN(created_t) as earliest_timestamp,
    MAX(created_t) as latest_timestamp
FROM openfoodfacts_raw;

-- =============================================
-- 3. POPULATE FOCUSED ALLERGY TABLE
-- =============================================

-- Insert into focused table with data cleaning
INSERT INTO food_products_allergy_focus (
    barcode, product_name, brands, categories, countries,
    allergens, allergens_en, traces, traces_en,
    ingredients_text, ingredients_tags,
    energy_kcal_100g, proteins_100g, fat_100g, carbohydrates_100g,
    fiber_100g, salt_100g,
    nutriscore_grade, nova_group, completeness,
    created_datetime, last_modified_datetime
)
SELECT 
    code as barcode,
    TRIM(product_name) as product_name,
    TRIM(brands) as brands,
    TRIM(categories) as categories,
    TRIM(countries) as countries,
    TRIM(allergens) as allergens,
    TRIM(allergens_en) as allergens_en,
    TRIM(traces) as traces,
    TRIM(traces_en) as traces_en,
    TRIM(ingredients_text) as ingredients_text,
    TRIM(ingredients_tags) as ingredients_tags,
    energy_kcal_100g,
    proteins_100g,
    fat_100g,
    carbohydrates_100g,
    fiber_100g,
    salt_100g,
    nutriscore_grade,
    nova_group,
    completeness,
    TO_TIMESTAMP(created_t) as created_datetime,
    TO_TIMESTAMP(last_modified_t) as last_modified_datetime
FROM openfoodfacts_raw
WHERE code IS NOT NULL  -- Must have barcode
  AND TRIM(product_name) IS NOT NULL  -- Must have product name
  AND LENGTH(TRIM(product_name)) > 0
  AND (
      allergens IS NOT NULL 
      OR allergens_en IS NOT NULL 
      OR traces IS NOT NULL 
      OR traces_en IS NOT NULL
      OR ingredients_text IS NOT NULL
  );  -- Must have some allergy-relevant data

-- =============================================
-- 4. FINAL VALIDATION AND SUMMARY
-- =============================================

-- Focused table summary
SELECT 
    'food_products_allergy_focus' as table_name,
    COUNT(*) as total_products,
    COUNT(DISTINCT barcode) as unique_barcodes,
    COUNT(CASE WHEN allergens_en IS NOT NULL THEN 1 END) as products_with_allergens_en,
    COUNT(CASE WHEN traces_en IS NOT NULL THEN 1 END) as products_with_traces_en,
    COUNT(CASE WHEN ingredients_text IS NOT NULL THEN 1 END) as products_with_ingredients,
    ROUND(AVG(completeness), 2) as avg_completeness_score,
    COUNT(CASE WHEN countries ILIKE '%new zealand%' OR countries ILIKE '%new-zealand%' THEN 1 END) as nz_products,
    COUNT(CASE WHEN countries ILIKE '%australia%' THEN 1 END) as au_products
FROM food_products_allergy_focus;

-- Sample allergen data
SELECT 
    allergens_en,
    COUNT(*) as product_count
FROM food_products_allergy_focus
WHERE allergens_en IS NOT NULL
GROUP BY allergens_en
ORDER BY product_count DESC
LIMIT 10;

-- Sample trace data  
SELECT 
    traces_en,
    COUNT(*) as product_count
FROM food_products_allergy_focus
WHERE traces_en IS NOT NULL
GROUP BY traces_en
ORDER BY product_count DESC
LIMIT 10;