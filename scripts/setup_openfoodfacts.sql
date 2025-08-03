-- =============================================
-- OPEN FOOD FACTS DATA SETUP
-- Load Open Food Facts CSV data into Snowflake
-- For TIPUĀNUKU food allergy detection project
-- =============================================

USE ROLE ACCOUNTADMIN;
USE DATABASE nz_partner_hackathon;
USE SCHEMA TIPUANUKU;
USE WAREHOUSE COMPUTE_WH;

-- =============================================
-- 1. CREATE STAGE AND FILE FORMAT
-- =============================================

-- Stage for Open Food Facts data
CREATE OR REPLACE STAGE openfoodfacts_stage
    COMMENT = 'Stage for Open Food Facts CSV data';

-- File format for tab-separated CSV
CREATE OR REPLACE FILE FORMAT openfoodfacts_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = '\t'
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'AUTO'
    TIME_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    COMMENT = 'File format for Open Food Facts tab-separated CSV';

-- =============================================
-- 2. CREATE MAIN TABLE (ALL COLUMNS)
-- =============================================

CREATE OR REPLACE TABLE openfoodfacts_raw (
    -- Basic product info
    code STRING,
    url STRING,
    creator STRING,
    created_t NUMBER,
    created_datetime STRING,
    last_modified_t NUMBER,
    last_modified_datetime STRING,
    last_modified_by STRING,
    last_updated_t NUMBER,
    last_updated_datetime STRING,
    
    -- Product identification
    product_name STRING,
    abbreviated_product_name STRING,
    generic_name STRING,
    quantity STRING,
    packaging STRING,
    packaging_tags STRING,
    packaging_en STRING,
    packaging_text STRING,
    
    -- Brand and category info
    brands STRING,
    brands_tags STRING,
    brands_en STRING,
    categories STRING,
    categories_tags STRING,
    categories_en STRING,
    
    -- Origin and location
    origins STRING,
    origins_tags STRING,
    origins_en STRING,
    manufacturing_places STRING,
    manufacturing_places_tags STRING,
    labels STRING,
    labels_tags STRING,
    labels_en STRING,
    emb_codes STRING,
    emb_codes_tags STRING,
    first_packaging_code_geo STRING,
    cities STRING,
    cities_tags STRING,
    purchase_places STRING,
    stores STRING,
    countries STRING,
    countries_tags STRING,
    countries_en STRING,
    
    -- CRITICAL: Ingredients and allergens
    ingredients_text STRING,
    ingredients_tags STRING,
    ingredients_analysis_tags STRING,
    allergens STRING,
    allergens_en STRING,
    traces STRING,
    traces_tags STRING,
    traces_en STRING,
    
    -- Nutritional metadata
    serving_size STRING,
    serving_quantity STRING,
    no_nutrition_data STRING,
    additives_n NUMBER,
    additives STRING,
    additives_tags STRING,
    additives_en STRING,
    nutriscore_score NUMBER,
    nutriscore_grade STRING,
    nova_group NUMBER,
    pnns_groups_1 STRING,
    pnns_groups_2 STRING,
    food_groups STRING,
    food_groups_tags STRING,
    food_groups_en STRING,
    
    -- Quality and metadata
    states STRING,
    states_tags STRING,
    states_en STRING,
    brand_owner STRING,
    environmental_score_score NUMBER,
    environmental_score_grade STRING,
    nutrient_levels_tags STRING,
    product_quantity STRING,
    owner STRING,
    data_quality_errors_tags STRING,
    unique_scans_n NUMBER,
    popularity_tags STRING,
    completeness NUMBER,
    last_image_t NUMBER,
    last_image_datetime STRING,
    main_category STRING,
    main_category_en STRING,
    
    -- Images
    image_url STRING,
    image_small_url STRING,
    image_ingredients_url STRING,
    image_ingredients_small_url STRING,
    image_nutrition_url STRING,
    image_nutrition_small_url STRING,
    
    -- Basic nutrition (per 100g)
    energy_kj_100g NUMBER,
    energy_kcal_100g NUMBER,
    energy_100g NUMBER,
    energy_from_fat_100g NUMBER,
    fat_100g NUMBER,
    saturated_fat_100g NUMBER,
    
    -- Skip detailed fatty acid columns for simplicity (cols 95-128)
    trans_fat_100g NUMBER,
    cholesterol_100g NUMBER,
    carbohydrates_100g NUMBER,
    sugars_100g NUMBER,
    added_sugars_100g NUMBER,
    starch_100g NUMBER,
    fiber_100g NUMBER,
    proteins_100g NUMBER,
    salt_100g NUMBER,
    sodium_100g NUMBER,
    alcohol_100g NUMBER,
    
    -- Key vitamins and minerals
    vitamin_a_100g NUMBER,
    vitamin_c_100g NUMBER,
    vitamin_d_100g NUMBER,
    calcium_100g NUMBER,
    iron_100g NUMBER,
    potassium_100g NUMBER,
    
    -- Scores and indexes
    nutrition_score_fr_100g NUMBER,
    nutrition_score_uk_100g NUMBER,
    
    -- Store remaining columns as VARIANT for flexibility
    additional_data VARIANT
);

-- =============================================
-- 3. CREATE FOCUSED ALLERGY TABLE
-- =============================================

CREATE OR REPLACE TABLE food_products_allergy_focus (
    -- Core identification
    barcode STRING PRIMARY KEY,
    product_name STRING,
    brands STRING,
    categories STRING,
    countries STRING,
    
    -- Allergy-critical information
    allergens STRING,
    allergens_en STRING,
    traces STRING,
    traces_en STRING,
    ingredients_text STRING,
    ingredients_tags STRING,
    
    -- Basic nutrition for context
    energy_kcal_100g NUMBER,
    proteins_100g NUMBER,
    fat_100g NUMBER,
    carbohydrates_100g NUMBER,
    fiber_100g NUMBER,
    salt_100g NUMBER,
    
    -- Quality indicators
    nutriscore_grade STRING,
    nova_group NUMBER,
    completeness NUMBER,
    
    -- Metadata
    created_datetime TIMESTAMP,
    last_modified_datetime TIMESTAMP
);

-- =============================================
-- 4. VALIDATION VIEWS
-- =============================================

-- Products with allergen information
CREATE OR REPLACE VIEW products_with_allergens AS
SELECT 
    barcode,
    product_name,
    brands,
    allergens_en,
    traces_en,
    countries,
    nutriscore_grade,
    completeness
FROM food_products_allergy_focus
WHERE allergens_en IS NOT NULL 
   OR traces_en IS NOT NULL
   OR allergens IS NOT NULL 
   OR traces IS NOT NULL;

-- New Zealand/Australia focused products
CREATE OR REPLACE VIEW oceania_food_products AS
SELECT *
FROM food_products_allergy_focus
WHERE countries ILIKE '%new zealand%' 
   OR countries ILIKE '%australia%'
   OR countries ILIKE '%new-zealand%'
   OR countries ILIKE '%en:new-zealand%'
   OR countries ILIKE '%en:australia%';

SHOW TABLES;
SHOW FILE FORMATS;
SHOW STAGES;