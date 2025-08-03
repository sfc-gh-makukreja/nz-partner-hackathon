-- ================================================================
-- FISHING DOCUMENTS CORTEX RAG SETUP - COMPLETE AI-READY SYSTEM
-- Processes all PDF fishing regulations and creates production Cortex Search Service
-- for intelligent Q&A and RAG applications
-- FIXED VERSION: Based on successful individual testing
-- ================================================================

-- ================================================================
-- SECTION 1: INFRASTRUCTURE SETUP & CONTEXT
-- ================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE nz_partner_hackathon;
USE SCHEMA WAITA;
USE WAREHOUSE COMPUTE_WH;

-- Grant Cortex privileges if needed
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE ACCOUNTADMIN;

-- ================================================================
-- SECTION 2: ENHANCED DOCUMENT PROCESSING INFRASTRUCTURE  
-- ================================================================

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS fishing_document_chunks;
DROP TABLE IF EXISTS fishing_documents;

-- Create enhanced fishing documents table
CREATE OR REPLACE TABLE fishing_documents (
    document_id STRING PRIMARY KEY,
    file_name STRING NOT NULL,
    file_path STRING NOT NULL,
    document_type STRING DEFAULT 'Fishing Regulations',
    nz_region STRING,
    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    parsed_text VARIANT COMMENT 'Full parsed document content from PARSE_DOCUMENT',
    document_size_bytes NUMBER,
    page_count NUMBER,
    extraction_method STRING DEFAULT 'CORTEX_PARSE_DOCUMENT_LAYOUT',
    processing_status STRING DEFAULT 'PENDING',
    error_message STRING,
    data_source STRING DEFAULT 'Fisheries NZ',
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Master table for parsed fishing regulation PDFs with comprehensive metadata';

-- Create enhanced document chunks table optimized for Cortex Search
CREATE OR REPLACE TABLE fishing_document_chunks (
    chunk_id STRING PRIMARY KEY,
    document_id STRING NOT NULL,
    file_name STRING NOT NULL,
    chunk_text STRING NOT NULL COMMENT 'Text chunk optimized for semantic search (≤512 tokens)',
    chunk_sequence NUMBER NOT NULL COMMENT 'Order of chunks within document',
    chunk_size_tokens NUMBER COMMENT 'Actual token count for this chunk',
    chunk_size_chars NUMBER COMMENT 'Character count for this chunk',
    document_section STRING COMMENT 'Auto-detected section category',
    nz_region STRING COMMENT 'Regional jurisdiction',
    keywords ARRAY COMMENT 'Extracted keywords for enhanced search',
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    data_source STRING DEFAULT 'Fisheries NZ',
    FOREIGN KEY (document_id) REFERENCES fishing_documents(document_id)
) COMMENT = 'Optimized text chunks for Cortex Search semantic retrieval and RAG applications';

-- Create the stage for PDFs (if not exists)
CREATE OR REPLACE STAGE fishing_documents_stage
DIRECTORY = (ENABLE = TRUE)
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
COMMENT = 'Stage for fishing regulation PDFs and maritime documents with server-side encryption for Cortex parsing';

-- ================================================================
-- SECTION 3: UPLOAD PDF FILES WITHOUT COMPRESSION
-- CRITICAL: PARSE_DOCUMENT requires uncompressed files
-- ================================================================

-- Upload PDFs without compression (CRITICAL for PARSE_DOCUMENT)
-- Remove any existing compressed files first
REMOVE @fishing_documents_stage;

-- Upload without compression - PARSE_DOCUMENT doesn't support .gz files
PUT file://data/fish-pdf/*.pdf @fishing_documents_stage AUTO_COMPRESS=FALSE;

-- Verify the upload
LIST @fishing_documents_stage;

-- ================================================================
-- SECTION 4: BATCH PDF PROCESSING - FIXED APPROACH
-- Process each PDF individually (not using RESULT_SCAN approach)
-- ================================================================

-- Process all PDFs in one batch using known filenames
INSERT INTO fishing_documents (document_id, file_name, file_path, nz_region, parsed_text, page_count, document_size_bytes, processing_status)
SELECT * FROM (
  -- Auckland document
  SELECT 'auckland-kermadec-2024-001', '7275-December-2024-Auckland-Kermadec-Recreational-Fishing-Rules.pdf', 
    '@fishing_documents_stage/7275-December-2024-Auckland-Kermadec-Recreational-Fishing-Rules.pdf', 'Auckland',
    TRY_CAST(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@fishing_documents_stage, '7275-December-2024-Auckland-Kermadec-Recreational-Fishing-Rules.pdf', {'mode': 'LAYOUT'}) AS VARIANT), 0, 0, 'PROCESSING'
  UNION ALL
  -- West Coast document
  SELECT 'challenger-west-2024-001', '39017-2024-Challenger-West-Rec-Fish-Rules-Dec-PRINT.pdf', 
    '@fishing_documents_stage/39017-2024-Challenger-West-Rec-Fish-Rules-Dec-PRINT.pdf', 'West Coast',
    TRY_CAST(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@fishing_documents_stage, '39017-2024-Challenger-West-Rec-Fish-Rules-Dec-PRINT.pdf', {'mode': 'LAYOUT'}) AS VARIANT), 0, 0, 'PROCESSING'
  UNION ALL
  -- Canterbury - Kaikoura document
  SELECT 'kaikoura-2022-001', '3915-3915-2022-Kaikoura-Rec-Fish-Rules-May-Web.pdf', 
    '@fishing_documents_stage/3915-3915-2022-Kaikoura-Rec-Fish-Rules-May-Web.pdf', 'Canterbury',
    TRY_CAST(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@fishing_documents_stage, '3915-3915-2022-Kaikoura-Rec-Fish-Rules-May-Web.pdf', {'mode': 'LAYOUT'}) AS VARIANT), 0, 0, 'PROCESSING'
  UNION ALL
  -- Canterbury - South East South document
  SELECT 'south-east-south-2024-001', '42237-2024-South-East-South-Recreational-Fishing-Rules.pdf', 
    '@fishing_documents_stage/42237-2024-South-East-South-Recreational-Fishing-Rules.pdf', 'Canterbury',
    TRY_CAST(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@fishing_documents_stage, '42237-2024-South-East-South-Recreational-Fishing-Rules.pdf', {'mode': 'LAYOUT'}) AS VARIANT), 0, 0, 'PROCESSING'
  UNION ALL
  -- Canterbury - Challenger East document
  SELECT 'challenger-east-2024-001', '42273-2024-Challenger-East-.pdf', 
    '@fishing_documents_stage/42273-2024-Challenger-East-.pdf', 'Canterbury',
    TRY_CAST(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@fishing_documents_stage, '42273-2024-Challenger-East-.pdf', {'mode': 'LAYOUT'}) AS VARIANT), 0, 0, 'PROCESSING'
  UNION ALL
  -- Canterbury - South East North document
  SELECT 'south-east-north-2023-001', '42366-42366-2023-South-East-North-Rec-Fishing-Rules-Dec-PRINT.pdf', 
    '@fishing_documents_stage/42366-42366-2023-South-East-North-Rec-Fishing-Rules-Dec-PRINT.pdf', 'Canterbury',
    TRY_CAST(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@fishing_documents_stage, '42366-42366-2023-South-East-North-Rec-Fishing-Rules-Dec-PRINT.pdf', {'mode': 'LAYOUT'}) AS VARIANT), 0, 0, 'PROCESSING'
  UNION ALL
  -- Southland document
  SELECT 'southland-2023-001', '929-2023-Southland-Rec-Fish-Rules-Dec-PRINT.pdf', 
    '@fishing_documents_stage/929-2023-Southland-Rec-Fish-Rules-Dec-PRINT.pdf', 'Southland',
    TRY_CAST(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@fishing_documents_stage, '929-2023-Southland-Rec-Fish-Rules-Dec-PRINT.pdf', {'mode': 'LAYOUT'}) AS VARIANT), 0, 0, 'PROCESSING'
  UNION ALL
  -- Fiordland document
  SELECT 'fiordland-001', '935-Fiordland-Recreational-Fishing-Rules.pdf', 
    '@fishing_documents_stage/935-Fiordland-Recreational-Fishing-Rules.pdf', 'Southland',
    TRY_CAST(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@fishing_documents_stage, '935-Fiordland-Recreational-Fishing-Rules.pdf', {'mode': 'LAYOUT'}) AS VARIANT), 0, 0, 'PROCESSING'
);

-- Update processing status and metadata for all documents
UPDATE fishing_documents 
SET 
    page_count = COALESCE(parsed_text:metadata:pageCount::NUMBER, 0),
    document_size_bytes = COALESCE(LENGTH(parsed_text:content::STRING), 0),
    processing_status = CASE 
        WHEN parsed_text IS NOT NULL AND LENGTH(parsed_text:content::STRING) > 100 
        THEN 'SUCCESS'
        ELSE 'FAILED'
    END,
    error_message = CASE 
        WHEN parsed_text IS NULL OR LENGTH(parsed_text:content::STRING) <= 100 
        THEN 'Failed to parse PDF or content too short'
        ELSE NULL
    END
WHERE processing_status = 'PROCESSING';

-- Check processing results
SELECT processing_status, COUNT(*) as document_count, SUM(page_count) as total_pages, SUM(document_size_bytes) as total_bytes
FROM fishing_documents 
GROUP BY processing_status
ORDER BY processing_status;

-- ================================================================
-- SECTION 5: INTELLIGENT TEXT CHUNKING WITH CORTEX
-- Chunk all successfully parsed documents for optimal search
-- ================================================================

-- Create optimized text chunks using SPLIT_TEXT_RECURSIVE_CHARACTER
INSERT INTO fishing_document_chunks (
    chunk_id, document_id, file_name, chunk_text, chunk_sequence,
    chunk_size_tokens, chunk_size_chars, document_section, nz_region, keywords
)
WITH document_chunks AS (
    SELECT 
        fd.document_id,
        fd.file_name,
        fd.nz_region,
        c.SEQ as chunk_sequence,
        c.VALUE::STRING as chunk_text
    FROM fishing_documents fd,
    LATERAL FLATTEN(
        input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
            fd.parsed_text:content::STRING,
            'markdown',          -- Format for better structure preservation
            512,                 -- Optimal chunk size for Cortex Search
            50                   -- Overlap for context preservation
        )
    ) c
    WHERE fd.processing_status = 'SUCCESS'
    AND LENGTH(TRIM(c.VALUE::STRING)) > 50  -- Filter out very short chunks
)
SELECT 
    dc.document_id || '_chunk_' || LPAD(dc.chunk_sequence::STRING, 4, '0') as chunk_id,
    dc.document_id,
    dc.file_name,
    dc.chunk_text,
    dc.chunk_sequence,
    SNOWFLAKE.CORTEX.COUNT_TOKENS('snowflake-arctic-embed-l-v2.0', dc.chunk_text) as chunk_size_tokens,
    LENGTH(dc.chunk_text) as chunk_size_chars,
    -- Intelligent section classification using comprehensive keywords
    CASE 
        -- Daily Bag Limits
        WHEN UPPER(dc.chunk_text) LIKE '%DAILY BAG LIMIT%' 
            OR UPPER(dc.chunk_text) LIKE '%BAG LIMIT%' 
            OR UPPER(dc.chunk_text) LIKE '%DAILY LIMIT%'
            OR UPPER(dc.chunk_text) LIKE '%PER DAY%'
            OR UPPER(dc.chunk_text) LIKE '%MAXIMUM%CATCH%'
        THEN 'Daily Bag Limits'
        
        -- Size Restrictions  
        WHEN UPPER(dc.chunk_text) LIKE '%SIZE LIMIT%' 
            OR UPPER(dc.chunk_text) LIKE '%MINIMUM SIZE%' 
            OR UPPER(dc.chunk_text) LIKE '%LENGTH LIMIT%'
            OR UPPER(dc.chunk_text) LIKE '%MINIMUM LENGTH%'
            OR UPPER(dc.chunk_text) LIKE '%CM%'
            OR UPPER(dc.chunk_text) LIKE '%MILLIMETR%'
        THEN 'Size Restrictions'
        
        -- Protected Areas
        WHEN UPPER(dc.chunk_text) LIKE '%MARINE RESERVE%' 
            OR UPPER(dc.chunk_text) LIKE '%PROTECTED AREA%' 
            OR UPPER(dc.chunk_text) LIKE '%NO FISHING%'
            OR UPPER(dc.chunk_text) LIKE '%PROHIBITED%'
            OR UPPER(dc.chunk_text) LIKE '%RESTRICTED AREA%'
            OR UPPER(dc.chunk_text) LIKE '%MARINE PARK%'
        THEN 'Protected Areas'
        
        -- Fishing Methods
        WHEN UPPER(dc.chunk_text) LIKE '%FISHING METHOD%' 
            OR UPPER(dc.chunk_text) LIKE '%NET%' 
            OR UPPER(dc.chunk_text) LIKE '%HOOK%' 
            OR UPPER(dc.chunk_text) LIKE '%LINE%'
            OR UPPER(dc.chunk_text) LIKE '%LURE%'
            OR UPPER(dc.chunk_text) LIKE '%BAIT%'
            OR UPPER(dc.chunk_text) LIKE '%ROD%'
        THEN 'Fishing Methods'
        
        -- Seasonal Restrictions
        WHEN UPPER(dc.chunk_text) LIKE '%SEASON%' 
            OR UPPER(dc.chunk_text) LIKE '%CLOSURE%' 
            OR UPPER(dc.chunk_text) LIKE '%CLOSED PERIOD%'
            OR UPPER(dc.chunk_text) LIKE '%SPAWNING%'
            OR UPPER(dc.chunk_text) LIKE '%BREEDING%'
        THEN 'Seasonal Restrictions'
        
        -- Commercial Regulations
        WHEN UPPER(dc.chunk_text) LIKE '%COMMERCIAL%' 
            OR UPPER(dc.chunk_text) LIKE '%QUOTA%' 
            OR UPPER(dc.chunk_text) LIKE '%LICENCE%'
            OR UPPER(dc.chunk_text) LIKE '%PERMIT%'
            OR UPPER(dc.chunk_text) LIKE '%VESSEL%'
        THEN 'Commercial Regulations'
        
        -- Recreational Fishing
        WHEN UPPER(dc.chunk_text) LIKE '%RECREATIONAL%' 
            OR UPPER(dc.chunk_text) LIKE '%AMATEUR%'
            OR UPPER(dc.chunk_text) LIKE '%NON-COMMERCIAL%'
        THEN 'Recreational Fishing'
        
        -- Species-specific rules
        WHEN UPPER(dc.chunk_text) LIKE '%SNAPPER%' 
            OR UPPER(dc.chunk_text) LIKE '%KAHAWAI%'
            OR UPPER(dc.chunk_text) LIKE '%JOHN DORY%'
            OR UPPER(dc.chunk_text) LIKE '%GURNARD%'
            OR UPPER(dc.chunk_text) LIKE '%FLOUNDER%'
            OR UPPER(dc.chunk_text) LIKE '%BLUE COD%'
        THEN 'Species-Specific Rules'
        
        ELSE 'General Regulations'
    END as document_section,
    dc.nz_region,
    -- Extract keywords for enhanced search
    ARRAY_COMPACT(ARRAY_CONSTRUCT(
        CASE WHEN UPPER(dc.chunk_text) LIKE '%SNAPPER%' THEN 'snapper' END,
        CASE WHEN UPPER(dc.chunk_text) LIKE '%KAHAWAI%' THEN 'kahawai' END,
        CASE WHEN UPPER(dc.chunk_text) LIKE '%BAG LIMIT%' THEN 'bag-limit' END,
        CASE WHEN UPPER(dc.chunk_text) LIKE '%SIZE LIMIT%' THEN 'size-limit' END,
        CASE WHEN UPPER(dc.chunk_text) LIKE '%MARINE RESERVE%' THEN 'marine-reserve' END,
        CASE WHEN UPPER(dc.chunk_text) LIKE '%RECREATIONAL%' THEN 'recreational' END,
        CASE WHEN UPPER(dc.chunk_text) LIKE '%COMMERCIAL%' THEN 'commercial' END
    )) as keywords
FROM document_chunks dc;

-- Check chunking results
SELECT 
    document_section,
    COUNT(*) as chunks_per_section,
    ROUND(AVG(chunk_size_tokens), 1) as avg_tokens,
    ROUND(AVG(chunk_size_chars), 1) as avg_chars
FROM fishing_document_chunks
GROUP BY document_section
ORDER BY chunks_per_section DESC;

-- ================================================================
-- SECTION 6: PRODUCTION CORTEX SEARCH SERVICE
-- Create enterprise-ready search service for RAG applications
-- ================================================================

-- Drop existing search service if it exists
DROP CORTEX SEARCH SERVICE IF EXISTS fishing_regulations_search;

-- Create production-ready Cortex Search Service
CREATE OR REPLACE CORTEX SEARCH SERVICE fishing_regulations_search
    ON chunk_text
    ATTRIBUTES document_section, nz_region, file_name, keywords
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '10 minutes'  -- Fast refresh for production use
    EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'  -- High-quality multilingual model
    AS (
        SELECT 
            chunk_id,
            document_id,
            file_name,
            chunk_text,
            document_section,
            nz_region,
            ARRAY_TO_STRING(keywords, ',') as keywords,
            data_source
        FROM fishing_document_chunks
        WHERE LENGTH(TRIM(chunk_text)) > 20  -- Quality filter
    );

-- ================================================================
-- SECTION 7: SEARCH SERVICE VALIDATION & TESTING
-- Comprehensive testing to ensure production readiness
-- ================================================================

-- Test 1: Basic search functionality
SELECT 'Basic Search Test: Auckland Snapper Rules' AS test_name,
       SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
           'fishing_regulations_search',
           '{
               "query": "snapper bag limits Auckland",
               "columns": ["file_name", "chunk_text", "document_section", "nz_region"],
               "limit": 2
           }'
       ) AS search_results;

-- Test 2: Filtered search by section
SELECT 'Filtered Search Test: Size Restrictions' AS test_name,
       SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
           'fishing_regulations_search',
           '{
               "query": "minimum size requirements",
               "columns": ["file_name", "chunk_text", "nz_region"],
               "filter": {"@eq": {"document_section": "Size Restrictions"}},
               "limit": 2
           }'
       ) AS search_results;

-- Test 3: Regional search
SELECT 'Regional Search Test: Southland Rules' AS test_name,
       SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
           'fishing_regulations_search',
           '{
               "query": "daily limits fishing rules",
               "columns": ["file_name", "chunk_text", "document_section"],
               "filter": {"@eq": {"nz_region": "Southland"}},
               "limit": 2
           }'
       ) AS search_results;

-- ================================================================
-- SECTION 8: PRODUCTION ANALYTICS & MONITORING VIEWS
-- Create views for monitoring and analytics
-- ================================================================

-- Document processing analytics view
CREATE OR REPLACE VIEW fishing_document_analytics AS
SELECT 
    COUNT(*) as total_documents,
    COUNT(CASE WHEN processing_status = 'SUCCESS' THEN 1 END) as successful_documents,
    COUNT(CASE WHEN processing_status = 'FAILED' THEN 1 END) as failed_documents,
    ROUND(COUNT(CASE WHEN processing_status = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*), 2) as success_rate_percent,
    SUM(page_count) as total_pages_processed,
    ROUND(AVG(page_count), 1) as avg_pages_per_document,
    COUNT(DISTINCT nz_region) as regions_covered,
    SUM(document_size_bytes) as total_bytes_processed
FROM fishing_documents;

-- Chunk analytics view
CREATE OR REPLACE VIEW fishing_chunk_analytics AS
SELECT 
    COUNT(*) as total_chunks,
    COUNT(DISTINCT document_id) as documents_chunked,
    ROUND(AVG(chunk_size_tokens), 1) as avg_tokens_per_chunk,
    ROUND(AVG(chunk_size_chars), 1) as avg_chars_per_chunk,
    COUNT(DISTINCT document_section) as unique_sections,
    COUNT(DISTINCT nz_region) as regions_covered,
    document_section,
    COUNT(*) as chunks_per_section
FROM fishing_document_chunks
GROUP BY document_section
ORDER BY chunks_per_section DESC;

-- Regional coverage view
CREATE OR REPLACE VIEW fishing_regional_coverage AS
SELECT 
    nz_region,
    COUNT(DISTINCT fd.document_id) as documents,
    COUNT(fdc.chunk_id) as total_chunks,
    ROUND(AVG(fdc.chunk_size_tokens), 1) as avg_chunk_tokens,
    STRING_AGG(DISTINCT fd.file_name, ', ') as document_files
FROM fishing_documents fd
LEFT JOIN fishing_document_chunks fdc ON fd.document_id = fdc.document_id
WHERE fd.processing_status = 'SUCCESS'
GROUP BY nz_region
ORDER BY documents DESC, total_chunks DESC;

-- ================================================================
-- SECTION 9: SUCCESS CONFIRMATION & SUMMARY
-- ================================================================

SELECT '🚀 CORTEX SEARCH SERVICE IS NOW PRODUCTION-READY!' as final_status;

-- Summary statistics
SELECT 
    'FINAL PRODUCTION SUMMARY' as report_section,
    da.total_documents,
    da.successful_documents,
    da.success_rate_percent || '%' as success_rate,
    da.total_pages_processed,
    da.regions_covered
FROM fishing_document_analytics da;

SELECT 'CHUNK ANALYTICS' as report_section;
SELECT document_section, chunks_per_section, avg_tokens_per_chunk 
FROM fishing_chunk_analytics 
ORDER BY chunks_per_section DESC;

SELECT 'REGIONAL COVERAGE' as report_section;
SELECT * FROM fishing_regional_coverage;

-- ================================================================
-- SECTION 10: RAG APPLICATION EXAMPLES
-- Ready-to-use queries for building applications
-- ================================================================

-- Show how to use the search service for applications
SELECT 'RAG APPLICATION EXAMPLES' as section_name;

-- Example for Streamlit apps
SELECT 'Use this in your Streamlit apps:' as example_info,
       'SNOWFLAKE.CORTEX.SEARCH_PREVIEW(''fishing_regulations_search'', ''{"query": "user_question_here"}'')' as search_function;

-- ================================================================
-- END OF SCRIPT
-- Your fishing regulations are now AI-ready for semantic search!
-- Production-ready RAG system complete!
-- ================================================================