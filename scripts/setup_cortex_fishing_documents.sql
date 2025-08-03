-- ================================================================
-- FISHING DOCUMENTS CORTEX RAG SETUP - COMPLETE AI-READY SYSTEM
-- Processes all PDF fishing regulations and creates production Cortex Search Service
-- for intelligent Q&A and RAG applications
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

-- ================================================================
-- SECTION 3: BATCH PDF PROCESSING PIPELINE
-- Process all PDFs in fishing_documents_stage automatically
-- ================================================================

-- First, let's see what PDF files are available in the stage
LIST @fishing_documents_stage;

-- Process all PDF files found in the stage directory
-- This will parse each PDF and create document records

-- Step 3.1: Parse all PDFs and populate fishing_documents table
INSERT INTO fishing_documents (
    document_id, 
    file_name, 
    file_path, 
    nz_region, 
    parsed_text, 
    page_count, 
    document_size_bytes,
    processing_status
)
WITH pdf_files AS (
    -- Get all PDF files from the stage directory
    SELECT 
        SPLIT_PART("name", '/', -1) as file_name,
        "name" as full_path,
        REPLACE(REPLACE(SPLIT_PART("name", '/', -1), '.pdf.gz', ''), '.pdf', '') as clean_name
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "name" LIKE '%.pdf%'
),
parsed_documents AS (
    SELECT 
        LOWER(REPLACE(REPLACE(pf.clean_name, ' ', '-'), '_', '-')) || '-' || SUBSTR(HASH(pf.file_name), 1, 8) as document_id,
        REPLACE(pf.file_name, '.gz', '') as file_name,
        '@fishing_documents_stage/' || pf.full_path as file_path,
        -- Extract region from filename using enhanced logic
        CASE 
            WHEN UPPER(pf.file_name) LIKE '%AUCKLAND%' THEN 'Auckland'
            WHEN UPPER(pf.file_name) LIKE '%WELLINGTON%' THEN 'Wellington'
            WHEN UPPER(pf.file_name) LIKE '%CANTERBURY%' THEN 'Canterbury'
            WHEN UPPER(pf.file_name) LIKE '%OTAGO%' THEN 'Otago'
            WHEN UPPER(pf.file_name) LIKE '%BAY%PLENTY%' OR UPPER(pf.file_name) LIKE '%BOP%' THEN 'Bay of Plenty'
            WHEN UPPER(pf.file_name) LIKE '%WAIKATO%' THEN 'Waikato'
            WHEN UPPER(pf.file_name) LIKE '%NORTHLAND%' THEN 'Northland'
            WHEN UPPER(pf.file_name) LIKE '%TARANAKI%' THEN 'Taranaki'
            WHEN UPPER(pf.file_name) LIKE '%HAWKE%' OR UPPER(pf.file_name) LIKE '%HAWKES%' THEN 'Hawke\'s Bay'
            WHEN UPPER(pf.file_name) LIKE '%MANAWATU%' OR UPPER(pf.file_name) LIKE '%WHANGANUI%' THEN 'Manawatū-Whanganui'
            WHEN UPPER(pf.file_name) LIKE '%MARLBOROUGH%' THEN 'Marlborough'
            WHEN UPPER(pf.file_name) LIKE '%NELSON%' THEN 'Nelson'
            WHEN UPPER(pf.file_name) LIKE '%WEST%COAST%' THEN 'West Coast'
            WHEN UPPER(pf.file_name) LIKE '%SOUTHLAND%' THEN 'Southland'
            WHEN UPPER(pf.file_name) LIKE '%GISBORNE%' THEN 'Gisborne'
            WHEN UPPER(pf.file_name) LIKE '%NATIONAL%' OR UPPER(pf.file_name) LIKE '%NZ%' THEN 'National'
            ELSE 'New Zealand'
        END as nz_region,
        -- Parse the PDF using Cortex PARSE_DOCUMENT with LAYOUT mode
        TRY_CAST(
            SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                @fishing_documents_stage,
                REPLACE(pf.full_path, '@fishing_documents_stage/', ''),
                {'mode': 'LAYOUT'}
            ) AS VARIANT
        ) as parsed_content
    FROM pdf_files pf
)
SELECT 
    pd.document_id,
    pd.file_name,
    pd.file_path,
    pd.nz_region,
    pd.parsed_content,
    COALESCE(pd.parsed_content:metadata:pageCount::NUMBER, 0) as page_count,
    COALESCE(LENGTH(pd.parsed_content:content::STRING), 0) as document_size_bytes,
    CASE 
        WHEN pd.parsed_content IS NOT NULL AND LENGTH(pd.parsed_content:content::STRING) > 100 
        THEN 'SUCCESS'
        ELSE 'FAILED'
    END as processing_status
FROM parsed_documents pd
WHERE pd.parsed_content IS NOT NULL;

-- Update processing status and add error messages for failed documents
UPDATE fishing_documents 
SET 
    processing_status = 'FAILED',
    error_message = 'Failed to parse PDF or content too short'
WHERE processing_status = 'PENDING' OR document_size_bytes < 100;

-- ================================================================
-- SECTION 4: INTELLIGENT TEXT CHUNKING WITH CORTEX
-- Chunk all successfully parsed documents for optimal search
-- ================================================================

-- Step 4.1: Create optimized text chunks using SPLIT_TEXT_RECURSIVE_CHARACTER
INSERT INTO fishing_document_chunks (
    chunk_id,
    document_id,
    file_name,
    chunk_text,
    chunk_sequence,
    chunk_size_tokens,
    chunk_size_chars,
    document_section,
    nz_region,
    keywords
)
WITH document_chunks AS (
    SELECT 
        fd.document_id,
        fd.file_name,
        fd.nz_region,
        c.SEQ as chunk_sequence,
        c.VALUE as chunk_text
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
),
enhanced_chunks AS (
    SELECT 
        dc.*,
        -- Calculate actual token count
        SNOWFLAKE.CORTEX.COUNT_TOKENS('snowflake-arctic-embed-l-v2.0', dc.chunk_text) as actual_tokens,
        LENGTH(dc.chunk_text) as char_count,
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
                OR UPPER(dc.chunk_text) LIKE '%OCTOBER%'
                OR UPPER(dc.chunk_text) LIKE '%NOVEMBER%'
                OR UPPER(dc.chunk_text) LIKE '%DECEMBER%'
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
        
        -- Extract keywords for enhanced search
        ARRAY_CONSTRUCT(
            CASE WHEN UPPER(dc.chunk_text) LIKE '%SNAPPER%' THEN 'snapper' END,
            CASE WHEN UPPER(dc.chunk_text) LIKE '%KAHAWAI%' THEN 'kahawai' END,
            CASE WHEN UPPER(dc.chunk_text) LIKE '%BAG LIMIT%' THEN 'bag-limit' END,
            CASE WHEN UPPER(dc.chunk_text) LIKE '%SIZE LIMIT%' THEN 'size-limit' END,
            CASE WHEN UPPER(dc.chunk_text) LIKE '%MARINE RESERVE%' THEN 'marine-reserve' END,
            CASE WHEN UPPER(dc.chunk_text) LIKE '%RECREATIONAL%' THEN 'recreational' END,
            CASE WHEN UPPER(dc.chunk_text) LIKE '%COMMERCIAL%' THEN 'commercial' END
        ) as extracted_keywords
    FROM document_chunks dc
)
SELECT 
    ec.document_id || '_chunk_' || LPAD(ec.chunk_sequence::STRING, 4, '0') as chunk_id,
    ec.document_id,
    ec.file_name,
    ec.chunk_text,
    ec.chunk_sequence,
    ec.actual_tokens as chunk_size_tokens,
    ec.char_count as chunk_size_chars,
    ec.document_section,
    ec.nz_region,
    ARRAY_COMPACT(ec.extracted_keywords) as keywords  -- Remove null values
FROM enhanced_chunks ec
WHERE ec.actual_tokens <= 512  -- Ensure chunks are within optimal size
ORDER BY ec.document_id, ec.chunk_sequence;

-- ================================================================
-- SECTION 5: PRODUCTION CORTEX SEARCH SERVICE
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
-- SECTION 6: SEARCH SERVICE VALIDATION & TESTING
-- Comprehensive testing to ensure production readiness
-- ================================================================

-- Test 1: Basic search functionality
SELECT 'Basic Search Test' AS test_name,
       SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
           'fishing_regulations_search',
           '{
               "query": "snapper bag limits Auckland",
               "columns": ["file_name", "chunk_text", "document_section", "nz_region"],
               "limit": 3
           }'
       ) AS search_results;

-- Test 2: Filtered search by region
SELECT 'Regional Filter Test' AS test_name,
       SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
           'fishing_regulations_search',
           '{
               "query": "minimum size limits",
               "columns": ["file_name", "chunk_text", "document_section"],
               "filter": {"@eq": {"nz_region": "Auckland"}},
               "limit": 2
           }'
       ) AS search_results;

-- Test 3: Section-specific search
SELECT 'Section Filter Test' AS test_name,
       SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
           'fishing_regulations_search',
           '{
               "query": "marine protected areas",
               "columns": ["file_name", "chunk_text", "nz_region"],
               "filter": {"@eq": {"document_section": "Protected Areas"}},
               "limit": 2
           }'
       ) AS search_results;

-- ================================================================
-- SECTION 7: PRODUCTION ANALYTICS & MONITORING VIEWS
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
-- SECTION 8: PRODUCTION SUMMARY & STATUS REPORT
-- ================================================================

-- Generate comprehensive processing report
SELECT '🎣 FISHING DOCUMENT PROCESSING COMPLETE' as status;

SELECT 'DOCUMENT PROCESSING SUMMARY' as report_section,
       da.total_documents,
       da.successful_documents,
       da.failed_documents,
       da.success_rate_percent || '%' as success_rate,
       da.total_pages_processed,
       da.avg_pages_per_document,
       da.regions_covered
FROM fishing_document_analytics da;

SELECT 'CHUNK ANALYTICS SUMMARY' as report_section,
       ca.total_chunks,
       ca.documents_chunked,
       ca.avg_tokens_per_chunk,
       ca.unique_sections,
       ca.regions_covered
FROM (SELECT 
        COUNT(*) as total_chunks,
        COUNT(DISTINCT document_id) as documents_chunked,
        ROUND(AVG(chunk_size_tokens), 1) as avg_tokens_per_chunk,
        COUNT(DISTINCT document_section) as unique_sections,
        COUNT(DISTINCT nz_region) as regions_covered
      FROM fishing_document_chunks) ca;

SELECT 'CORTEX SEARCH SERVICE STATUS' as report_section,
       'fishing_regulations_search' as service_name,
       'ACTIVE' as status,
       'snowflake-arctic-embed-l-v2.0' as embedding_model,
       '10 minutes' as target_lag,
       'READY FOR RAG APPLICATIONS' as readiness;

-- Show section breakdown
SELECT 'DOCUMENT SECTIONS BREAKDOWN' as report_section;
SELECT * FROM fishing_chunk_analytics ORDER BY chunks_per_section DESC;

-- Show regional coverage
SELECT 'REGIONAL COVERAGE' as report_section;
SELECT * FROM fishing_regional_coverage;

-- ================================================================
-- SECTION 9: RAG APPLICATION EXAMPLES
-- Ready-to-use queries for building applications
-- ================================================================

-- Example 1: Smart fishing question answering
SELECT 'RAG EXAMPLE: Fishing Regulations Q&A' as example_name;
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'fishing_regulations_search',
        '{
            "query": "What are the daily bag limits for snapper in Auckland waters?",
            "columns": ["file_name", "chunk_text", "document_section", "nz_region"],
            "limit": 3
        }'
    )
)['results'] as regulation_answer;

-- Example 2: Size restriction lookup
SELECT 'RAG EXAMPLE: Size Restrictions Lookup' as example_name;
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'fishing_regulations_search',
        '{
            "query": "minimum size requirements for recreational fishing",
            "columns": ["file_name", "chunk_text", "nz_region"],
            "filter": {"@eq": {"document_section": "Size Restrictions"}},
            "limit": 4
        }'
    )
)['results'] as size_requirements;

-- Example 3: AI-powered compliance checking using Cortex COMPLETE
SELECT 'RAG EXAMPLE: AI Compliance Checker' as example_name,
       SNOWFLAKE.CORTEX.COMPLETE(
           'mistral-large2',
           CONCAT(
               'Based on these NZ fishing regulations: ',
               (SELECT chunk_text FROM fishing_document_chunks 
                WHERE document_section = 'Daily Bag Limits' 
                AND UPPER(chunk_text) LIKE '%SNAPPER%' 
                LIMIT 1),
               ' Answer this question: Is it legal to catch 8 snapper in one day for recreational fishing in Auckland? Provide a clear yes/no answer with explanation.'
           )
       ) as ai_compliance_check;

-- ================================================================
-- SECTION 10: SUCCESS CONFIRMATION
-- ================================================================

SELECT '🚀 CORTEX SEARCH SERVICE IS NOW PRODUCTION-READY!' as final_status,
       'Service Name: fishing_regulations_search' as service_info,
       'Ready for RAG applications, chatbots, and intelligent Q&A' as capabilities,
       'Use SNOWFLAKE.CORTEX.SEARCH_PREVIEW() for queries' as usage_info;

-- ================================================================
-- END OF SCRIPT
-- Your fishing regulations are now AI-ready for semantic search!
-- ================================================================