-- Data Sharing Setup for NZ Partner Hackathon
-- Creates reusable functions and procedures for sharing database with participants

USE ROLE ACCOUNTADMIN;
USE DATABASE NZ_PARTNER_HACKATHON;

-- Create a stored procedure for setting up data sharing with participant accounts
CREATE OR REPLACE PROCEDURE setup_participant_data_share(
    participant_account_identifier STRING,
    participant_org_name STRING DEFAULT 'Hackathon Participant'
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    share_name STRING;
    result_message STRING;
BEGIN
    -- Generate share name based on participant account
    share_name := 'NZ_HACKATHON_SHARE_' || REPLACE(participant_account_identifier, '.', '_');
    
    -- Create the share
    EXECUTE IMMEDIATE 'CREATE OR REPLACE SHARE ' || share_name || ' COMMENT = ''NZ Partner Hackathon Data Share for ' || participant_org_name || '''';
    
    -- Grant database access to the share
    EXECUTE IMMEDIATE 'GRANT USAGE ON DATABASE NZ_PARTNER_HACKATHON TO SHARE ' || share_name;
    
    -- Grant schema access for all themes
    EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA NZ_PARTNER_HACKATHON.URU_RANGI TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA NZ_PARTNER_HACKATHON.TIPUANUKU TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA NZ_PARTNER_HACKATHON.WAITA TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA NZ_PARTNER_HACKATHON.WAIPUNA_RANGI TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA NZ_PARTNER_HACKATHON.HIWA_I_TE_RANGI TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT USAGE ON SCHEMA NZ_PARTNER_HACKATHON.FOUNDATIONAL TO SHARE ' || share_name;
    
    -- Grant SELECT on all existing tables and views in all schemas
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL TABLES IN SCHEMA NZ_PARTNER_HACKATHON.URU_RANGI TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.URU_RANGI TO SHARE ' || share_name;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL TABLES IN SCHEMA NZ_PARTNER_HACKATHON.TIPUANUKU TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.TIPUANUKU TO SHARE ' || share_name;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL TABLES IN SCHEMA NZ_PARTNER_HACKATHON.WAITA TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.WAITA TO SHARE ' || share_name;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL TABLES IN SCHEMA NZ_PARTNER_HACKATHON.WAIPUNA_RANGI TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.WAIPUNA_RANGI TO SHARE ' || share_name;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL TABLES IN SCHEMA NZ_PARTNER_HACKATHON.HIWA_I_TE_RANGI TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.HIWA_I_TE_RANGI TO SHARE ' || share_name;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL TABLES IN SCHEMA NZ_PARTNER_HACKATHON.FOUNDATIONAL TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON ALL VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.FOUNDATIONAL TO SHARE ' || share_name;
    
    -- Grant future SELECT privileges for new objects
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE TABLES IN SCHEMA NZ_PARTNER_HACKATHON.URU_RANGI TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.URU_RANGI TO SHARE ' || share_name;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE TABLES IN SCHEMA NZ_PARTNER_HACKATHON.TIPUANUKU TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.TIPUANUKU TO SHARE ' || share_name;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE TABLES IN SCHEMA NZ_PARTNER_HACKATHON.WAITA TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.WAITA TO SHARE ' || share_name;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE TABLES IN SCHEMA NZ_PARTNER_HACKATHON.WAIPUNA_RANGI TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.WAIPUNA_RANGI TO SHARE ' || share_name;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE TABLES IN SCHEMA NZ_PARTNER_HACKATHON.HIWA_I_TE_RANGI TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.HIWA_I_TE_RANGI TO SHARE ' || share_name;
    
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE TABLES IN SCHEMA NZ_PARTNER_HACKATHON.FOUNDATIONAL TO SHARE ' || share_name;
    EXECUTE IMMEDIATE 'GRANT SELECT ON FUTURE VIEWS IN SCHEMA NZ_PARTNER_HACKATHON.FOUNDATIONAL TO SHARE ' || share_name;
    
    -- Add the account to the share
    EXECUTE IMMEDIATE 'ALTER SHARE ' || share_name || ' ADD ACCOUNTS = (' || participant_account_identifier || ')';
    
    result_message := 'Successfully created share: ' || share_name || ' for account: ' || participant_account_identifier;
    RETURN result_message;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error creating share: ' || SQLERRM;
END;
$$;

-- Create a function to list all current data shares
CREATE OR REPLACE FUNCTION list_hackathon_shares()
RETURNS TABLE (
    share_name STRING,
    created_on TIMESTAMP,
    comment STRING,
    shared_accounts ARRAY
)
LANGUAGE SQL
AS
$$
    SELECT 
        name as share_name,
        created_on,
        comment,
        PARSE_JSON(to_accounts) as shared_accounts
    FROM INFORMATION_SCHEMA.OUTBOUND_SHARES 
    WHERE name LIKE 'NZ_HACKATHON_SHARE_%'
    ORDER BY created_on DESC
$$;

-- Create procedure to remove a participant's access
CREATE OR REPLACE PROCEDURE remove_participant_access(
    participant_account_identifier STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    share_name STRING;
    result_message STRING;
BEGIN
    share_name := 'NZ_HACKATHON_SHARE_' || REPLACE(participant_account_identifier, '.', '_');
    
    -- Drop the share (this removes all access)
    EXECUTE IMMEDIATE 'DROP SHARE IF EXISTS ' || share_name;
    
    result_message := 'Successfully removed share: ' || share_name || ' for account: ' || participant_account_identifier;
    RETURN result_message;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error removing share: ' || SQLERRM;
END;
$$;

-- Example usage and helper queries
/*
-- EXAMPLE USAGE:

-- 1. Setup data sharing for a participant
CALL setup_participant_data_share('abc12345.ap-southeast-2.aws', 'Team Alpha');

-- 2. Setup sharing for multiple participants
CALL setup_participant_data_share('xyz67890.us-east-1.aws', 'Team Beta');
CALL setup_participant_data_share('def11111.eu-west-1.aws', 'Team Gamma');

-- 3. List all current shares
SELECT * FROM TABLE(list_hackathon_shares());

-- 4. Remove access for a participant
CALL remove_participant_access('abc12345.ap-southeast-2.aws');

-- 5. Quick setup for trial accounts (common pattern)
-- Trial accounts usually follow format: orgname-randomid.region.cloud
CALL setup_participant_data_share('hackathon-trial123.ap-southeast-2.aws', 'Trial Team 1');

*/

-- Create a view to monitor share usage
CREATE OR REPLACE VIEW share_usage_monitoring AS
SELECT 
    s.name as share_name,
    s.created_on as share_created,
    s.comment as participant_info,
    PARSE_JSON(s.to_accounts) as shared_accounts,
    -- Add query history if needed
    'Active' as status
FROM INFORMATION_SCHEMA.OUTBOUND_SHARES s
WHERE s.name LIKE 'NZ_HACKATHON_SHARE_%'
ORDER BY s.created_on DESC;

-- Instructions for participants (to be shared with them)
/*
PARTICIPANT INSTRUCTIONS:

Once you receive access to the shared database, you can:

1. Create your database from the share:
   CREATE DATABASE nz_hackathon_data FROM SHARE <your_provider_account>.NZ_HACKATHON_SHARE_<your_account>;

2. Explore the themes:
   USE DATABASE nz_hackathon_data;
   SHOW SCHEMAS;

3. Start with electricity data:
   USE SCHEMA uru_rangi;
   SHOW TABLES;
   SHOW VIEWS;

4. Sample query to get started:
   SELECT * FROM electricity_daily_summary LIMIT 10;

5. Available themes and schemas:
   - URU_RANGI: Wind/Energy data (electricity, renewables)
   - TIPUANUKU: Food/Agriculture data  
   - WAITA: Ocean/Marine data
   - WAIPUNA_RANGI: Rain/Water data
   - HIWA_I_TE_RANGI: Travel/Tourism data
   - FOUNDATIONAL: Population, economic data
*/

-- Grant execution permissions on the procedures
GRANT USAGE ON PROCEDURE setup_participant_data_share(STRING, STRING) TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE remove_participant_access(STRING) TO ROLE SYSADMIN;
GRANT USAGE ON FUNCTION list_hackathon_shares() TO ROLE SYSADMIN;

SELECT 'Data sharing setup complete! Use the procedures above to manage participant access.' as status;