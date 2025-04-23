USE ROLE ACCOUNTADMIN;

CREATE OR ALTER WAREHOUSE DEVOPS_WH 
  WAREHOUSE_SIZE = XSMALL 
  AUTO_SUSPEND = 120 
  AUTO_RESUME= TRUE;


-- Separate database for git repository
CREATE OR ALTER DATABASE DEVOPS_COMMON;


-- API integration is needed for GitHub integration
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/nileshvarshney')
  ENABLED = TRUE;

-- Git repository object is similar to external stage
CREATE OR REPLACE GIT REPOSITORY devops_common.public.snowflake_data_engineering
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/nileshvarshney/snowflake_data_engineering'; 


CREATE OR ALTER DATABASE production_db;


-- To monitor data pipeline's completion
CREATE OR REPLACE NOTIFICATION INTEGRATION email_integration
  TYPE=EMAIL
  ENABLED=TRUE;


-- Database level objects
CREATE OR ALTER SCHEMA sales;
CREATE OR ALTER SCHEMA accounting;
CREATE OR ALTER SCHEMA revenue;


-- Schema level objects
CREATE OR REPLACE FILE FORMAT sales.json_format TYPE = 'json';
CREATE OR ALTER STAGE sales.raw;


-- Copy file from GitHub to internal stage
copy files into @bronze.raw from @quickstart_common.public.quickstart_repo/branches/main/data/airport_list.json;