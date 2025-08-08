-- Create/Replace Database and Schema
CREATE OR REPLACE DATABASE weather_data_db;
USE DATABASE weather_data_db;
CREATE OR REPLACE SCHEMA raw_json_schema;
USE SCHEMA raw_json_schema;

-- Create/Replace External Stage
CREATE OR REPLACE STAGE weather_stage_sgp
  URL = 's3://weather-raw-json-namitha-01/'
  STORAGE_INTEGRATION = S3_INT_WEATHER_SGP
  FILE_FORMAT = (TYPE = 'JSON');

DESC STAGE weather_stage_sgp;
LIST @weather_stage_sgp; -- Useful for checking staged files
