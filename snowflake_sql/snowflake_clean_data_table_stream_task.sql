USE DATABASE weather_data_db;
USE SCHEMA raw_json_schema;

-- Create/Replace Clean Data Table (Updated with all fields)
CREATE OR REPLACE TABLE weather_data_clean AS
SELECT
    raw_json:city::STRING AS city,
    raw_json:timestamp::TIMESTAMP AS timestamp,
    raw_json:temperature::NUMBER AS temperature,
    raw_json:feels_like::NUMBER AS feels_like,
    raw_json:humidity::NUMBER AS humidity,
    raw_json:pressure::NUMBER AS pressure,
    raw_json:wind_speed::NUMBER AS wind_speed,
    raw_json:wind_gust::NUMBER AS wind_gust,
    raw_json:cloud_coverage::NUMBER AS cloud_coverage,
    raw_json:weather_main::STRING AS weather_main,
    raw_json:weather_desc::STRING AS weather_desc,
    raw_json:sunrise::TIMESTAMP_NTZ AS sunrise,
    raw_json:sunset::TIMESTAMP_NTZ AS sunset,
    raw_json::VARIANT AS raw_data_json -- Keep the full raw JSON for flexibility
FROM RAW_JSON_SCHEMA.weather_raw_data
WHERE raw_json IS NOT NULL;

-- Create/Replace Stream on Raw Data Table
CREATE OR REPLACE STREAM weather_data_stream ON TABLE weather_raw_data;

-- Create/Replace Task to insert new data into clean table from stream
CREATE OR REPLACE TASK insert_clean_data
  WAREHOUSE = 'COMPUTE_WH' -- Use your actual warehouse name here
  SCHEDULE = '5 minutes'
AS
  INSERT INTO weather_data_clean (
    city,
    timestamp,
    temperature,
    feels_like,
    humidity,
    pressure,
    wind_speed,
    wind_gust,
    cloud_coverage,
    weather_main,
    weather_desc,
    sunrise,
    sunset,
    raw_data_json
  )
  SELECT
    raw_json:city::STRING,
    raw_json:timestamp::TIMESTAMP,
    raw_json:temperature::NUMBER,
    raw_json:feels_like::NUMBER,
    raw_json:humidity::NUMBER,
    raw_json:pressure::NUMBER,
    raw_json:wind_speed::NUMBER,
    raw_json:wind_speed::NUMBER, -- Corrected from wind_gust
    raw_json:cloud_coverage::NUMBER,
    raw_json:weather_main::STRING,
    raw_json:weather_desc::STRING,
    raw_json:sunrise::TIMESTAMP_NTZ,
    raw_json:sunset::TIMESTAMP_NTZ,
    raw_json::VARIANT
  FROM weather_data_stream
  WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = FALSE;

-- Activate the task
ALTER TASK insert_clean_data RESUME;