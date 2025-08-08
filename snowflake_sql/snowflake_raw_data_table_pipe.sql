USE DATABASE weather_data_db;
USE SCHEMA raw_json_schema;

-- Create/Replace Raw Data Table
CREATE OR REPLACE TABLE weather_raw_data (
  raw_json VARIANT,
  filename STRING,
  row_number NUMBER,
  inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Create/Replace Snowpipe
CREATE OR REPLACE PIPE weather_pipe_sgp
  AUTO_INGEST = TRUE
  AS
  COPY INTO weather_raw_data(raw_json, filename, row_number)
  FROM (
    SELECT 
      $1, 
      METADATA$FILENAME, 
      METADATA$FILE_ROW_NUMBER 
    FROM @weather_stage_sgp
  )
  FILE_FORMAT = (TYPE = 'JSON')
  PATTERN = '.*\.json';

SHOW PIPES;