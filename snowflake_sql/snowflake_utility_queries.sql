USE DATABASE weather_data_db;
USE SCHEMA raw_json_schema;

-- Check raw data table
SELECT * FROM RAW_JSON_SCHEMA.weather_raw_data ORDER BY inserted_at DESC LIMIT 20;

-- Check clean data table
SELECT * FROM weather_data_clean ORDER BY timestamp DESC LIMIT 10;
SELECT * FROM weather_data_clean;
SELECT *
FROM weather_data_clean
WHERE city IN ('Bangalore', 'Delhi', 'Mumbai')
ORDER BY timestamp DESC
LIMIT 3;

-- Check Snowpipe history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => 'WEATHER_RAW_DATA', START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())));

-- Manually refresh pipe (for re-triggering, if needed)
ALTER PIPE weather_pipe_sgp REFRESH;