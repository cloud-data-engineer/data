-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer Streaming DLT Pipeline
-- MAGIC 
-- MAGIC This Delta Live Tables (DLT) pipeline reads JSON data from a file source and writes it to a bronze layer table using streaming.
-- MAGIC The bronze layer typically contains raw, unprocessed data with minimal transformations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer Table
-- MAGIC 
-- MAGIC Creates a streaming bronze table that reads JSON files from the specified path.
-- MAGIC This table will automatically process new files as they arrive in the source location.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_raw_data (
  CONSTRAINT valid_timestamp EXPECT (_rescued_data IS NULL OR _rescued_data = '') ON VIOLATION DROP ROW
)
COMMENT "Bronze layer table containing raw JSON data with streaming ingestion"
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "bronze"
)
AS SELECT 
  *,
  current_timestamp() as ingestion_timestamp,
  _metadata.file_path as source_file_name
FROM STREAM(
  read_files(
    "/databricks-datasets/nyctaxi/sample/json/",
    format => "json",
    header => "false"
  )
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Quality View
-- MAGIC 
-- MAGIC Creates a view to monitor data quality and provide insights into the bronze layer data.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_data_quality
COMMENT "Data quality metrics for bronze layer"
AS SELECT 
  source_file_name,
  count(*) as record_count,
  count_if(_rescued_data IS NOT NULL AND _rescued_data != '') as malformed_records,
  min(ingestion_timestamp) as first_ingestion,
  max(ingestion_timestamp) as last_ingestion
FROM LIVE.bronze_raw_data
GROUP BY source_file_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuration Notes
-- MAGIC 
-- MAGIC - **Streaming**: Uses `STREAMING LIVE TABLE` for continuous processing
-- MAGIC - **Data Quality**: Includes constraint to handle malformed records
-- MAGIC - **Metadata**: Adds ingestion timestamp and source file tracking
-- MAGIC - **Monitoring**: Includes quality metrics table for observability
-- MAGIC 
-- MAGIC To customize for your specific JSON source:
-- MAGIC 1. Update the file path in the `read_files()` function
-- MAGIC 2. Adjust data quality constraints based on your data schema
-- MAGIC 3. Add additional metadata columns as needed
