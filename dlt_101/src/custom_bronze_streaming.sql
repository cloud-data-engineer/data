-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Custom Bronze Layer Streaming DLT Pipeline
-- MAGIC 
-- MAGIC This template provides a customizable streaming DLT pipeline for JSON data ingestion.
-- MAGIC Modify the configuration section below to match your specific data source and requirements.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuration
-- MAGIC 
-- MAGIC Update these parameters for your specific use case:

-- COMMAND ----------

-- Configuration parameters (modify these for your use case)
-- Source path for JSON files
-- DECLARE OR REPLACE VARIABLE source_path STRING DEFAULT '/path/to/your/json/files/';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer - Raw Data Ingestion
-- MAGIC 
-- MAGIC Streams JSON files from the source location into a bronze layer table with data quality checks.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_json_raw (
  -- Data quality constraints
  CONSTRAINT valid_data EXPECT (_rescued_data IS NULL OR _rescued_data = '') ON VIOLATION DROP ROW,
  CONSTRAINT non_empty_record EXPECT (
    DOLocationID IS NOT NULL OR 
    PULocationID IS NOT NULL OR 
    fare_amount IS NOT NULL OR 
    total_amount IS NOT NULL
  ) ON VIOLATION DROP ROW
)
COMMENT "Bronze layer: Raw JSON data with streaming ingestion and data quality validation"
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "bronze",
  "delta.autoOptimize.optimizeWrite" = "true",
  "delta.autoOptimize.autoCompact" = "true"
)
AS SELECT 
  -- Original data
  *,
  
  -- Metadata columns
  current_timestamp() as ingestion_timestamp,
  _metadata.file_path as source_file_name,
  
  -- Extract file metadata
  regexp_extract(_metadata.file_path, '([^/]+)$', 1) as file_name,
  
  -- Data lineage
  'streaming_dlt_pipeline' as ingestion_method,
  
  -- Partition column (optional - useful for large datasets)
  date(current_timestamp()) as ingestion_date

FROM STREAM(
  read_files(
    "/databricks-datasets/nyctaxi/sample/json/", -- Replace with your JSON file path
    format => "json",
    header => "false",
    multiLine => "true",  -- Enable if your JSON spans multiple lines
    rescuedDataColumn => "_rescued_data"
  )
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Quality Monitoring
-- MAGIC 
-- MAGIC Creates monitoring tables to track data quality and ingestion metrics.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_ingestion_metrics
COMMENT "Ingestion metrics and data quality monitoring for bronze layer"
TBLPROPERTIES (
  "quality" = "monitoring"
)
AS SELECT 
  source_file_name,
  file_name,
  ingestion_date,
  
  -- Record counts
  count(*) as total_records,
  count_if(_rescued_data IS NOT NULL AND _rescued_data != '') as malformed_records,
  count(*) - count_if(_rescued_data IS NOT NULL AND _rescued_data != '') as valid_records,
  
  -- Quality metrics
  round((count(*) - count_if(_rescued_data IS NOT NULL AND _rescued_data != '')) / count(*) * 100, 2) as data_quality_percentage,
  
  -- Timing information
  min(ingestion_timestamp) as first_record_timestamp,
  max(ingestion_timestamp) as last_record_timestamp,
  
  -- File processing info
  count(distinct source_file_name) as files_processed

FROM LIVE.bronze_json_raw
GROUP BY source_file_name, file_name, ingestion_date;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Quality Alerts
-- MAGIC 
-- MAGIC Creates a view to identify potential data quality issues that need attention.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE bronze_data_quality_alerts
COMMENT "Data quality alerts for bronze layer monitoring"
AS SELECT 
  *,
  CASE 
    WHEN data_quality_percentage < 95 THEN 'HIGH'
    WHEN data_quality_percentage < 99 THEN 'MEDIUM'
    ELSE 'LOW'
  END as alert_severity,
  
  CASE 
    WHEN data_quality_percentage < 95 THEN 'Data quality below 95% - investigate malformed records'
    WHEN data_quality_percentage < 99 THEN 'Data quality below 99% - monitor for trends'
    ELSE 'Data quality acceptable'
  END as alert_message

FROM LIVE.bronze_ingestion_metrics
WHERE data_quality_percentage < 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Usage Instructions
-- MAGIC 
-- MAGIC ### To customize this pipeline:
-- MAGIC 
-- MAGIC 1. **Update the source path**: Replace `/databricks-datasets/nyctaxi/sample/json/` with your JSON file location
-- MAGIC 2. **Modify data quality constraints**: Adjust the `CONSTRAINT` clauses based on your data requirements
-- MAGIC 3. **Add schema validation**: Include specific column checks if you know your JSON schema
-- MAGIC 4. **Configure partitioning**: Uncomment and modify partition columns for large datasets
-- MAGIC 5. **Set up notifications**: Configure alerts in the pipeline YAML for data quality issues
-- MAGIC 
-- MAGIC ### Pipeline Features:
-- MAGIC 
-- MAGIC - **Streaming ingestion**: Automatically processes new JSON files
-- MAGIC - **Data quality validation**: Drops malformed records and tracks quality metrics
-- MAGIC - **Metadata enrichment**: Adds ingestion timestamps and source file tracking
-- MAGIC - **Monitoring**: Provides quality metrics and alerting capabilities
-- MAGIC - **Auto-optimization**: Enables Delta Lake optimizations for better performance
