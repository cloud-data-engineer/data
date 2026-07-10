CREATE OR REFRESH STREAMING TABLE silver_ims_cdrs
COMMENT "Cleaned and typed IMS CDR data"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(JSON_EXTRACT(raw_data, '$.cdr_type') AS STRING) AS cdr_type,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.sessionStartTime') AS BIGINT)) AS TIMESTAMP) AS session_start_time,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.sessionEndTime') AS BIGINT)) AS TIMESTAMP) AS session_end_time,
  CAST(JSON_EXTRACT(raw_data, '$.sessionDuration') AS INT) AS session_duration,
  CAST(JSON_EXTRACT(raw_data, '$.originatingURI') AS STRING) AS originating_uri,
  CAST(JSON_EXTRACT(raw_data, '$.destinationURI') AS STRING) AS destination_uri,
  CAST(JSON_EXTRACT(raw_data, '$.serviceType') AS STRING) AS service_type,
  CAST(JSON_EXTRACT(raw_data, '$.mediaTypes') AS STRING) AS media_types,
  CAST(JSON_EXTRACT(raw_data, '$.networkType') AS STRING) AS network_type,
  CAST(JSON_EXTRACT(raw_data, '$.qosClass') AS STRING) AS qos_class,
  CAST(JSON_EXTRACT(raw_data, '$.chargeID') AS STRING) AS charge_id,
  CAST(JSON_EXTRACT(raw_data, '$.user_id') AS STRING) AS user_id,
  CAST(JSON_EXTRACT(raw_data, '$.is_anomaly') AS BOOLEAN) AS is_anomaly,
  CAST(JSON_EXTRACT(raw_data, '$.anomaly_type') AS STRING) AS anomaly_type,
  CAST(JSON_EXTRACT(raw_data, '$.terminationReason') AS STRING) AS termination_reason,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.sessionStartTime') AS BIGINT)) AS DATE) AS event_date,
  CAST(HOUR(CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.sessionStartTime') AS BIGINT)) AS TIMESTAMP)) AS INT) AS event_hour,
  _ingested_at,
  CURRENT_TIMESTAMP() AS _updated_at
FROM STREAM(ims_cdrs)
CONSTRAINT valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_charge_id EXPECT (charge_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_duration EXPECT (session_duration >= 0) ON VIOLATION DROP ROW;
