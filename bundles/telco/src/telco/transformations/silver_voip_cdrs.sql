CREATE OR REFRESH STREAMING TABLE silver_voip_cdrs
COMMENT "Cleaned and typed VoIP CDR data"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(JSON_EXTRACT(raw_data, '$.cdr_type') AS STRING) AS cdr_type,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.sessionStartTime') AS BIGINT)) AS TIMESTAMP) AS session_start_time,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.sessionEndTime') AS BIGINT)) AS TIMESTAMP) AS session_end_time,
  CAST(JSON_EXTRACT(raw_data, '$.sessionDuration') AS INT) AS session_duration,
  CAST(JSON_EXTRACT(raw_data, '$.callingPartyURI') AS STRING) AS calling_party_uri,
  CAST(JSON_EXTRACT(raw_data, '$.calledPartyURI') AS STRING) AS called_party_uri,
  CAST(JSON_EXTRACT(raw_data, '$.codec') AS STRING) AS codec,
  CAST(JSON_EXTRACT(raw_data, '$.callQualityIndicator') AS INT) AS call_quality_indicator,
  CAST(JSON_EXTRACT(raw_data, '$.bytesTransferred') AS BIGINT) AS bytes_transferred,
  CAST(JSON_EXTRACT(raw_data, '$.packetLoss') AS DECIMAL(18,4)) AS packet_loss,
  CAST(JSON_EXTRACT(raw_data, '$.jitter') AS DECIMAL(18,4)) AS jitter,
  CAST(JSON_EXTRACT(raw_data, '$.latency') AS DECIMAL(18,4)) AS latency,
  CAST(JSON_EXTRACT(raw_data, '$.chargeID') AS STRING) AS charge_id,
  CAST(JSON_EXTRACT(raw_data, '$.user_id') AS STRING) AS user_id,
  CAST(JSON_EXTRACT(raw_data, '$.is_anomaly') AS BOOLEAN) AS is_anomaly,
  CAST(JSON_EXTRACT(raw_data, '$.anomaly_type') AS STRING) AS anomaly_type,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.sessionStartTime') AS BIGINT)) AS DATE) AS event_date,
  CAST(HOUR(CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.sessionStartTime') AS BIGINT)) AS TIMESTAMP)) AS INT) AS event_hour,
  _ingested_at,
  CURRENT_TIMESTAMP() AS _updated_at
FROM STREAM(voip_cdrs)
CONSTRAINT valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_charge_id EXPECT (charge_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_duration EXPECT (session_duration >= 0) ON VIOLATION DROP ROW;
