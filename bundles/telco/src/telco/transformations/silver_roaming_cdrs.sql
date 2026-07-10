CREATE OR REFRESH STREAMING TABLE silver_roaming_cdrs
COMMENT "Cleaned and typed roaming CDR data"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(JSON_EXTRACT(raw_data, '$.cdr_type') AS STRING) AS cdr_type,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.callEventStartTime') AS BIGINT)) AS TIMESTAMP) AS call_event_start_time,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.callEventEndTime') AS BIGINT)) AS TIMESTAMP) AS call_event_end_time,
  CAST(JSON_EXTRACT(raw_data, '$.callDuration') AS INT) AS call_duration,
  CAST(JSON_EXTRACT(raw_data, '$.callingPartyNumber') AS STRING) AS calling_party_number,
  CAST(JSON_EXTRACT(raw_data, '$.calledPartyNumber') AS STRING) AS called_party_number,
  CAST(JSON_EXTRACT(raw_data, '$.imsi') AS STRING) AS imsi,
  CAST(JSON_EXTRACT(raw_data, '$.imei') AS STRING) AS imei,
  CAST(JSON_EXTRACT(raw_data, '$.roamingCountry') AS STRING) AS roaming_country,
  CAST(JSON_EXTRACT(raw_data, '$.visitedNetwork') AS STRING) AS visited_network,
  CAST(JSON_EXTRACT(raw_data, '$.mcc') AS STRING) AS mcc,
  CAST(JSON_EXTRACT(raw_data, '$.mnc') AS STRING) AS mnc,
  CAST(JSON_EXTRACT(raw_data, '$.roamingType') AS STRING) AS roaming_type,
  CAST(JSON_EXTRACT(raw_data, '$.serviceType') AS STRING) AS service_type,
  CAST(JSON_EXTRACT(raw_data, '$.chargeAmount') AS DECIMAL(18,2)) AS charge_amount,
  CAST(JSON_EXTRACT(raw_data, '$.roamingStatus') AS STRING) AS roaming_status,
  CAST(JSON_EXTRACT(raw_data, '$.user_id') AS STRING) AS user_id,
  CAST(JSON_EXTRACT(raw_data, '$.is_anomaly') AS BOOLEAN) AS is_anomaly,
  CAST(JSON_EXTRACT(raw_data, '$.anomaly_type') AS STRING) AS anomaly_type,
  CAST(JSON_EXTRACT(raw_data, '$.suspiciousActivity') AS BOOLEAN) AS suspicious_activity,
  CAST(JSON_EXTRACT(raw_data, '$.fraudScore') AS DECIMAL(5,4)) AS fraud_score,
  CAST(JSON_EXTRACT(raw_data, '$.connectionAllowed') AS BOOLEAN) AS connection_allowed,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.callEventStartTime') AS BIGINT)) AS DATE) AS event_date,
  CAST(HOUR(CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.callEventStartTime') AS BIGINT)) AS TIMESTAMP)) AS INT) AS event_hour,
  _ingested_at,
  CURRENT_TIMESTAMP() AS _updated_at
FROM STREAM(roaming_cdrs)
CONSTRAINT valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_imsi EXPECT (LENGTH(imsi) = 15) ON VIOLATION WARN
CONSTRAINT valid_charge_id EXPECT (charge_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_fraud_score EXPECT (fraud_score BETWEEN 0 AND 1) ON VIOLATION DROP ROW;
