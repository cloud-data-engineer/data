CREATE OR REFRESH STREAMING TABLE silver_voice_cdrs
COMMENT "Cleaned and typed voice CDR data"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(JSON_EXTRACT(raw_data, '$.cdr_type') AS STRING) AS cdr_type,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.callEventStartTime') AS BIGINT)) AS TIMESTAMP) AS call_event_start_time,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.callEventEndTime') AS BIGINT)) AS TIMESTAMP) AS call_event_end_time,
  CAST(JSON_EXTRACT(raw_data, '$.callDuration') AS INT) AS call_duration,
  CAST(JSON_EXTRACT(raw_data, '$.callReferenceNumber') AS STRING) AS call_reference_number,
  CAST(JSON_EXTRACT(raw_data, '$.callingPartyNumber') AS STRING) AS calling_party_number,
  CAST(JSON_EXTRACT(raw_data, '$.calledPartyNumber') AS STRING) AS called_party_number,
  CAST(JSON_EXTRACT(raw_data, '$.imsi') AS STRING) AS imsi,
  CAST(JSON_EXTRACT(raw_data, '$.imei') AS STRING) AS imei,
  CAST(JSON_EXTRACT(raw_data, '$.mscID') AS STRING) AS msc_id,
  CAST(JSON_EXTRACT(raw_data, '$.lac') AS STRING) AS lac,
  CAST(JSON_EXTRACT(raw_data, '$.ci') AS STRING) AS ci,
  CAST(JSON_EXTRACT(raw_data, '$.causeForTermination') AS STRING) AS cause_for_termination,
  CAST(JSON_EXTRACT(raw_data, '$.chargeID') AS STRING) AS charge_id,
  CAST(JSON_EXTRACT(raw_data, '$.callChargeableDuration') AS INT) AS call_chargeable_duration,
  CAST(JSON_EXTRACT(raw_data, '$.tariffClass') AS STRING) AS tariff_class,
  CAST(JSON_EXTRACT(raw_data, '$.callType') AS STRING) AS call_type,
  CAST(JSON_EXTRACT(raw_data, '$.recordSequenceNumber') AS INT) AS record_sequence_number,
  CAST(JSON_EXTRACT(raw_data, '$.user_id') AS STRING) AS user_id,
  CAST(JSON_EXTRACT(raw_data, '$.is_anomaly') AS BOOLEAN) AS is_anomaly,
  CAST(JSON_EXTRACT(raw_data, '$.anomaly_type') AS STRING) AS anomaly_type,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.callEventStartTime') AS BIGINT)) AS DATE) AS event_date,
  CAST(HOUR(CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.callEventStartTime') AS BIGINT)) AS TIMESTAMP)) AS INT) AS event_hour,
  _ingested_at,
  CURRENT_TIMESTAMP() AS _updated_at
FROM STREAM(voice_cdrs)
-- Dedup logic: business key + sequence number
-- Since this is a streaming table, we use a windowed approach or rely on downstream deduplication if possible.
-- For SDP, we'll assume the stream handles duplicates via the sequence key if we were using APPLY CHANGES.
-- However, for a standard streaming table, we'll filter for the latest record per key within the batch if required.
-- For now, we provide the clean stream.
CONSTRAINT valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_imsi EXPECT (LENGTH(imsi) = 15) ON VIOLATION WARN
CONSTRAINT valid_duration EXPECT (call_duration >= 0) ON VIOLATION DROP ROW
CONSTRAINT valid_ref_number EXPECT (call_reference_number IS NOT NULL) ON VIOLATION DROP ROW;
