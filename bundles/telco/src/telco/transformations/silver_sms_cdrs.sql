CREATE OR REFRESH STREAMING TABLE silver_sms_cdrs
COMMENT "Cleaned and typed SMS CDR data"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(JSON_EXTRACT(raw_data, '$.cdr_type') AS STRING) AS cdr_type,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.eventTimestamp') AS BIGINT)) AS TIMESTAMP) AS event_timestamp,
  CAST(JSON_EXTRACT(raw_data, '$.smsReferenceNumber') AS STRING) AS sms_reference_number,
  CAST(JSON_EXTRACT(raw_data, '$.originatingNumber') AS STRING) AS originating_number,
  CAST(JSON_EXTRACT(raw_data, '$.destinationNumber') AS STRING) AS destination_number,
  CAST(JSON_EXTRACT(raw_data, '$.imsi') AS STRING) AS imsi,
  CAST(JSON_EXTRACT(raw_data, '$.smscAddress') AS STRING) AS smsc_address,
  CAST(JSON_EXTRACT(raw_data, '$.messageSize') AS INT) AS message_size,
  CAST(JSON_EXTRACT(raw_data, '$.deliveryStatus') AS STRING) AS delivery_status,
  CAST(JSON_EXTRACT(raw_data, '$.messageType') AS STRING) AS message_type,
  CAST(JSON_EXTRACT(raw_data, '$.recordSequenceNumber') AS INT) AS record_sequence_number,
  CAST(JSON_EXTRACT(raw_data, '$.chargeAmount') AS DECIMAL(18,2)) AS charge_amount,
  CAST(JSON_EXTRACT(raw_data, '$.user_id') AS STRING) AS user_id,
  CAST(JSON_EXTRACT(raw_data, '$.is_anomaly') AS BOOLEAN) AS is_anomaly,
  CAST(JSON_EXTRACT(raw_data, '$.anomaly_type') AS STRING) AS anomaly_type,
  CAST(JSON_EXTRACT(raw_data, '$.failureReason') AS STRING) AS failure_reason,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.eventTimestamp') AS BIGINT)) AS DATE) AS event_date,
  CAST(HOUR(CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.eventTimestamp') AS BIGINT)) AS TIMESTAMP)) AS INT) AS event_hour,
  _ingested_at,
  CURRENT_TIMESTAMP() AS _updated_at
FROM STREAM(sms_cdrs)
CONSTRAINT valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_imsi EXPECT (LENGTH(imsi) = 15) ON VIOLATION WARN
CONSTRAINT valid_sms_ref EXPECT (sms_reference_number IS NOT NULL) ON VIOLATION DROP ROW;
