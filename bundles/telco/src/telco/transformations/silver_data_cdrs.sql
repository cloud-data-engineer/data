CREATE OR REFRESH STREAMING TABLE silver_data_cdrs
COMMENT "Cleaned and typed data CDR data"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(JSON_EXTRACT(raw_data, '$.cdr_type') AS STRING) AS cdr_type,
  CAST(JSON_EXTRACT(raw_data, '$.recordType') AS INT) AS record_type,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.recordOpeningTime') AS BIGINT)) AS TIMESTAMP) AS record_opening_time,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.recordClosingTime') AS BIGINT)) AS TIMESTAMP) AS record_closing_time,
  CAST(JSON_EXTRACT(raw_data, '$.duration') AS INT) AS duration,
  CAST(JSON_EXTRACT(raw_data, '$.imsi') AS STRING) AS imsi,
  CAST(JSON_EXTRACT(raw_data, '$.imei') AS STRING) AS imei,
  CAST(JSON_EXTRACT(raw_data, '$.msisdn') AS STRING) AS msisdn,
  CAST(JSON_EXTRACT(raw_data, '$.dataVolumeUplink') AS BIGINT) AS data_volume_uplink,
  CAST(JSON_EXTRACT(raw_data, '$.dataVolumeDownlink') AS BIGINT) AS data_volume_downlink,
  (CAST(JSON_EXTRACT(raw_data, '$.dataVolumeUplink') AS DOUBLE) + CAST(JSON_EXTRACT(raw_data, '$.dataVolumeDownlink') AS DOUBLE)) / 1048576.0 AS data_volume_total_mb,
  CAST(JSON_EXTRACT(raw_data, '$.servingNodeAddress') AS STRING) AS serving_node_address,
  CAST(JSON_EXTRACT(raw_data, '$.apn') AS STRING) AS apn,
  CAST(JSON_EXTRACT(raw_data, '$.pdnType') AS STRING) AS pdn_type,
  CAST(JSON_EXTRACT(raw_data, '$.ratType') AS STRING) AS rat_type,
  CASE 
    WHEN JSON_EXTRACT(raw_data, '$.ratType') = 'GERAN' THEN '2G'
    WHEN JSON_EXTRACT(raw_data, '$.ratType') = 'UTRAN' THEN '3G'
    WHEN JSON_EXTRACT(raw_data, '$.ratType') = 'EUTRAN' THEN '4G'
    WHEN JSON_EXTRACT(raw_data, '$.ratType') = 'NR' THEN '5G'
    WHEN JSON_EXTRACT(raw_data, '$.ratType') = 'WLAN' THEN 'WiFi'
    ELSE 'Other'
  END AS network_generation,
  CAST(JSON_EXTRACT(raw_data, '$.chargingID') AS STRING) AS charging_id,
  CAST(JSON_EXTRACT(raw_data, '$.qci') AS INT) AS qci,
  CAST(JSON_EXTRACT(raw_data, '$.recordSequenceNumber') AS INT) AS record_sequence_number,
  CAST(JSON_EXTRACT(raw_data, '$.sgsn_address') AS STRING) AS sgsn_address,
  CAST(JSON_EXTRACT(raw_data, '$.user_id') AS STRING) AS user_id,
  CAST(JSON_EXTRACT(raw_data, '$.is_anomaly') AS BOOLEAN) AS is_anomaly,
  CAST(JSON_EXTRACT(raw_data, '$.anomaly_type') AS STRING) AS anomaly_type,
  CAST(JSON_EXTRACT(raw_data, '$.causeForRecordClosing') AS STRING) AS cause_for_record_closing,
  CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.recordOpeningTime') AS BIGINT)) AS DATE) AS event_date,
  CAST(HOUR(CAST(FROM_UNIXTIME(CAST(JSON_EXTRACT(raw_data, '$.recordOpeningTime') AS BIGINT)) AS TIMESTAMP)) AS INT) AS event_hour,
  _ingested_at,
  CURRENT_TIMESTAMP() AS _updated_at
FROM STREAM(data_cdrs)
CONSTRAINT valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_imsi EXPECT (LENGTH(imsi) = 15) ON VIOLATION WARN
CONSTRAINT valid_charging_id EXPECT (charging_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_qci EXPECT (qci BETWEEN 1 AND 9) ON VIOLATION DROP ROW;
