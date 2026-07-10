CREATE OR REFRESH STREAMING TABLE silver_users
COMMENT "Cleaned and typed user data — SCD Type 1"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  CAST(JSON_EXTRACT(raw_data, '$.user_id') AS STRING) AS user_id,
  CAST(JSON_EXTRACT(raw_data, '$.msisdn') AS STRING) AS msisdn,
  CAST(JSON_EXTRACT(raw_data, '$.imsi') AS STRING) AS imsi,
  CAST(JSON_EXTRACT(raw_data, '$.imei') AS STRING) AS imei,
  CAST(JSON_EXTRACT(raw_data, '$.plan_name') AS STRING) AS plan_name,
  CAST(JSON_EXTRACT(raw_data, '$.data_limit_gb') AS INT) AS data_limit_gb,
  CAST(JSON_EXTRACT(raw_data, '$.voice_minutes') AS INT) AS voice_minutes,
  CAST(JSON_EXTRACT(raw_data, '$.sms_count') AS INT) AS sms_count,
  CAST(JSON_EXTRACT(raw_data, '$.city') AS STRING) AS city,
  CAST(JSON_EXTRACT(raw_data, '$.state') AS STRING) AS state,
  CAST(JSON_EXTRACT(raw_data, '$.activation_date') AS DATE) AS activation_date,
  _ingested_at,
  CURRENT_TIMESTAMP() AS _updated_at
FROM STREAM(users);

APPLY CHANGES INTO LIVE.silver_users
KEYS (user_id)
SEQUENCE BY _kafka_offset
STORED AS SCD TYPE 1;
