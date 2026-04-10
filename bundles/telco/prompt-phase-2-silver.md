# Phase 2 — Silver

## Goal
Parse, type, deduplicate and validate all 9 bronze tables into silver.
Done when: silver row counts match bronze (minus legitimate drops), all types correct, expectations logged.

## Prerequisite
Phase 1 complete — `dev_bronze.telco.*` tables populated.

## Stack & Key Rules
- SQL-first: each silver table is a `.sql` file with `CREATE OR REFRESH STREAMING TABLE`
- Read from bronze with plain table name (e.g. `FROM STREAM voice_cdrs`) — no `LIVE.` prefix
- `dp.create_streaming_table` + `dp.create_auto_cdc_flow` for users (SCD Type 1)
- SDP manages checkpoints — do not add checkpoint options

## Unity Catalog
```
dev_silver.telco.users          ← SCD Type 1 via AUTO CDC
dev_silver.telco.voice_cdrs
dev_silver.telco.data_cdrs
dev_silver.telco.sms_cdrs
dev_silver.telco.voip_cdrs
dev_silver.telco.ims_cdrs
dev_silver.telco.mms_cdrs
dev_silver.telco.roaming_cdrs
dev_silver.telco.wifi_calling_cdrs
```

## Source Schemas (parse from `raw_data` JSON)

**User Schema:**
```json
{
  "user_id": "uuid",
  "msisdn": "11-digit phone number",
  "imsi": "15-digit IMSI",
  "imei": "15-digit IMEI",
  "plan_name": "Basic|Standard|Premium|Unlimited",
  "data_limit_gb": "integer",
  "voice_minutes": "integer",
  "sms_count": "integer",
  "city": "string",
  "state": "string",
  "activation_date": "YYYY-MM-DD"
}
```

**Voice CDR Schema (3GPP TS 32.298):**
```json
{
  "cdr_type": "voice",
  "callEventStartTime": "unix_timestamp",
  "callEventEndTime": "unix_timestamp",
  "callDuration": "seconds",
  "callReferenceNumber": "string",
  "callingPartyNumber": "msisdn",
  "calledPartyNumber": "msisdn",
  "imsi": "string",
  "imei": "string",
  "mscID": "string (Mobile Switching Center ID)",
  "lac": "string (Location Area Code)",
  "ci": "string (Cell Identity)",
  "causeForTermination": "string (3GPP cause code)",
  "chargeID": "string",
  "callChargeableDuration": "seconds",
  "tariffClass": "standard_voice|premium_voice|international|roaming",
  "callType": "MOC|MTC|forwarded (Mobile Originated/Terminated)",
  "recordSequenceNumber": "integer",
  "user_id": "uuid",
  "is_anomaly": "boolean",
  "anomaly_type": "dropped_call|failed_connection|poor_quality|null"
}
```

**Data CDR Schema (3GPP GGSN/PGW):**
```json
{
  "cdr_type": "data_usage",
  "recordType": "integer",
  "recordOpeningTime": "unix_timestamp",
  "recordClosingTime": "unix_timestamp",
  "duration": "seconds",
  "imsi": "string",
  "imei": "string",
  "msisdn": "string",
  "dataVolumeUplink": "bytes",
  "dataVolumeDownlink": "bytes",
  "servingNodeAddress": "ip_address",
  "apn": "string (Access Point Name)",
  "pdnType": "IPv4|IPv6|IPv4v6",
  "ratType": "UTRAN|GERAN|WLAN|GAN|HSPA|EUTRAN|NR (2G-5G)",
  "chargingID": "string",
  "qci": "integer (QoS Class Identifier 1-9)",
  "recordSequenceNumber": "integer",
  "sgsn_address": "ip_address (Serving GPRS Support Node)",
  "user_id": "uuid",
  "is_anomaly": "boolean",
  "anomaly_type": "dropped_session|connection_timeout|abnormal_usage|null",
  "causeForRecordClosing": "normalRelease|abnormalRelease|timeLimit|null"
}
```

**SMS CDR Schema:**
```json
{
  "cdr_type": "sms",
  "eventTimestamp": "unix_timestamp",
  "smsReferenceNumber": "string",
  "originatingNumber": "msisdn",
  "destinationNumber": "msisdn",
  "imsi": "string",
  "smscAddress": "string (SMS Center)",
  "messageSize": "integer (characters)",
  "deliveryStatus": "delivered|failed|pending",
  "messageType": "SMS-MO|SMS-MT (Mobile Originated/Terminated)",
  "recordSequenceNumber": "integer",
  "chargeAmount": "decimal",
  "user_id": "uuid",
  "is_anomaly": "boolean",
  "anomaly_type": "delivery_failure|routing_error|null",
  "failureReason": "subscriber_absent|invalid_destination|network_error|null"
}
```

**VoIP CDR Schema:**
```json
{
  "cdr_type": "voip",
  "sessionStartTime": "unix_timestamp",
  "sessionEndTime": "unix_timestamp",
  "sessionDuration": "seconds",
  "callingPartyURI": "sip_uri",
  "calledPartyURI": "sip_uri",
  "codec": "G.711|G.722|G.729|OPUS|AMR-WB",
  "callQualityIndicator": "1-5 (1=poor, 5=excellent)",
  "bytesTransferred": "integer",
  "packetLoss": "decimal (percentage)",
  "jitter": "decimal (milliseconds)",
  "latency": "decimal (milliseconds)",
  "chargeID": "string",
  "user_id": "uuid",
  "is_anomaly": "boolean",
  "anomaly_type": "high_packet_loss|high_jitter|codec_failure|null"
}
```

**IMS CDR Schema (IP Multimedia Subsystem):**
```json
{
  "cdr_type": "ims",
  "sessionStartTime": "unix_timestamp",
  "sessionEndTime": "unix_timestamp",
  "sessionDuration": "seconds",
  "originatingURI": "sip_uri",
  "destinationURI": "sip_uri",
  "serviceType": "voice|video|messaging|presence|file_transfer",
  "mediaTypes": "audio|video|audio+video|text|application",
  "networkType": "LTE|5G|WiFi",
  "qosClass": "conversational|streaming|interactive|background",
  "chargeID": "string",
  "user_id": "uuid",
  "is_anomaly": "boolean",
  "anomaly_type": "session_timeout|authentication_failure|null",
  "terminationReason": "normal|timeout|authentication_failed|null"
}
```

**MMS CDR Schema:**
```json
{
  "cdr_type": "mms",
  "eventTimestamp": "unix_timestamp",
  "mmsReferenceNumber": "string",
  "originatingNumber": "msisdn",
  "destinationNumber": "msisdn",
  "imsi": "string",
  "mmscAddress": "string (MMS Center)",
  "messageSize": "bytes",
  "contentType": "image/jpeg|image/png|video/mp4|audio/mpeg",
  "deliveryStatus": "delivered|failed|pending",
  "chargeAmount": "decimal",
  "user_id": "uuid",
  "is_anomaly": "boolean",
  "anomaly_type": "delivery_failure|size_limit_exceeded|null",
  "failureReason": "recipient_unavailable|network_error|message_too_large|null"
}
```

**Roaming CDR Schema:**
```json
{
  "cdr_type": "roaming",
  "callEventStartTime": "unix_timestamp",
  "callEventEndTime": "unix_timestamp",
  "callDuration": "seconds",
  "callingPartyNumber": "msisdn",
  "calledPartyNumber": "msisdn",
  "imsi": "string",
  "imei": "string",
  "roamingCountry": "string",
  "visitedNetwork": "string",
  "mcc": "string (Mobile Country Code)",
  "mnc": "string (Mobile Network Code)",
  "roamingType": "international|national",
  "serviceType": "voice|data|sms",
  "chargeAmount": "decimal",
  "roamingStatus": "authorized|unauthorized",
  "user_id": "uuid",
  "is_anomaly": "boolean",
  "anomaly_type": "roaming_fraud|unauthorized_network|null",
  "suspiciousActivity": "boolean|null",
  "fraudScore": "decimal (0.0-1.0)|null",
  "connectionAllowed": "boolean|null"
}
```

**WiFi Calling CDR Schema:**
```json
{
  "cdr_type": "wifi_calling",
  "sessionStartTime": "unix_timestamp",
  "sessionEndTime": "unix_timestamp",
  "sessionDuration": "seconds",
  "callingPartyNumber": "msisdn",
  "calledPartyNumber": "msisdn",
  "imsi": "string",
  "wifiSSID": "string",
  "wifiAccessPoint": "ip_address",
  "signalStrength": "integer (dBm, -80 to -30)",
  "codec": "G.711|G.722|OPUS|AMR-WB",
  "callQuality": "1-5",
  "handoverToCell": "boolean",
  "chargeID": "string",
  "user_id": "uuid",
  "is_anomaly": "boolean",
  "anomaly_type": "string|null"
}
```

## Silver Requirements

**All tables:**
- Convert unix timestamps → TIMESTAMP 
- Add `event_date` (DATE), `event_hour` (INT)
- Add `_ingested_at` (pass through from bronze), `_updated_at` (current_timestamp)
- Dedup on business key + sequence number within micro-batch

**data_cdrs additionally:**
- `data_volume_total_mb` = (uplink + downlink) / 1048576.0
- `network_generation`: GERAN→2G, UTRAN→3G, EUTRAN→4G, NR→5G, WLAN→WiFi

**users:** SCD Type 1 — SQL `APPLY CHANGES INTO ... STORED AS SCD TYPE 1`, key=`user_id`, sequence_by=`_kafka_offset`.

**Dedup key per table:**
| Table | Business key |
|---|---|
| voice_cdrs | `callReferenceNumber` |
| data_cdrs | `chargingID` |
| sms_cdrs | `smsReferenceNumber` |
| voip_cdrs | `chargeID` |
| ims_cdrs | `chargeID` |
| mms_cdrs | `mmsReferenceNumber` |
| roaming_cdrs | `chargeID` + `callEventStartTime` |
| wifi_calling_cdrs | `chargeID` |

**Expectations (SDP CONSTRAINTS) — silver only:**
- NOT NULL on: `user_id`, `imsi`, primary timestamp, business key
- IMSI format: `LENGTH(imsi) = 15` — WARN (not DROP, source data may have gaps)
- Duration >= 0 where applicable
- `qci` BETWEEN 1 AND 9 for data_cdrs
- `fraud_score` BETWEEN 0 AND 1 for roaming_cdrs

**Do not implement:** `call_cost` — no tariff rate table available. Document as future enhancement.

## Deliverables
```
src/telco/transformations/
├── silver_users.sql APPLY CHANGES INTO (SCD Type 1, SQL)
├── silver_voice_cdrs.sql
├── silver_data_cdrs.sql
├── silver_sms_cdrs.sql
├── silver_voip_cdrs.sql
├── silver_ims_cdrs.sql
├── silver_mms_cdrs.sql
├── silver_roaming_cdrs.sql
└── silver_wifi_calling_cdrs.sql
```
