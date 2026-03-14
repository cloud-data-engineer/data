# Phase 1 — DAB Scaffold + Bronze

## Goal
Create the DAB bundle and ingest all 9 Kafka topics into bronze streaming tables.
Done when: all 9 tables exist in `dev_bronze.telco.*` with rows flowing.

## Stack & Key Rules
- Serverless SDP, Python pipeline
- Table name = entity only, no layer prefix — catalog already communicates the layer

## Unity Catalog
```
dev_bronze.telco.users
dev_bronze.telco.voice_cdrs
dev_bronze.telco.data_cdrs
dev_bronze.telco.sms_cdrs
dev_bronze.telco.voip_cdrs
dev_bronze.telco.ims_cdrs
dev_bronze.telco.mms_cdrs
dev_bronze.telco.roaming_cdrs
dev_bronze.telco.wifi_calling_cdrs
```

## Kafka Topics
| Topic | Table |
|---|---|
| `telco-users` | `users` |
| `telco-voice-cdrs` | `voice_cdrs` |
| `telco-data-cdrs` | `data_cdrs` |
| `telco-sms-cdrs` | `sms_cdrs` |
| `telco-voip-cdrs` | `voip_cdrs` |
| `telco-ims-cdrs` | `ims_cdrs` |
| `telco-mms-cdrs` | `mms_cdrs` |
| `telco-roaming-cdrs` | `roaming_cdrs` |
| `telco-wifi-calling-cdrs` | `wifi_calling_cdrs` |

Confluent Cloud — SASL_SSL / PLAIN. Secret scope keys: `bootstrap_servers`, `api_key`, `api_secret`.

## Bronze Requirements
- Store raw Kafka `value` as `raw_data STRING` — no parsing
- Metadata columns: `_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_ingested_at`
- All columns nullable
- All 9 topics share identical ingestion logic — use a Python loop

## DAB
Targets: `dev` (mode: development, default) and `prod` (mode: production).
