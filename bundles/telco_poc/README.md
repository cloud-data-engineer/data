# Telco CDR Ingestion POC — Databricks Asset Bundle

Streaming medallion pipeline ingesting telco CDR data from Confluent Kafka into Unity Catalog, with a Customer 360 AI/BI dashboard.

Built with the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) and Claude Code.

---

## Architecture

```
Confluent Kafka (9 topics)
        │
        ▼
dev_bronze.telco_poc.bronze_*     ← raw JSON, one streaming table per topic
        │
        ▼
dev_silver.telco_poc.silver_*     ← parsed, typed, validated CDR tables
        │
        ▼
dev_gold.telco_poc.gold_*         ← Customer 360 materialized views
        │
        ▼
AI/BI Dashboard                   ← Customer 360 Dashboard
```

## Kafka Topics

| Topic | Bronze Table | CDR Type |
|---|---|---|
| telco-users | bronze_users | Subscriber profiles (SCD Type 1) |
| telco-voice-cdrs | bronze_voice_cdrs | Voice calls |
| telco-data-cdrs | bronze_data_cdrs | Mobile data sessions |
| telco-sms-cdrs | bronze_sms_cdrs | SMS |
| telco-voip-cdrs | bronze_voip_cdrs | VoIP sessions |
| telco-ims-cdrs | bronze_ims_cdrs | IMS sessions |
| telco-mms-cdrs | bronze_mms_cdrs | MMS |
| telco-roaming-cdrs | bronze_roaming_cdrs | Roaming events |
| telco-wifi-calling-cdrs | bronze_wifi_calling_cdrs | WiFi calling |

## Gold Layer

| View | Description |
|---|---|
| `gold_customer_360` | One row per subscriber — lifetime voice/data/SMS/roaming totals, charges, quality metrics |
| `gold_daily_cdr_summary` | Daily event counts and durations by subscriber |
| `gold_roaming_summary` | Countries visited, fraud scores, blocked connections |
| `gold_anomaly_summary` | Anomaly signal counts across all 8 CDR types |

---

## Prerequisites

**Kafka secrets** — store in Databricks secret scope `dev_kafka`:

```bash
databricks secrets create-scope dev_kafka
databricks secrets put-secret dev_kafka bootstrap_servers --string-value "..."
databricks secrets put-secret dev_kafka api_key --string-value "..."
databricks secrets put-secret dev_kafka api_secret --string-value "..."
```

**Unity Catalog** — the following catalogs must exist before deploying:

```sql
CREATE CATALOG IF NOT EXISTS dev_bronze;
CREATE CATALOG IF NOT EXISTS dev_silver;
CREATE CATALOG IF NOT EXISTS dev_gold;
```

---

## Deploy

```bash
cd bundles/telco_poc

# Validate
databricks bundle validate --profile dev

# Deploy pipeline + dashboard
databricks bundle deploy --profile dev

# Run the pipeline
databricks bundle run telco_poc --profile dev
```

---

## Bundle Structure

```
bundles/telco_poc/
├── databricks.yml                              ← bundle config, dev/prod targets
├── resources/
│   ├── telco_poc.pipeline.yml                  ← serverless SDP definition
│   └── telco_poc.dashboard.yml                 ← AI/BI dashboard definition
└── src/telco_poc/
    ├── transformations/
    │   ├── bronze.py                           ← 9 Kafka streaming tables
    │   ├── silver.py                           ← parsed + validated CDR tables
    │   └── gold.py                             ← Customer 360 materialized views
    └── dashboards/
        └── telco_customer_360.lvdash.json      ← Customer 360 dashboard
```
