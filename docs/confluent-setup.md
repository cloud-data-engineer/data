# Confluent Cloud Setup — Telco CDR Pipeline

## Quick Reference

### One-command fresh deployment
```bash
python setup_confluent.py \
  --env-name telco-cdr-env \
  --cluster-name telco-cdr-cluster \
  --cloud aws \
  --region us-east-1
```
Creates: Environment → Cluster → API keys → Schema Registry → 9 topics → 18 Avro schemas → `confluent_config.json`

### Existing cluster — topics + schemas only
```bash
confluent environment use <env-id>
confluent kafka cluster use <cluster-id>
python setup_topics_schemas.py
```

### Set env vars from config
```bash
export CONFLUENT_KAFKA_BOOTSTRAP_SERVERS="pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
export CONFLUENT_API_KEY="YOUR_API_KEY"
export CONFLUENT_API_SECRET="YOUR_API_SECRET"
```

### Send data
```bash
# Users
python export_users_to_kafka.py \
  --bootstrap-servers $CONFLUENT_KAFKA_BOOTSTRAP_SERVERS \
  --sasl-username $CONFLUENT_API_KEY \
  --sasl-password $CONFLUENT_API_SECRET \
  --topic telco-users --num-users 1000

# CDRs with anomalies
python kafka_producer.py \
  --bootstrap-servers $CONFLUENT_KAFKA_BOOTSTRAP_SERVERS \
  --sasl-username $CONFLUENT_API_KEY \
  --sasl-password $CONFLUENT_API_SECRET \
  --interval 0.5 --users-file users.json \
  --enable-anomalies --anomaly-rate 0.1
```

### Verify
```bash
confluent kafka topic list
confluent schema-registry schema list
confluent kafka topic consume telco-voice-cdrs --from-beginning
```

---

## Automation Scripts

| Script | Purpose |
|--------|---------|
| `setup_confluent.py` | Full automation — creates everything from scratch, idempotent |
| `setup_topics_schemas.py` | Quick setup — topics and schemas only, requires existing cluster |

Both scripts are **idempotent** — safe to re-run; they check for existing resources before creating.

---

## Manual Setup (Step by Step)

### 1. Install Confluent CLI

```bash
# macOS
brew install confluentinc/tap/cli

# Linux
curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest

confluent version   # verify
confluent login --save
```

### 2. Create environment and cluster

```bash
confluent environment create telco-cdr-env
confluent environment use <env-id>

confluent kafka cluster create telco-cdr-cluster \
  --cloud aws --region us-east-1 --type basic
confluent kafka cluster use <cluster-id>
```

### 3. Create API keys

```bash
confluent api-key create --resource <cluster-id>
confluent api-key use <api-key> --resource <cluster-id>
```

### 4. Create topics (9 total, 3 partitions each)

```bash
confluent kafka topic create telco-users            --partitions 3
confluent kafka topic create telco-voice-cdrs       --partitions 3
confluent kafka topic create telco-data-cdrs        --partitions 3
confluent kafka topic create telco-sms-cdrs         --partitions 3
confluent kafka topic create telco-voip-cdrs        --partitions 3
confluent kafka topic create telco-ims-cdrs         --partitions 3
confluent kafka topic create telco-mms-cdrs         --partitions 3
confluent kafka topic create telco-roaming-cdrs     --partitions 3
confluent kafka topic create telco-wifi-calling-cdrs --partitions 3
```

### 5. Enable Schema Registry

```bash
confluent schema-registry cluster enable --cloud aws --geo us
confluent api-key create --resource <schema-registry-id>
```

### 6. Register schemas

```bash
# User schemas
confluent schema-registry schema create \
  --subject telco-users-key   --schema schemas/user_key.avsc   --type AVRO
confluent schema-registry schema create \
  --subject telco-users-value --schema schemas/user_value.avsc --type AVRO

# CDR key (shared across all CDR topics)
confluent schema-registry schema create \
  --subject telco-voice-cdrs-key --schema schemas/cdr_key.avsc --type AVRO

# CDR value schemas (repeat pattern for each type)
confluent schema-registry schema create \
  --subject telco-voice-cdrs-value \
  --schema schemas/voice_cdr_value.avsc --type AVRO
# ... or just run: python setup_topics_schemas.py
```

---

## Avro Schema Layout

```
schemas/
├── user_key.avsc                  # User key
├── user_value.avsc                # User profile
├── cdr_key.avsc                   # CDR key (shared by all CDR topics)
├── voice_cdr_value.avsc
├── data_cdr_value.avsc
├── sms_cdr_value.avsc
├── voip_cdr_value.avsc
├── ims_cdr_value.avsc
├── mms_cdr_value.avsc
├── roaming_cdr_value.avsc
└── wifi_calling_cdr_value.avsc
```

### Topic → Schema Registry subject mapping

```
telco-users              → telco-users-key / telco-users-value
telco-voice-cdrs         → telco-voice-cdrs-key / telco-voice-cdrs-value
telco-data-cdrs          → telco-data-cdrs-key / telco-data-cdrs-value
telco-sms-cdrs           → telco-sms-cdrs-key / telco-sms-cdrs-value
telco-voip-cdrs          → telco-voip-cdrs-key / telco-voip-cdrs-value
telco-ims-cdrs           → telco-ims-cdrs-key / telco-ims-cdrs-value
telco-mms-cdrs           → telco-mms-cdrs-key / telco-mms-cdrs-value
telco-roaming-cdrs       → telco-roaming-cdrs-key / telco-roaming-cdrs-value
telco-wifi-calling-cdrs  → telco-wifi-calling-cdrs-key / telco-wifi-calling-cdrs-value
```

All CDR value schemas include `is_anomaly` (boolean) and `anomaly_type` (nullable string). Schema evolution uses backward/forward compatibility with null-union types and default values.

---

## Configuration File

`confluent_config.json` is generated by `setup_confluent.py`:

```json
{
  "environment_id": "env-xxxxx",
  "cluster_id": "lkc-xxxxx",
  "bootstrap_servers": "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
  "api_key": "YOUR_KAFKA_API_KEY",
  "api_secret": "YOUR_KAFKA_API_SECRET",
  "schema_registry_id": "lsrc-xxxxx",
  "schema_registry_api_key": "YOUR_SR_API_KEY",
  "schema_registry_api_secret": "YOUR_SR_API_SECRET",
  "cloud": "aws",
  "region": "us-east-1"
}
```

Do not commit this file — it contains credentials.

---

## Troubleshooting

| Error | Check |
|-------|-------|
| Schema registration fails | `confluent schema-registry cluster describe` |
| Topic creation fails | `confluent kafka cluster describe` |
| Authentication errors | `confluent api-key list` |

---

## Cleanup

```bash
confluent kafka topic delete telco-users
confluent kafka topic delete telco-voice-cdrs
# ... repeat for all topics

confluent kafka cluster delete <cluster-id>
confluent environment delete <env-id>
```

---

## Cost Notes

- Basic cluster: pay-as-you-go
- Schema Registry: included with cluster
- Ingress: free; egress: charged per GB
- Storage: charged per GB-hour

Monitor usage in the Confluent Cloud Console.
