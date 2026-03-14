# Telco CDR Data Generator

Synthetic telco data generator for streaming into Databricks via Kafka. Produces 3GPP-compliant CDRs across 8 record types with built-in anomaly injection and referential integrity between users and CDRs.

## Components

| File | Purpose |
|------|---------|
| `user_generator.py` | Generates synthetic user profiles (MSISDN, IMSI, IMEI, plan) |
| `cdr_generator.py` | Generates 8 CDR types with configurable type weights |
| `anomaly_generator.py` | Wraps CDR generator and injects realistic network failures |
| `kafka_producer.py` | Streams CDRs to Confluent Kafka topics continuously |
| `export_users_to_kafka.py` | Publishes user profiles to the `telco-users` topic |
| `test_generator.py` | Local test — prints CDR samples and anomaly statistics |

## CDR Types & Kafka Topics

| CDR Type | Topic | 3GPP Reference |
|----------|-------|----------------|
| Voice | `telco-voice-cdrs` | TS 32.298 CS domain |
| Data | `telco-data-cdrs` | TS 32.251 GGSN/PGW |
| SMS | `telco-sms-cdrs` | SMS-MO / SMS-MT |
| VoIP | `telco-voip-cdrs` | SIP/IMS (RFC 3261) |
| IMS | `telco-ims-cdrs` | 3GPP IMS charging |
| MMS | `telco-mms-cdrs` | MMSC routing |
| Roaming | `telco-roaming-cdrs` | MCC/MNC, visited network |
| WiFi Calling | `telco-wifi-calling-cdrs` | WiFi SSID, handover |

User profiles are published to `telco-users`.

## Anomaly Types

Injected via `anomaly_generator.py` — all CDRs carry `is_anomaly` (boolean) and `anomaly_type` (nullable string) fields.

| CDR Type | Anomaly Types |
|----------|--------------|
| Voice | `dropped_call`, `failed_connection`, `poor_quality` |
| Data | `dropped_session`, `connection_timeout`, `abnormal_usage` |
| SMS / MMS | `delivery_failure`, `routing_error`, `size_limit_exceeded` |
| VoIP | `high_packet_loss`, `high_jitter`, `codec_failure` |
| IMS | `session_timeout`, `authentication_failure` |
| Roaming | `roaming_fraud`, `unauthorized_network` |

17 anomaly types total. Each reflects a real-world network failure with realistic field mutations (e.g. `dropped_call` truncates `callDuration`; `abnormal_usage` sets data volume to 500 MB–2 GB).

## 3GPP Standards Compliance

All mandatory fields from the relevant 3GPP specifications are present:

- **Subscriber identifiers**: IMSI (15-digit), IMEI (15-digit), MSISDN (E.164)
- **Network elements**: MSC ID, SGSN address, LAC (Location Area Code), CI (Cell Identity)
- **Timing**: start time, end time, duration (all record types)
- **Charging**: charge ID, tariff class, chargeable duration
- **Sequence numbers**: `recordSequenceNumber` on all CDR types — enables duplicate detection, gap analysis, and audit trails
- **Call/message classification**: `callType` (MOC/MTC) on voice; `messageType` (SMS-MO/SMS-MT) on SMS
- **QoS**: QCI (1–9 standardised, 128–254 operator-specific), RAT type (GERAN/UTRAN/EUTRAN/NR)

Avro schemas in `schemas/` are registered in Confluent Schema Registry with backward/forward compatibility and null-union defaults for optional fields.

## Data Consistency

CDRs always reference `user_id` values drawn from the generated user pool. Use the same `--users-file` across both `export_users_to_kafka.py` and `kafka_producer.py` to ensure downstream joins work correctly.

## Usage

### 1. Generate & test locally

```bash
python test_generator.py        # prints samples + anomaly stats
python user_generator.py        # writes users.json (1000 users)
python cdr_generator.py         # writes sample CDRs
```

### 2. Publish users to Kafka

```bash
python export_users_to_kafka.py \
  --bootstrap-servers $CONFLUENT_KAFKA_BOOTSTRAP_SERVERS \
  --sasl-username $CONFLUENT_API_KEY \
  --sasl-password $CONFLUENT_API_SECRET \
  --topic telco-users \
  --num-users 1000
```

### 3. Stream CDRs continuously

```bash
# Without anomalies
python kafka_producer.py \
  --bootstrap-servers $CONFLUENT_KAFKA_BOOTSTRAP_SERVERS \
  --sasl-username $CONFLUENT_API_KEY \
  --sasl-password $CONFLUENT_API_SECRET \
  --interval 0.5 \
  --users-file users.json

# With 10% anomaly rate
python kafka_producer.py \
  --bootstrap-servers $CONFLUENT_KAFKA_BOOTSTRAP_SERVERS \
  --sasl-username $CONFLUENT_API_KEY \
  --sasl-password $CONFLUENT_API_SECRET \
  --interval 0.5 \
  --users-file users.json \
  --enable-anomalies \
  --anomaly-rate 0.1
```

Press `Ctrl+C` to stop.

## Tuning Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--num-users` | 1000 | User pool size |
| `--users-file` | `users.json` | Reuse existing user pool |
| `--interval` | 0.5 | Seconds between messages |
| `--max-count` | unlimited | Stop after N messages |
| `--enable-anomalies` | off | Enable anomaly injection |
| `--anomaly-rate` | 0.1 | Fraction of CDRs that are anomalies (0.0–1.0) |

CDR type distribution is controlled by `cdr_type_weights` in `cdr_generator.py`.

## Comparison with Other Open-Source Tools

| Feature | RealImpact (Scala) | mayconbordin (Java) | This Generator |
|---------|--------------------|---------------------|----------------|
| CDR types | Voice only | Voice only | **8 types** |
| Anomaly generation | No | No | **17 types** |
| Kafka integration | No | No | Yes |
| Avro + Schema Registry | No | No | Yes |
| 3GPP compliance | Partial | Partial | Full |
| Continuous streaming | No | No | Yes |
| Social network patterns | Yes | No | No |
| Spark scale-out | Yes | No | No |
| Maintained | Archived | Inactive | Active |

Notable gaps vs. existing tools (potential future improvements):
- **Social network call patterns** — realistic call graphs where users call "friends" more often
- **Time distribution** — peak/off-peak hourly and day-of-week patterns
- **Spark-based generation** — parallel output for very large datasets
