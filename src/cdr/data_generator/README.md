# Telco CDR Data Generator

This directory contains scripts to generate synthetic telco user profiles and Call Detail Records (CDRs) for demonstration purposes. The data can be sent to Kafka topics for streaming ingestion into Databricks Delta Live Tables (DLT).

## Components

- `user_generator.py`: Generates synthetic user profiles with phone numbers, IMSIs, IMEIs, and plan details
- `cdr_generator.py`: Generates different types of CDRs (voice, data, SMS, VoIP, IMS, MMS, roaming, WiFi calling)
- `anomaly_generator.py`: Injects realistic network anomalies into CDRs for testing and analysis
- `kafka_producer.py`: Sends generated CDRs to Kafka topics continuously
- `export_users_to_kafka.py`: Exports user profiles to a Kafka topic
- `test_generator.py`: Test script to demonstrate CDR generation and anomaly injection

## Data Consistency

The data generator ensures that CDRs reference valid user IDs from the generated user profiles. This consistency allows for proper joining between user data and CDR data in downstream processing. When generating CDRs, the system uses the same pool of user IDs that were created during user generation, ensuring referential integrity.

## CDR Types

The generator produces eight types of CDRs:

1. **Voice CDRs**: Call details including duration, calling/called numbers
2. **Data CDRs**: Data session details with uplink/downlink volumes
3. **SMS CDRs**: Message details with sender/receiver information
4. **VoIP CDRs**: Voice over IP call details with quality metrics
5. **IMS CDRs**: IP Multimedia Subsystem session details
6. **MMS CDRs**: Multimedia message details with content type and size
7. **Roaming CDRs**: International roaming activity with visited networks
8. **WiFi Calling CDRs**: Calls made over WiFi networks with signal strength

## Anomaly Generation

The anomaly generator injects realistic network issues into CDRs for testing anomaly detection systems:

**Voice Anomalies:**
- Dropped calls (premature termination)
- Failed connections (zero duration)
- Poor quality indicators

**Data Anomalies:**
- Dropped sessions (unexpected disconnection)
- Connection timeouts
- Abnormal usage spikes (potential fraud/malware)

**SMS/MMS Anomalies:**
- Delivery failures
- Routing errors
- Size limit exceeded

**VoIP Anomalies:**
- High packet loss (>15%)
- High jitter (>150ms)
- Codec failures

**IMS Anomalies:**
- Session timeouts
- Authentication failures

**Roaming Anomalies:**
- Roaming fraud patterns
- Unauthorized network access

## Usage

### Prerequisites

1. Python 3.6+
2. Install required packages:
   ```
   pip install confluent-kafka
   ```

3. Confluent CLI (for automated setup):
   ```
   # Install Confluent CLI
   # https://docs.confluent.io/confluent-cli/current/install.html
   
   # Login to Confluent Cloud
   confluent login
   ```

### Automated Confluent Cloud Setup

#### Option 1: Full Automation (Creates Everything)

This will create environment, cluster, topics, and register schemas:

```bash
python setup_confluent.py \
  --env-name telco-cdr-env \
  --cluster-name telco-cdr-cluster \
  --cloud aws \
  --region us-east-1
```

The script will:
- Create or use existing environment
- Create or use existing Kafka cluster (Basic tier)
- Create API keys
- Enable Schema Registry
- Create all topics
- Register all Avro schemas
- Save configuration to `confluent_config.json`

#### Option 2: Quick Setup (Existing Cluster)

If you already have a cluster and just need topics and schemas:

```bash
# First, select your environment and cluster
confluent environment use <env-id>
confluent kafka cluster use <cluster-id>

# Then run quick setup
python setup_topics_schemas.py
```

### Manual Setup

If you prefer manual setup:

1. Create topics in Confluent Cloud UI or CLI
2. Register schemas from the `schemas/` directory
3. Create API keys for Kafka and Schema Registry

### Generate Sample Data Files

To generate sample user and CDR data files without sending to Kafka:

```bash
# Generate 1000 users
python user_generator.py

# Generate 100 CDRs
python cdr_generator.py

# Test new CDR types and anomaly generation
python test_generator.py
```

### Send Data to Kafka

1. First, export user profiles to Kafka:

```bash
python export_users_to_kafka.py \
  --bootstrap-servers $CONFLUENT_KAFKA_BOOTSTRAP_SERVERS \
  --sasl-username $CONFLUENT_API_KEY \
  --sasl-password $CONFLUENT_API_SECRET \
  --topic telco-users \
  --num-users 1000
```

2. Then, start sending CDRs continuously using the same user pool:

```bash
# Without anomalies
python kafka_producer.py \
  --bootstrap-servers $CONFLUENT_KAFKA_BOOTSTRAP_SERVERS \
  --sasl-username $CONFLUENT_API_KEY \
  --sasl-password $CONFLUENT_API_SECRET \
  --interval 0.5 \
  --users-file users.json

# With anomalies (10% anomaly rate)
python kafka_producer.py \
  --bootstrap-servers $CONFLUENT_KAFKA_BOOTSTRAP_SERVERS \
  --sasl-username $CONFLUENT_API_KEY \
  --sasl-password $CONFLUENT_API_SECRET \
  --interval 0.5 \
  --users-file users.json \
  --enable-anomalies \
  --anomaly-rate 0.1
```

**Important**: Make sure to use the same `--num-users` value or `--users-file` that was used when generating users to ensure proper join capabilities in downstream processing.

The producer will send CDRs to the following topics:
- `telco-voice-cdrs`: Voice call records
- `telco-data-cdrs`: Data usage records
- `telco-sms-cdrs`: SMS message records
- `telco-voip-cdrs`: VoIP call records
- `telco-ims-cdrs`: IMS session records
- `telco-mms-cdrs`: MMS message records
- `telco-roaming-cdrs`: Roaming activity records
- `telco-wifi-calling-cdrs`: WiFi calling records

Press Ctrl+C to stop the producer.

## Customization

- Adjust the CDR type distribution by modifying the `cdr_type_weights` in `cdr_generator.py`
- Change the number of users with the `--num-users` parameter
- Adjust the message sending rate with the `--interval` parameter (in seconds)
- Limit the total number of messages with the `--max-count` parameter
- Use a specific user file with the `--users-file` parameter to ensure consistent user references

## Data Joining in Downstream Processing

The consistent user IDs across all CDR types enable several analytical scenarios:

- Join user profiles with any CDR type to analyze usage patterns by plan type
- Correlate activity across different CDR types for the same user
- Calculate aggregated metrics per user across all services
- Track user behavior over time with consistent identification
