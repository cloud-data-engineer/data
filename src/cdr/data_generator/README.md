# Telco CDR Data Generator

This directory contains scripts to generate synthetic telco user profiles and Call Detail Records (CDRs) for demonstration purposes. The data can be sent to Kafka topics for streaming ingestion into Databricks Delta Live Tables (DLT).

## Components

- `user_generator.py`: Generates synthetic user profiles with phone numbers, IMSIs, IMEIs, and plan details
- `cdr_generator.py`: Generates different types of CDRs (voice, data, SMS, VoIP, IMS)
- `kafka_producer.py`: Sends generated CDRs to Kafka topics continuously
- `export_users_to_kafka.py`: Exports user profiles to a Kafka topic

## Data Consistency

The data generator ensures that CDRs reference valid user IDs from the generated user profiles. This consistency allows for proper joining between user data and CDR data in downstream processing. When generating CDRs, the system uses the same pool of user IDs that were created during user generation, ensuring referential integrity.

## CDR Types

The generator produces five types of CDRs:

1. **Voice CDRs**: Call details including duration, calling/called numbers
2. **Data CDRs**: Data session details with uplink/downlink volumes
3. **SMS CDRs**: Message details with sender/receiver information
4. **VoIP CDRs**: Voice over IP call details
5. **IMS CDRs**: IP Multimedia Subsystem session details

## Usage

### Prerequisites

1. Python 3.6+
2. Install required packages:
   ```
   pip install confluent-kafka
   ```

3. Access to a Kafka cluster (e.g., Confluent Cloud)

### Generate Sample Data Files

To generate sample user and CDR data files without sending to Kafka:

```bash
# Generate 1000 users
python user_generator.py

# Generate 100 CDRs
python cdr_generator.py
```

### Send Data to Kafka

1. First, export user profiles to Kafka:

```bash
python export_users_to_kafka.py \
  --bootstrap-servers <your-bootstrap-servers> \
  --sasl-username <your-api-key> \
  --sasl-password <your-api-secret> \
  --topic telco-users \
  --num-users 1000
```

2. Then, start sending CDRs continuously using the same user pool:

```bash
python kafka_producer.py \
  --bootstrap-servers <your-bootstrap-servers> \
  --sasl-username <your-api-key> \
  --sasl-password <your-api-secret> \
  --interval 0.5 \
  --num-users 1000 \
  --users-file users.json
```

**Important**: Make sure to use the same `--num-users` value or `--users-file` that was used when generating users to ensure proper join capabilities in downstream processing.

The producer will send CDRs to the following topics:
- `telco-voice-cdrs`: Voice call records
- `telco-data-cdrs`: Data usage records
- `telco-sms-cdrs`: SMS message records
- `telco-voip-cdrs`: VoIP call records
- `telco-ims-cdrs`: IMS session records

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
