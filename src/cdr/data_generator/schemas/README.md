# Avro Schema Documentation

This directory contains Avro schemas for all Telco CDR data types.

## Schema Files

### User Schemas
- `user_key.avsc` - Key schema for user records (user_id)
- `user_value.avsc` - Value schema for user profiles

### CDR Key Schema
- `cdr_key.avsc` - Shared key schema for all CDR types (user_id)

### CDR Value Schemas
- `voice_cdr_value.avsc` - Voice call records
- `data_cdr_value.avsc` - Data usage records
- `sms_cdr_value.avsc` - SMS message records
- `voip_cdr_value.avsc` - VoIP call records
- `ims_cdr_value.avsc` - IMS session records
- `mms_cdr_value.avsc` - MMS message records
- `roaming_cdr_value.avsc` - Roaming activity records
- `wifi_calling_cdr_value.avsc` - WiFi calling records

## Schema Registry Subjects

When registered in Confluent Schema Registry, schemas are mapped to subjects:

### User Topic
- Subject: `telco-users-key` → Schema: `user_key.avsc`
- Subject: `telco-users-value` → Schema: `user_value.avsc`

### Voice CDR Topic
- Subject: `telco-voice-cdrs-key` → Schema: `cdr_key.avsc`
- Subject: `telco-voice-cdrs-value` → Schema: `voice_cdr_value.avsc`

### Data CDR Topic
- Subject: `telco-data-cdrs-key` → Schema: `cdr_key.avsc`
- Subject: `telco-data-cdrs-value` → Schema: `data_cdr_value.avsc`

### SMS CDR Topic
- Subject: `telco-sms-cdrs-key` → Schema: `cdr_key.avsc`
- Subject: `telco-sms-cdrs-value` → Schema: `sms_cdr_value.avsc`

### VoIP CDR Topic
- Subject: `telco-voip-cdrs-key` → Schema: `cdr_key.avsc`
- Subject: `telco-voip-cdrs-value` → Schema: `voip_cdr_value.avsc`

### IMS CDR Topic
- Subject: `telco-ims-cdrs-key` → Schema: `cdr_key.avsc`
- Subject: `telco-ims-cdrs-value` → Schema: `ims_cdr_value.avsc`

### MMS CDR Topic
- Subject: `telco-mms-cdrs-key` → Schema: `cdr_key.avsc`
- Subject: `telco-mms-cdrs-value` → Schema: `mms_cdr_value.avsc`

### Roaming CDR Topic
- Subject: `telco-roaming-cdrs-key` → Schema: `cdr_key.avsc`
- Subject: `telco-roaming-cdrs-value` → Schema: `roaming_cdr_value.avsc`

### WiFi Calling CDR Topic
- Subject: `telco-wifi-calling-cdrs-key` → Schema: `cdr_key.avsc`
- Subject: `telco-wifi-calling-cdrs-value` → Schema: `wifi_calling_cdr_value.avsc`

## Common Fields

All CDR schemas include these anomaly-related fields:
- `is_anomaly` (boolean) - Whether this record contains an anomaly
- `anomaly_type` (string, nullable) - Type of anomaly if present

## Schema Evolution

These schemas support backward and forward compatibility:
- New optional fields can be added with default values
- Existing fields should not be removed or have their types changed
- Use union types with null for optional fields: `["null", "string"]`

## Manual Registration

To manually register a schema:

```bash
confluent schema-registry schema create \
  --subject telco-voice-cdrs-value \
  --schema schemas/voice_cdr_value.avsc \
  --type AVRO
```

## Validation

To validate a schema file:

```bash
# Using Confluent CLI
confluent schema-registry schema validate \
  --schema schemas/voice_cdr_value.avsc \
  --type AVRO
```
