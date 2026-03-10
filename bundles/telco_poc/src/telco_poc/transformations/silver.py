"""
Silver layer: parse raw JSON, apply schema, normalize timestamps, validate.

Each CDR type gets its own table matching its Avro schema.  Unix epoch
timestamps (seconds) are promoted to proper TIMESTAMP columns.
The is_anomaly flag from the generator is preserved for downstream use.

Users are upserted via AUTO CDC (SCD Type 1) so the table always holds
the current state of each subscriber.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType, BooleanType,
)

# ---------------------------------------------------------------------------
# Schema definitions (mirror the Avro schemas in src/cdr/data_generator/schemas)
# ---------------------------------------------------------------------------

USER_SCHEMA = StructType([
    StructField("user_id",          StringType()),
    StructField("msisdn",           StringType()),
    StructField("imsi",             StringType()),
    StructField("imei",             StringType()),
    StructField("plan_name",        StringType()),
    StructField("data_limit_gb",    IntegerType()),
    StructField("voice_minutes",    IntegerType()),
    StructField("sms_count",        IntegerType()),
    StructField("city",             StringType()),
    StructField("state",            StringType()),
    StructField("activation_date",  StringType()),
])

VOICE_SCHEMA = StructType([
    StructField("cdr_type",               StringType()),
    StructField("callEventStartTime",      LongType()),
    StructField("callEventEndTime",        LongType()),
    StructField("callDuration",            IntegerType()),
    StructField("callReferenceNumber",     StringType()),
    StructField("callingPartyNumber",      StringType()),
    StructField("calledPartyNumber",       StringType()),
    StructField("imsi",                    StringType()),
    StructField("imei",                    StringType()),
    StructField("mscID",                   StringType()),
    StructField("lac",                     StringType()),
    StructField("ci",                      StringType()),
    StructField("causeForTermination",     StringType()),
    StructField("chargeID",                StringType()),
    StructField("callChargeableDuration",  IntegerType()),
    StructField("tariffClass",             StringType()),
    StructField("callType",                StringType()),
    StructField("recordSequenceNumber",    IntegerType()),
    StructField("user_id",                 StringType()),
    StructField("is_anomaly",              BooleanType()),
    StructField("anomaly_type",            StringType()),
])

DATA_SCHEMA = StructType([
    StructField("cdr_type",             StringType()),
    StructField("recordType",           IntegerType()),
    StructField("recordOpeningTime",    LongType()),
    StructField("recordClosingTime",    LongType()),
    StructField("duration",             IntegerType()),
    StructField("imsi",                 StringType()),
    StructField("imei",                 StringType()),
    StructField("msisdn",               StringType()),
    StructField("dataVolumeUplink",     LongType()),
    StructField("dataVolumeDownlink",   LongType()),
    StructField("servingNodeAddress",   StringType()),
    StructField("apn",                  StringType()),
    StructField("pdnType",              StringType()),
    StructField("ratType",              StringType()),
    StructField("chargingID",           StringType()),
    StructField("qci",                  IntegerType()),
    StructField("recordSequenceNumber", IntegerType()),
    StructField("sgsn_address",         StringType()),
    StructField("user_id",              StringType()),
    StructField("is_anomaly",           BooleanType()),
    StructField("anomaly_type",         StringType()),
    StructField("causeForRecordClosing",StringType()),
])

SMS_SCHEMA = StructType([
    StructField("cdr_type",             StringType()),
    StructField("eventTimestamp",       LongType()),
    StructField("smsReferenceNumber",   StringType()),
    StructField("originatingNumber",    StringType()),
    StructField("destinationNumber",    StringType()),
    StructField("imsi",                 StringType()),
    StructField("smscAddress",          StringType()),
    StructField("messageSize",          IntegerType()),
    StructField("deliveryStatus",       StringType()),
    StructField("messageType",          StringType()),
    StructField("recordSequenceNumber", IntegerType()),
    StructField("chargeAmount",         DoubleType()),
    StructField("user_id",              StringType()),
    StructField("is_anomaly",           BooleanType()),
    StructField("anomaly_type",         StringType()),
    StructField("failureReason",        StringType()),
])

VOIP_SCHEMA = StructType([
    StructField("cdr_type",              StringType()),
    StructField("sessionStartTime",      LongType()),
    StructField("sessionEndTime",        LongType()),
    StructField("sessionDuration",       IntegerType()),
    StructField("callingPartyURI",       StringType()),
    StructField("calledPartyURI",        StringType()),
    StructField("codec",                 StringType()),
    StructField("callQualityIndicator",  IntegerType()),
    StructField("bytesTransferred",      LongType()),
    StructField("packetLoss",            DoubleType()),
    StructField("jitter",                DoubleType()),
    StructField("latency",               DoubleType()),
    StructField("chargeID",              StringType()),
    StructField("user_id",               StringType()),
    StructField("is_anomaly",            BooleanType()),
    StructField("anomaly_type",          StringType()),
])

IMS_SCHEMA = StructType([
    StructField("cdr_type",             StringType()),
    StructField("sessionStartTime",     LongType()),
    StructField("sessionEndTime",       LongType()),
    StructField("sessionDuration",      IntegerType()),
    StructField("originatingURI",       StringType()),
    StructField("destinationURI",       StringType()),
    StructField("serviceType",          StringType()),
    StructField("mediaTypes",           StringType()),
    StructField("networkType",          StringType()),
    StructField("qosClass",             StringType()),
    StructField("chargeID",             StringType()),
    StructField("user_id",              StringType()),
    StructField("is_anomaly",           BooleanType()),
    StructField("anomaly_type",         StringType()),
    StructField("terminationReason",    StringType()),
])

MMS_SCHEMA = StructType([
    StructField("cdr_type",             StringType()),
    StructField("eventTimestamp",       LongType()),
    StructField("mmsReferenceNumber",   StringType()),
    StructField("originatingNumber",    StringType()),
    StructField("destinationNumber",    StringType()),
    StructField("imsi",                 StringType()),
    StructField("mmscAddress",          StringType()),
    StructField("messageSize",          LongType()),
    StructField("contentType",          StringType()),
    StructField("deliveryStatus",       StringType()),
    StructField("chargeAmount",         DoubleType()),
    StructField("user_id",              StringType()),
    StructField("is_anomaly",           BooleanType()),
    StructField("anomaly_type",         StringType()),
    StructField("failureReason",        StringType()),
])

ROAMING_SCHEMA = StructType([
    StructField("cdr_type",             StringType()),
    StructField("callEventStartTime",   LongType()),
    StructField("callEventEndTime",     LongType()),
    StructField("callDuration",         IntegerType()),
    StructField("callingPartyNumber",   StringType()),
    StructField("calledPartyNumber",    StringType()),
    StructField("imsi",                 StringType()),
    StructField("imei",                 StringType()),
    StructField("roamingCountry",       StringType()),
    StructField("visitedNetwork",       StringType()),
    StructField("mcc",                  StringType()),
    StructField("mnc",                  StringType()),
    StructField("roamingType",          StringType()),
    StructField("serviceType",          StringType()),
    StructField("chargeAmount",         DoubleType()),
    StructField("roamingStatus",        StringType()),
    StructField("user_id",              StringType()),
    StructField("is_anomaly",           BooleanType()),
    StructField("anomaly_type",         StringType()),
    StructField("suspiciousActivity",   BooleanType()),
    StructField("fraudScore",           DoubleType()),
    StructField("connectionAllowed",    BooleanType()),
])

WIFI_CALLING_SCHEMA = StructType([
    StructField("cdr_type",             StringType()),
    StructField("sessionStartTime",     LongType()),
    StructField("sessionEndTime",       LongType()),
    StructField("sessionDuration",      IntegerType()),
    StructField("callingPartyNumber",   StringType()),
    StructField("calledPartyNumber",    StringType()),
    StructField("imsi",                 StringType()),
    StructField("wifiSSID",             StringType()),
    StructField("wifiAccessPoint",      StringType()),
    StructField("signalStrength",       IntegerType()),
    StructField("codec",                StringType()),
    StructField("callQuality",          IntegerType()),
    StructField("handoverToCell",       BooleanType()),
    StructField("chargeID",             StringType()),
    StructField("user_id",              StringType()),
    StructField("is_anomaly",           BooleanType()),
    StructField("anomaly_type",         StringType()),
])

# ---------------------------------------------------------------------------
# Catalog targets (sourced from pipeline configuration)
# ---------------------------------------------------------------------------

_bronze = spark.conf.get("bronze_catalog")
_silver = spark.conf.get("silver_catalog")
_schema = "telco_poc"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_json(bronze_table: str, schema: StructType):
    """Stream from a bronze table, parse the _raw_json column using schema."""
    return (
        spark.readStream.table(f"{_bronze}.{_schema}.{bronze_table}")
        .withColumn("_parsed", F.from_json(F.col("_raw_json"), schema))
        .select(
            F.col("_parsed.*"),
            F.col("_kafka_timestamp"),
            F.col("_ingested_at"),
        )
        # Drop rows where user_id could not be parsed (malformed JSON)
        .filter(F.col("user_id").isNotNull())
    )


def _epoch_to_ts(col_name: str) -> F.Column:
    """Convert a Unix-epoch-seconds LongType column to TIMESTAMP."""
    return F.to_timestamp(F.from_unixtime(F.col(col_name))).alias(col_name + "_ts")


# ---------------------------------------------------------------------------
# Silver — Users (AUTO CDC / SCD Type 1)
# ---------------------------------------------------------------------------

# Intermediate streaming view of parsed user events (feeds AUTO CDC below)
@dp.temporary_view(name="bronze_users_parsed")
def bronze_users_parsed():
    return (
        spark.readStream.table(f"{_bronze}.{_schema}.bronze_users")
        .withColumn("_parsed", F.from_json(F.col("_raw_json"), USER_SCHEMA))
        .select(
            F.col("_parsed.*"),
            F.col("_kafka_timestamp").alias("_updated_at"),
            F.col("_ingested_at"),
        )
        .filter(F.col("user_id").isNotNull())
    )


dp.create_streaming_table(f"{_silver}.{_schema}.silver_users")

dp.create_auto_cdc_flow(
    target=f"{_silver}.{_schema}.silver_users",
    source="bronze_users_parsed",
    keys=["user_id"],
    sequence_by="_updated_at",
    stored_as_scd_type=1,
)

# ---------------------------------------------------------------------------
# Silver — Voice CDRs
# ---------------------------------------------------------------------------

@dp.table(name=f"{_silver}.{_schema}.silver_voice_cdrs", cluster_by=["user_id", "callEventStartTime_ts"])
def silver_voice_cdrs():
    return (
        _parse_json("bronze_voice_cdrs", VOICE_SCHEMA)
        .withColumn("callEventStartTime_ts", _epoch_to_ts("callEventStartTime"))
        .withColumn("callEventEndTime_ts",   _epoch_to_ts("callEventEndTime"))
        .drop("callEventStartTime", "callEventEndTime")
    )

# ---------------------------------------------------------------------------
# Silver — Data CDRs
# ---------------------------------------------------------------------------

@dp.table(name=f"{_silver}.{_schema}.silver_data_cdrs", cluster_by=["user_id", "recordOpeningTime_ts"])
def silver_data_cdrs():
    return (
        _parse_json("bronze_data_cdrs", DATA_SCHEMA)
        .withColumn("recordOpeningTime_ts", _epoch_to_ts("recordOpeningTime"))
        .withColumn("recordClosingTime_ts", _epoch_to_ts("recordClosingTime"))
        # Convert bytes to MB for readability
        .withColumn("dataVolumeUplink_mb",   (F.col("dataVolumeUplink")   / 1_048_576).cast("double"))
        .withColumn("dataVolumeDownlink_mb", (F.col("dataVolumeDownlink") / 1_048_576).cast("double"))
        .drop("recordOpeningTime", "recordClosingTime")
    )

# ---------------------------------------------------------------------------
# Silver — SMS CDRs
# ---------------------------------------------------------------------------

@dp.table(name=f"{_silver}.{_schema}.silver_sms_cdrs", cluster_by=["user_id", "eventTimestamp_ts"])
def silver_sms_cdrs():
    return (
        _parse_json("bronze_sms_cdrs", SMS_SCHEMA)
        .withColumn("eventTimestamp_ts", _epoch_to_ts("eventTimestamp"))
        .drop("eventTimestamp")
    )

# ---------------------------------------------------------------------------
# Silver — VoIP CDRs
# ---------------------------------------------------------------------------

@dp.table(name=f"{_silver}.{_schema}.silver_voip_cdrs", cluster_by=["user_id", "sessionStartTime_ts"])
def silver_voip_cdrs():
    return (
        _parse_json("bronze_voip_cdrs", VOIP_SCHEMA)
        .withColumn("sessionStartTime_ts", _epoch_to_ts("sessionStartTime"))
        .withColumn("sessionEndTime_ts",   _epoch_to_ts("sessionEndTime"))
        .drop("sessionStartTime", "sessionEndTime")
    )

# ---------------------------------------------------------------------------
# Silver — IMS CDRs
# ---------------------------------------------------------------------------

@dp.table(name=f"{_silver}.{_schema}.silver_ims_cdrs", cluster_by=["user_id", "sessionStartTime_ts"])
def silver_ims_cdrs():
    return (
        _parse_json("bronze_ims_cdrs", IMS_SCHEMA)
        .withColumn("sessionStartTime_ts", _epoch_to_ts("sessionStartTime"))
        .withColumn("sessionEndTime_ts",   _epoch_to_ts("sessionEndTime"))
        .drop("sessionStartTime", "sessionEndTime")
    )

# ---------------------------------------------------------------------------
# Silver — MMS CDRs
# ---------------------------------------------------------------------------

@dp.table(name=f"{_silver}.{_schema}.silver_mms_cdrs", cluster_by=["user_id", "eventTimestamp_ts"])
def silver_mms_cdrs():
    return (
        _parse_json("bronze_mms_cdrs", MMS_SCHEMA)
        .withColumn("eventTimestamp_ts", _epoch_to_ts("eventTimestamp"))
        # Convert message size bytes → KB
        .withColumn("messageSize_kb", (F.col("messageSize") / 1024.0).cast("double"))
        .drop("eventTimestamp")
    )

# ---------------------------------------------------------------------------
# Silver — Roaming CDRs
# ---------------------------------------------------------------------------

@dp.table(name=f"{_silver}.{_schema}.silver_roaming_cdrs", cluster_by=["user_id", "callEventStartTime_ts"])
def silver_roaming_cdrs():
    return (
        _parse_json("bronze_roaming_cdrs", ROAMING_SCHEMA)
        .withColumn("callEventStartTime_ts", _epoch_to_ts("callEventStartTime"))
        .withColumn("callEventEndTime_ts",   _epoch_to_ts("callEventEndTime"))
        .drop("callEventStartTime", "callEventEndTime")
    )

# ---------------------------------------------------------------------------
# Silver — WiFi Calling CDRs
# ---------------------------------------------------------------------------

@dp.table(name=f"{_silver}.{_schema}.silver_wifi_calling_cdrs", cluster_by=["user_id", "sessionStartTime_ts"])
def silver_wifi_calling_cdrs():
    return (
        _parse_json("bronze_wifi_calling_cdrs", WIFI_CALLING_SCHEMA)
        .withColumn("sessionStartTime_ts", _epoch_to_ts("sessionStartTime"))
        .withColumn("sessionEndTime_ts",   _epoch_to_ts("sessionEndTime"))
        .drop("sessionStartTime", "sessionEndTime")
    )
