# Databricks notebook source
# MAGIC %md
# MAGIC # Telco CDR Silver Layer
# MAGIC
# MAGIC This notebook defines the Silver layer tables for the Telco CDR pipeline using Delta Live Tables (DLT).
# MAGIC
# MAGIC The Silver layer performs data validation, cleansing, deduplication, and standardization.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, from_json, current_timestamp, lit, hash, concat_ws, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType, TimestampType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDR Schemas

# COMMAND ----------

# Voice CDR schema
voice_cdr_schema = StructType([
    StructField("callEventStartTime", LongType(), True),
    StructField("callEventEndTime", LongType(), True),
    StructField("callDuration", IntegerType(), True),
    StructField("callReferenceNumber", StringType(), True),
    StructField("callingPartyNumber", StringType(), True),
    StructField("calledPartyNumber", StringType(), True),
    StructField("imsi", StringType(), True),
    StructField("imei", StringType(), True),
    StructField("mscID", StringType(), True),
    StructField("lac", StringType(), True),
    StructField("ci", StringType(), True),
    StructField("causeForTermination", StringType(), True),
    StructField("chargeID", StringType(), True),
    StructField("callChargeableDuration", IntegerType(), True),
    StructField("tariffClass", StringType(), True),
    StructField("user_id", StringType(), True)
])

# Data CDR schema
data_cdr_schema = StructType([
    StructField("recordType", IntegerType(), True),
    StructField("recordOpeningTime", LongType(), True),
    StructField("recordClosingTime", LongType(), True),
    StructField("duration", IntegerType(), True),
    StructField("imsi", StringType(), True),
    StructField("imei", StringType(), True),
    StructField("msisdn", StringType(), True),
    StructField("dataVolumeUplink", LongType(), True),
    StructField("dataVolumeDownlink", LongType(), True),
    StructField("servingNodeAddress", StringType(), True),
    StructField("apn", StringType(), True),
    StructField("pdnType", StringType(), True),
    StructField("ratType", StringType(), True),
    StructField("chargingID", StringType(), True),
    StructField("qci", IntegerType(), True),
    StructField("user_id", StringType(), True)
])

# SMS CDR schema
sms_cdr_schema = StructType([
    StructField("eventTimestamp", LongType(), True),
    StructField("smsReferenceNumber", StringType(), True),
    StructField("originatingNumber", StringType(), True),
    StructField("destinationNumber", StringType(), True),
    StructField("imsi", StringType(), True),
    StructField("smscAddress", StringType(), True),
    StructField("messageSize", IntegerType(), True),
    StructField("deliveryStatus", StringType(), True),
    StructField("chargeAmount", DoubleType(), True),
    StructField("user_id", StringType(), True)
])

# VoIP CDR schema
voip_cdr_schema = StructType([
    StructField("sessionStartTime", LongType(), True),
    StructField("sessionEndTime", LongType(), True),
    StructField("sessionDuration", IntegerType(), True),
    StructField("callingPartyURI", StringType(), True),
    StructField("calledPartyURI", StringType(), True),
    StructField("codec", StringType(), True),
    StructField("callQualityIndicator", IntegerType(), True),
    StructField("bytesTransferred", LongType(), True),
    StructField("packetLoss", DoubleType(), True),
    StructField("jitter", DoubleType(), True),
    StructField("latency", DoubleType(), True),
    StructField("chargeID", StringType(), True),
    StructField("user_id", StringType(), True)
])

# IMS CDR schema
ims_cdr_schema = StructType([
    StructField("sessionStartTime", LongType(), True),
    StructField("sessionEndTime", LongType(), True),
    StructField("sessionDuration", IntegerType(), True),
    StructField("originatingURI", StringType(), True),
    StructField("destinationURI", StringType(), True),
    StructField("serviceType", StringType(), True),
    StructField("mediaTypes", StringType(), True),
    StructField("networkType", StringType(), True),
    StructField("qosClass", StringType(), True),
    StructField("chargeID", StringType(), True),
    StructField("user_id", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

# Get env_scope for catalog naming
env_scope = spark.conf.get("env_scope")

def get_silver_table_properties():
    """Return standard silver table properties"""
    return {
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    }

def add_silver_metadata(df):
    """Add standard silver layer metadata columns"""
    return df.withColumn("silver_processing_time", current_timestamp())



# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Users Table

# COMMAND ----------

@dlt.table(
    name=f"{env_scope}_silver.cdr.users",
    comment="Cleansed and deduplicated user data",
    table_properties=get_silver_table_properties()
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_msisdn", "msisdn IS NOT NULL AND length(msisdn) >= 10")
def silver_users():
    df = dlt.read_stream(f"{env_scope}_bronze.cdr.users")
    
    # Extract parsed data and add metadata
    silver_df = df.select(
        col("parsed_data.user_id").alias("user_id"),
        col("parsed_data.msisdn").alias("msisdn"),
        col("parsed_data.imsi").alias("imsi"),
        col("parsed_data.imei").alias("imei"),
        col("parsed_data.plan_name").alias("plan_name"),
        col("parsed_data.data_limit_gb").alias("data_limit_gb"),
        col("parsed_data.voice_minutes").alias("voice_minutes"),
        col("parsed_data.sms_count").alias("sms_count"),
        col("parsed_data.registration_date").alias("registration_date"),
        col("parsed_data.active").alias("active"),
        col("parsed_data.location.city").alias("city"),
        col("parsed_data.location.state").alias("state"),
        col("processing_time").alias("bronze_processing_time")
    )
    
    return add_silver_metadata(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Data CDRs

# COMMAND ----------

@dlt.table(
    name=f"{env_scope}_silver.cdr.data_cdrs",
    comment="Cleansed and validated data CDR data",
    table_properties=get_silver_table_properties()
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_bytes", "data_volume_uplink >= 0 AND data_volume_downlink >= 0")
def silver_data_cdrs():
    df = dlt.read_stream(f"{env_scope}_bronze.cdr.data_cdrs")
    
    # Parse JSON data
    parsed_df = df.withColumn("parsed_data", from_json(col("raw_data"), data_cdr_schema))
    
    # Extract and validate data
    silver_df = parsed_df.select(
        col("parsed_data.user_id").alias("user_id"),
        col("parsed_data.imsi").alias("imsi"),
        col("parsed_data.imei").alias("imei"),
        col("parsed_data.msisdn").alias("msisdn"),
        F.from_unixtime(col("parsed_data.recordOpeningTime")).cast(TimestampType()).alias("session_start_time"),
        F.from_unixtime(col("parsed_data.recordClosingTime")).cast(TimestampType()).alias("session_end_time"),
        col("parsed_data.duration").alias("duration_seconds"),
        col("parsed_data.dataVolumeUplink").alias("data_volume_uplink"),
        col("parsed_data.dataVolumeDownlink").alias("data_volume_downlink"),
        (col("parsed_data.dataVolumeUplink") + col("parsed_data.dataVolumeDownlink")).alias("total_bytes"),
        col("parsed_data.apn").alias("apn"),
        col("parsed_data.servingNodeAddress").alias("serving_node_address"),
        col("parsed_data.pdnType").alias("pdn_type"),
        col("parsed_data.ratType").alias("rat_type"),
        col("parsed_data.qci").alias("qci"),
        col("processing_time").alias("bronze_processing_time"),
        lit("data").alias("cdr_type")
    ).filter(col("parsed_data").isNotNull())
    
    return add_silver_metadata(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Voice CDRs

# COMMAND ----------

@dlt.table(
    name=f"{env_scope}_silver.cdr.voice_cdrs",
    comment="Cleansed and validated voice CDR data",
    table_properties=get_silver_table_properties()
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_duration", "call_duration >= 0")
def silver_voice_cdrs():
    df = dlt.read_stream(f"{env_scope}_bronze.cdr.voice_cdrs")
    
    # Parse JSON data
    parsed_df = df.withColumn("parsed_data", from_json(col("raw_data"), voice_cdr_schema))
    
    # Extract and validate data
    silver_df = parsed_df.select(
        col("parsed_data.user_id").alias("user_id"),
        col("parsed_data.imsi").alias("imsi"),
        col("parsed_data.imei").alias("imei"),
        col("parsed_data.callingPartyNumber").alias("calling_party_number"),
        col("parsed_data.calledPartyNumber").alias("called_party_number"),
        F.from_unixtime(col("parsed_data.callEventStartTime")).cast(TimestampType()).alias("call_start_time"),
        F.from_unixtime(col("parsed_data.callEventEndTime")).cast(TimestampType()).alias("call_end_time"),
        col("parsed_data.callDuration").alias("call_duration"),
        col("parsed_data.callReferenceNumber").alias("call_reference_number"),
        col("parsed_data.mscID").alias("msc_id"),
        col("parsed_data.lac").alias("lac"),
        col("parsed_data.ci").alias("ci"),
        col("parsed_data.causeForTermination").alias("cause_for_termination"),
        col("parsed_data.chargeID").alias("charge_id"),
        col("parsed_data.callChargeableDuration").alias("call_chargeable_duration"),
        col("parsed_data.tariffClass").alias("tariff_class"),
        col("processing_time").alias("bronze_processing_time"),
        lit("voice").alias("cdr_type")
    ).filter(col("parsed_data").isNotNull())
    
    return add_silver_metadata(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver SMS CDRs

# COMMAND ----------

@dlt.table(
    name=f"{env_scope}_silver.cdr.sms_cdrs",
    comment="Cleansed and validated SMS CDR data",
    table_properties=get_silver_table_properties()
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_message_size", "message_size > 0")
def silver_sms_cdrs():
    df = dlt.read_stream(f"{env_scope}_bronze.cdr.sms_cdrs")
    
    # Parse JSON data
    parsed_df = df.withColumn("parsed_data", from_json(col("raw_data"), sms_cdr_schema))
    
    # Extract and validate data
    silver_df = parsed_df.select(
        col("parsed_data.user_id").alias("user_id"),
        col("parsed_data.imsi").alias("imsi"),
        col("parsed_data.originatingNumber").alias("originating_number"),
        col("parsed_data.destinationNumber").alias("destination_number"),
        F.from_unixtime(col("parsed_data.eventTimestamp")).cast(TimestampType()).alias("event_timestamp"),
        col("parsed_data.messageSize").alias("message_size"),
        col("parsed_data.smsReferenceNumber").alias("sms_reference_number"),
        col("parsed_data.smscAddress").alias("smsc_address"),
        col("parsed_data.deliveryStatus").alias("delivery_status"),
        col("parsed_data.chargeAmount").alias("charge_amount"),
        col("processing_time").alias("bronze_processing_time"),
        lit("sms").alias("cdr_type")
    ).filter(col("parsed_data").isNotNull())
    
    return add_silver_metadata(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver VoIP CDRs

# COMMAND ----------

@dlt.table(
    name=f"{env_scope}_silver.cdr.voip_cdrs",
    comment="Cleansed and validated VoIP CDR data",
    table_properties=get_silver_table_properties()
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_duration", "session_duration >= 0")
def silver_voip_cdrs():
    df = dlt.read(f"{env_scope}_bronze.cdr.voip_cdrs")
    
    # Parse JSON data
    parsed_df = df.withColumn("parsed_data", from_json(col("raw_data"), voip_cdr_schema))
    
    # Extract and validate data
    silver_df = parsed_df.select(
        col("parsed_data.user_id").alias("user_id"),
        col("parsed_data.callingPartyURI").alias("calling_party_uri"),
        col("parsed_data.calledPartyURI").alias("called_party_uri"),
        F.from_unixtime(col("parsed_data.sessionStartTime")).cast(TimestampType()).alias("session_start_time"),
        F.from_unixtime(col("parsed_data.sessionEndTime")).cast(TimestampType()).alias("session_end_time"),
        col("parsed_data.sessionDuration").alias("session_duration"),
        col("parsed_data.codec").alias("codec"),
        col("parsed_data.callQualityIndicator").alias("call_quality_indicator"),
        col("parsed_data.bytesTransferred").alias("bytes_transferred"),
        col("parsed_data.packetLoss").alias("packet_loss"),
        col("parsed_data.jitter").alias("jitter"),
        col("parsed_data.latency").alias("latency"),
        col("parsed_data.chargeID").alias("charge_id"),
        col("processing_time").alias("bronze_processing_time"),
        lit("voip").alias("cdr_type")
    ).filter(col("parsed_data").isNotNull())

    return add_silver_metadata(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver IMS CDRs

# COMMAND ----------

@dlt.table(
    name=f"{env_scope}_silver.cdr.ims_cdrs",
    comment="Cleansed and validated IMS CDR data",
    table_properties=get_silver_table_properties()
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_duration", "session_duration >= 0")
def silver_ims_cdrs():
    df = dlt.read(f"{env_scope}_bronze.cdr.ims_cdrs")
    
    # Parse JSON data
    parsed_df = df.withColumn("parsed_data", from_json(col("raw_data"), ims_cdr_schema))
    
    # Extract and validate data
    silver_df = parsed_df.select(
        col("parsed_data.user_id").alias("user_id"),
        col("parsed_data.serviceType").alias("service_type"),
        F.from_unixtime(col("parsed_data.sessionStartTime")).cast(TimestampType()).alias("session_start_time"),
        F.from_unixtime(col("parsed_data.sessionEndTime")).cast(TimestampType()).alias("session_end_time"),
        col("parsed_data.sessionDuration").alias("session_duration"),
        col("parsed_data.originatingURI").alias("originating_uri"),
        col("parsed_data.destinationURI").alias("destination_uri"),
        col("parsed_data.mediaTypes").alias("media_types"),
        col("parsed_data.networkType").alias("network_type"),
        col("parsed_data.qosClass").alias("qos_class"),
        col("parsed_data.chargeID").alias("charge_id"),
        col("processing_time").alias("bronze_processing_time"),
        lit("ims").alias("cdr_type")
    ).filter(col("parsed_data").isNotNull())

    return add_silver_metadata(silver_df)
