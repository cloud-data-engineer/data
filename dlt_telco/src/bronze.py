# Databricks notebook source
# MAGIC %md
# MAGIC # Telco CDR Bronze Layer
# MAGIC 
# MAGIC This notebook defines the Bronze layer tables for the Telco CDR pipeline using Delta Live Tables (DLT).
# MAGIC 
# MAGIC The Bronze layer ingests raw data from Kafka topics with minimal transformation, preserving the original data.

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, BooleanType
import pyspark.sql.functions as F
import json
from pyspark.dbutils import DBUtils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Kafka Connection Configuration

# COMMAND ----------

# Retrieve Kafka connection parameters from Databricks secrets
# Get params
env_scope = spark.conf.get("env_scope")

# Get Kafka credentials from Databricks secret
dbutils = DBUtils(spark)
kafka_settings = json.loads(dbutils.secrets.get(scope=env_scope, key="telco-kafka"))

# Extract values from the secret
KAFKA_BOOTSTRAP_SERVERS = kafka_settings["bootstrap_server"]
api_key = kafka_settings["api_key"]
api_secret = kafka_settings["api_secret"]

# Configure Kafka security options
KAFKA_SECURITY_OPTIONS = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{api_key}' password='{api_secret}';",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Schemas

# COMMAND ----------

# User schema for parsing user data
user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("msisdn", StringType(), True),
    StructField("imsi", StringType(), True),
    StructField("imei", StringType(), True),
    StructField("plan_name", StringType(), True),
    StructField("data_limit_gb", IntegerType(), True),
    StructField("voice_minutes", IntegerType(), True),
    StructField("sms_count", IntegerType(), True),
    StructField("registration_date", StringType(), True),
    StructField("active", BooleanType(), True),
    StructField("location", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def read_from_kafka(topic):
    """Helper function to read from Kafka with consistent options"""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .options(**KAFKA_SECURITY_OPTIONS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

def get_bronze_table_properties():
    """Return standard bronze table properties"""
    return {
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.reset.allowed": "false"
    }

def get_standard_bronze_columns(df):
    """Return standardized bronze layer columns"""
    return df.select(
        col("key"),
        col("timestamp"),
        col("topic"),
        current_timestamp().alias("processing_time"),
        col("value").cast("string").alias("raw_data")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Users from Kafka

# COMMAND ----------

@dlt.table(
    name=f"{env_scope}_bronze.cdr.users",
    comment="Raw user data from Kafka",
    table_properties=get_bronze_table_properties()
)
def bronze_users():
    df = get_standard_bronze_columns(read_from_kafka("telco-users"))
    return df.withColumn("parsed_data", from_json(col("raw_data"), user_schema))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define CDR Ingestion Function

# COMMAND ----------

def create_bronze_cdr_table(cdr_type, topic_name):
    """Create a Bronze table for a specific CDR type"""
    
    @dlt.table(
        name=f"{env_scope}_bronze.cdr.{cdr_type}_cdrs",
        comment=f"Raw {cdr_type} CDR data from Kafka",
        table_properties=get_bronze_table_properties()
    )
    def bronze_cdr_table():
        return get_standard_bronze_columns(read_from_kafka(topic_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Tables for Each CDR Type

# COMMAND ----------

# Define CDR types and their corresponding topics
cdr_topics = {
    "voice": "telco-voice-cdrs",
    "data": "telco-data-cdrs",
    "sms": "telco-sms-cdrs",
    "voip": "telco-voip-cdrs",
    "ims": "telco-ims-cdrs"
}

# Create bronze tables for each CDR type
for cdr_type, topic in cdr_topics.items():
    create_bronze_cdr_table(cdr_type, topic)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Multiplexed Bronze Table (All CDR Types)

# COMMAND ----------

@dlt.table(
    name=f"{env_scope}_bronze.cdr.all_cdrs",
    comment="Multiplexed table containing all CDR types",
    table_properties=get_bronze_table_properties()
)
def bronze_all_cdrs():
    # Combine all topics with commas
    all_topics = ",".join(cdr_topics.values())
    return get_standard_bronze_columns(read_from_kafka(all_topics))
