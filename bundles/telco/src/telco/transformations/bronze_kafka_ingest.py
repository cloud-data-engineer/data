"""
Bronze layer — raw Kafka ingestion for all 9 telco CDR topics.

One streaming table per topic, generated via a factory loop.
No parsing or transformation — raw Kafka value stored as raw_data STRING.
Type casting and schema enforcement happen in silver.

Metadata columns on every table:
  _topic              Kafka topic name
  _kafka_partition    Kafka partition number
  _kafka_offset       Kafka offset within the partition
  _kafka_timestamp    Message timestamp from the Kafka broker
  _ingested_at        Wall-clock time this row was ingested by the pipeline

Secrets are read from the fixed `dev_kafka` scope (Kafka credentials are
environment-independent within this workspace — the scope name is stable).
The general env_scope variable is available for other secrets if needed.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Kafka connection — built once, shared across all topic readers
# ---------------------------------------------------------------------------

_bootstrap = dbutils.secrets.get(scope="dev_kafka", key="bootstrap_servers")
_api_key    = dbutils.secrets.get(scope="dev_kafka", key="api_key")
_api_secret = dbutils.secrets.get(scope="dev_kafka", key="api_secret")

# IMPORTANT: Databricks Serverless uses a shaded Kafka client.
# The JAAS config MUST use the kafkashaded. prefix —
# org.apache.kafka...PlainLoginModule raises "No LoginModule found" at runtime.
_JAAS = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
    f' required username="{_api_key}" password="{_api_secret}";'
)

_KAFKA_BASE_OPTS = {
    "kafka.bootstrap.servers":    _bootstrap,
    "kafka.security.protocol":    "SASL_SSL",
    "kafka.sasl.mechanism":       "PLAIN",
    "kafka.sasl.jaas.config":     _JAAS,
    # Confluent Cloud recommended timeouts
    "kafka.session.timeout.ms":   "45000",
    "kafka.request.timeout.ms":   "60000",
    "startingOffsets":            "earliest",
    "failOnDataLoss":             "false",
}

# ---------------------------------------------------------------------------
# Topic → table mapping
# Table name = entity only — no layer prefix (catalog communicates the layer)
# ---------------------------------------------------------------------------

TOPICS = {
    "telco-users":            "users",
    "telco-voice-cdrs":       "voice_cdrs",
    "telco-data-cdrs":        "data_cdrs",
    "telco-sms-cdrs":         "sms_cdrs",
    "telco-voip-cdrs":        "voip_cdrs",
    "telco-ims-cdrs":         "ims_cdrs",
    "telco-mms-cdrs":         "mms_cdrs",
    "telco-roaming-cdrs":     "roaming_cdrs",
    "telco-wifi-calling-cdrs":"wifi_calling_cdrs",
}

# ---------------------------------------------------------------------------
# Factory function — required to correctly bind loop variables.
# A plain nested def captures variables by reference (Python late binding),
# so all tables would read from the last topic in the loop.
# The factory function forces early binding via default argument values.
# ---------------------------------------------------------------------------

def _make_bronze(topic: str, table_name: str) -> None:
    @dp.table(
        name=table_name,
        comment=f"Raw {topic} events from Confluent Kafka — no parsing applied",
        table_properties={
            "quality": "bronze",
            "pipelines.reset.allowed": "false",
        },
    )
    def _table():
        reader = spark.readStream.format("kafka")
        for k, v in {**_KAFKA_BASE_OPTS, "subscribe": topic}.items():
            reader = reader.option(k, v)

        return (
            reader.load()
            .select(
                F.col("value").cast("string").alias("raw_data"),
                F.col("topic").alias("_topic"),
                F.col("partition").alias("_kafka_partition"),
                F.col("offset").alias("_kafka_offset"),
                F.col("timestamp").alias("_kafka_timestamp"),
                F.current_timestamp().alias("_ingested_at"),
            )
        )


for _topic, _table_name in TOPICS.items():
    _make_bronze(_topic, _table_name)
