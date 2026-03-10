"""
Bronze layer: raw JSON ingestion from Confluent Kafka topics.

One streaming table per topic. Each table captures the raw JSON value,
the Kafka message key, topic metadata, and an ingest timestamp.
No parsing or transformation — that happens in silver.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Kafka connection options — sourced from pipeline configuration
# ---------------------------------------------------------------------------

def _kafka_options(topic: str) -> dict:
    bootstrap_servers = dbutils.secrets.get("dev_kafka", "bootstrap_servers")
    api_key           = dbutils.secrets.get("dev_kafka", "api_key")
    api_secret        = dbutils.secrets.get("dev_kafka", "api_secret")
    starting_offsets  = spark.conf.get("kafka_starting_offsets", "latest")
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
        f' required username="{api_key}" password="{api_secret}";'
    )
    return {
        "kafka.bootstrap.servers": bootstrap_servers,
        "subscribe": topic,
        "startingOffsets": starting_offsets,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        # Confluent Cloud recommended settings
        "kafka.session.timeout.ms": "45000",
        "kafka.request.timeout.ms": "60000",
        "failOnDataLoss": "false",
    }


def _bronze_kafka(topic_conf_key: str, table_name: str):
    """Read a Kafka topic into a raw bronze streaming table."""
    topic = spark.conf.get(topic_conf_key)
    opts = _kafka_options(topic)

    reader = spark.readStream.format("kafka")
    for k, v in opts.items():
        reader = reader.option(k, v)

    return (
        reader.load()
        .selectExpr(
            "CAST(key AS STRING)   AS _kafka_key",
            "CAST(value AS STRING) AS _raw_json",
            "topic                 AS _topic",
            "partition             AS _partition",
            "offset                AS _offset",
            "timestamp             AS _kafka_timestamp",
        )
        .withColumn("_ingested_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# Bronze tables
# ---------------------------------------------------------------------------

@dp.table(name="bronze_users", cluster_by=["_ingested_at"])
def bronze_users():
    return _bronze_kafka("topic_users", "bronze_users")


@dp.table(name="bronze_voice_cdrs", cluster_by=["_ingested_at"])
def bronze_voice_cdrs():
    return _bronze_kafka("topic_voice", "bronze_voice_cdrs")


@dp.table(name="bronze_data_cdrs", cluster_by=["_ingested_at"])
def bronze_data_cdrs():
    return _bronze_kafka("topic_data", "bronze_data_cdrs")


@dp.table(name="bronze_sms_cdrs", cluster_by=["_ingested_at"])
def bronze_sms_cdrs():
    return _bronze_kafka("topic_sms", "bronze_sms_cdrs")


@dp.table(name="bronze_voip_cdrs", cluster_by=["_ingested_at"])
def bronze_voip_cdrs():
    return _bronze_kafka("topic_voip", "bronze_voip_cdrs")


@dp.table(name="bronze_ims_cdrs", cluster_by=["_ingested_at"])
def bronze_ims_cdrs():
    return _bronze_kafka("topic_ims", "bronze_ims_cdrs")


@dp.table(name="bronze_mms_cdrs", cluster_by=["_ingested_at"])
def bronze_mms_cdrs():
    return _bronze_kafka("topic_mms", "bronze_mms_cdrs")


@dp.table(name="bronze_roaming_cdrs", cluster_by=["_ingested_at"])
def bronze_roaming_cdrs():
    return _bronze_kafka("topic_roaming", "bronze_roaming_cdrs")


@dp.table(name="bronze_wifi_calling_cdrs", cluster_by=["_ingested_at"])
def bronze_wifi_calling_cdrs():
    return _bronze_kafka("topic_wifi_calling", "bronze_wifi_calling_cdrs")
