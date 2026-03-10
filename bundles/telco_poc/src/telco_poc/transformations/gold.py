"""
Gold layer: business-ready aggregations for the Customer 360 dashboard.

Tables
------
gold_customer_360        — one row per subscriber, lifetime + recent usage summary
gold_daily_cdr_summary   — daily CDR counts & durations by user + type
gold_roaming_summary     — roaming activity and revenue per user
gold_anomaly_summary     — anomaly/fraud signal counts per user
"""
import functools

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Catalog targets (sourced from pipeline configuration)
# ---------------------------------------------------------------------------

_silver = spark.conf.get("silver_catalog")
_gold   = spark.conf.get("gold_catalog")
_schema = "telco_poc"

def _s(table: str) -> str:
    """Fully-qualified silver table name."""
    return f"{_silver}.{_schema}.{table}"

def _g(table: str) -> str:
    """Fully-qualified gold table name."""
    return f"{_gold}.{_schema}.{table}"


# ---------------------------------------------------------------------------
# gold_customer_360
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name=_g("gold_customer_360"),
    cluster_by=["user_id"],
    comment="One row per subscriber with lifetime usage totals and last-activity date",
)
def gold_customer_360():
    users = spark.read.table(_s("silver_users"))

    voice = (
        spark.read.table(_s("silver_voice_cdrs"))
        .groupBy("user_id")
        .agg(
            F.count("*").alias("voice_call_count"),
            F.sum("callDuration").alias("voice_total_seconds"),
            F.max("callEventStartTime_ts").alias("voice_last_call_ts"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("voice_anomaly_count"),
        )
    )

    data = (
        spark.read.table(_s("silver_data_cdrs"))
        .groupBy("user_id")
        .agg(
            F.count("*").alias("data_session_count"),
            F.sum("dataVolumeUplink_mb").alias("data_uplink_mb"),
            F.sum("dataVolumeDownlink_mb").alias("data_downlink_mb"),
            F.max("recordOpeningTime_ts").alias("data_last_session_ts"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("data_anomaly_count"),
        )
    )

    sms = (
        spark.read.table(_s("silver_sms_cdrs"))
        .groupBy("user_id")
        .agg(
            F.count("*").alias("sms_tx_count"),
            F.sum("chargeAmount").alias("sms_total_charge"),
            F.max("eventTimestamp_ts").alias("sms_last_ts"),
            F.sum(F.when(F.col("deliveryStatus") == "failed", 1).otherwise(0)).alias("sms_failed_count"),
        )
    )

    mms = (
        spark.read.table(_s("silver_mms_cdrs"))
        .groupBy("user_id")
        .agg(
            F.count("*").alias("mms_count"),
            F.sum("chargeAmount").alias("mms_total_charge"),
            F.sum("messageSize_kb").alias("mms_total_size_kb"),
        )
    )

    roaming = (
        spark.read.table(_s("silver_roaming_cdrs"))
        .groupBy("user_id")
        .agg(
            F.count("*").alias("roaming_event_count"),
            F.sum("chargeAmount").alias("roaming_total_charge"),
            F.countDistinct("roamingCountry").alias("roaming_countries_visited"),
            F.max("callEventStartTime_ts").alias("roaming_last_ts"),
        )
    )

    voip = (
        spark.read.table(_s("silver_voip_cdrs"))
        .groupBy("user_id")
        .agg(
            F.count("*").alias("voip_session_count"),
            F.avg("callQualityIndicator").alias("voip_avg_quality"),
            F.avg("packetLoss").alias("voip_avg_packet_loss_pct"),
            F.avg("latency").alias("voip_avg_latency_ms"),
        )
    )

    wifi = (
        spark.read.table(_s("silver_wifi_calling_cdrs"))
        .groupBy("user_id")
        .agg(
            F.count("*").alias("wifi_call_count"),
            F.avg("callQuality").alias("wifi_avg_quality"),
            F.avg("signalStrength").alias("wifi_avg_signal_dbm"),
            F.sum(F.when(F.col("handoverToCell"), 1).otherwise(0)).alias("wifi_handover_count"),
        )
    )

    return (
        users
        .join(voice,   "user_id", "left")
        .join(data,    "user_id", "left")
        .join(sms,     "user_id", "left")
        .join(mms,     "user_id", "left")
        .join(roaming, "user_id", "left")
        .join(voip,    "user_id", "left")
        .join(wifi,    "user_id", "left")
        .select(
            # Identity
            "user_id", "msisdn", "imsi", "plan_name",
            "city", "state", "activation_date",
            "data_limit_gb", "voice_minutes", "sms_count",
            # Voice
            F.coalesce(F.col("voice_call_count"),    F.lit(0)).alias("voice_call_count"),
            F.coalesce(F.col("voice_total_seconds"), F.lit(0)).alias("voice_total_seconds"),
            (F.coalesce(F.col("voice_total_seconds"), F.lit(0)) / 60.0).alias("voice_total_minutes"),
            "voice_last_call_ts",
            # Data
            F.coalesce(F.col("data_session_count"),   F.lit(0)).alias("data_session_count"),
            F.coalesce(F.col("data_uplink_mb"),        F.lit(0.0)).alias("data_uplink_mb"),
            F.coalesce(F.col("data_downlink_mb"),      F.lit(0.0)).alias("data_downlink_mb"),
            ((F.coalesce(F.col("data_uplink_mb"), F.lit(0.0)) + F.coalesce(F.col("data_downlink_mb"), F.lit(0.0))) / 1024.0).alias("data_total_gb"),
            "data_last_session_ts",
            # SMS / MMS
            F.coalesce(F.col("sms_tx_count"),      F.lit(0)).alias("sms_count_actual"),
            F.coalesce(F.col("sms_total_charge"),  F.lit(0.0)).alias("sms_total_charge"),
            F.coalesce(F.col("sms_failed_count"),  F.lit(0)).alias("sms_failed_count"),
            "sms_last_ts",
            F.coalesce(F.col("mms_count"),         F.lit(0)).alias("mms_count"),
            F.coalesce(F.col("mms_total_charge"),  F.lit(0.0)).alias("mms_total_charge"),
            # Roaming
            F.coalesce(F.col("roaming_event_count"),       F.lit(0)).alias("roaming_event_count"),
            F.coalesce(F.col("roaming_total_charge"),      F.lit(0.0)).alias("roaming_total_charge"),
            F.coalesce(F.col("roaming_countries_visited"), F.lit(0)).alias("roaming_countries_visited"),
            "roaming_last_ts",
            # VoIP quality
            F.coalesce(F.col("voip_session_count"), F.lit(0)).alias("voip_session_count"),
            "voip_avg_quality",
            "voip_avg_packet_loss_pct",
            "voip_avg_latency_ms",
            # WiFi calling
            F.coalesce(F.col("wifi_call_count"), F.lit(0)).alias("wifi_call_count"),
            "wifi_avg_quality",
            "wifi_avg_signal_dbm",
            # Total revenue
            (
                F.coalesce(F.col("sms_total_charge"),     F.lit(0.0)) +
                F.coalesce(F.col("mms_total_charge"),     F.lit(0.0)) +
                F.coalesce(F.col("roaming_total_charge"), F.lit(0.0))
            ).alias("total_charge_usd"),
            # Anomaly signals
            F.coalesce(F.col("voice_anomaly_count"), F.lit(0)).alias("voice_anomaly_count"),
            F.coalesce(F.col("data_anomaly_count"),  F.lit(0)).alias("data_anomaly_count"),
        )
    )


# ---------------------------------------------------------------------------
# gold_daily_cdr_summary
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name=_g("gold_daily_cdr_summary"),
    cluster_by=["event_date", "user_id"],
    comment="Daily CDR counts and durations by subscriber",
)
def gold_daily_cdr_summary():
    voice = (
        spark.read.table(_s("silver_voice_cdrs"))
        .withColumn("event_date", F.to_date("callEventStartTime_ts"))
        .groupBy("user_id", "event_date")
        .agg(
            F.count("*").alias("voice_calls"),
            F.sum("callDuration").alias("voice_seconds"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("voice_anomalies"),
        )
    )

    data = (
        spark.read.table(_s("silver_data_cdrs"))
        .withColumn("event_date", F.to_date("recordOpeningTime_ts"))
        .groupBy("user_id", "event_date")
        .agg(
            F.count("*").alias("data_sessions"),
            F.sum("dataVolumeDownlink_mb").alias("data_downlink_mb"),
            F.sum("dataVolumeUplink_mb").alias("data_uplink_mb"),
        )
    )

    sms = (
        spark.read.table(_s("silver_sms_cdrs"))
        .withColumn("event_date", F.to_date("eventTimestamp_ts"))
        .groupBy("user_id", "event_date")
        .agg(
            F.count("*").alias("sms_messages"),
            F.sum("chargeAmount").alias("sms_charge"),
        )
    )

    all_dates = (
        voice.select("user_id", "event_date")
        .union(data.select("user_id", "event_date"))
        .union(sms.select("user_id", "event_date"))
        .distinct()
    )

    return (
        all_dates
        .join(voice, ["user_id", "event_date"], "left")
        .join(data,  ["user_id", "event_date"], "left")
        .join(sms,   ["user_id", "event_date"], "left")
        .select(
            "user_id", "event_date",
            F.coalesce(F.col("voice_calls"),     F.lit(0)).alias("voice_calls"),
            F.coalesce(F.col("voice_seconds"),   F.lit(0)).alias("voice_seconds"),
            F.coalesce(F.col("voice_anomalies"), F.lit(0)).alias("voice_anomalies"),
            F.coalesce(F.col("data_sessions"),   F.lit(0)).alias("data_sessions"),
            F.coalesce(F.col("data_downlink_mb"),F.lit(0.0)).alias("data_downlink_mb"),
            F.coalesce(F.col("data_uplink_mb"),  F.lit(0.0)).alias("data_uplink_mb"),
            F.coalesce(F.col("sms_messages"),    F.lit(0)).alias("sms_messages"),
            F.coalesce(F.col("sms_charge"),      F.lit(0.0)).alias("sms_charge"),
        )
        .orderBy("event_date", "user_id")
    )


# ---------------------------------------------------------------------------
# gold_roaming_summary
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name=_g("gold_roaming_summary"),
    cluster_by=["user_id"],
    comment="Roaming activity, charges, and fraud signals per subscriber",
)
def gold_roaming_summary():
    return (
        spark.read.table(_s("silver_roaming_cdrs"))
        .withColumn("event_date", F.to_date("callEventStartTime_ts"))
        .groupBy("user_id")
        .agg(
            F.count("*").alias("roaming_events"),
            F.countDistinct("roamingCountry").alias("countries_visited"),
            F.collect_set("roamingCountry").alias("countries_list"),
            F.sum("chargeAmount").alias("total_roaming_charge"),
            F.sum("callDuration").alias("total_roaming_seconds"),
            F.avg("fraudScore").alias("avg_fraud_score"),
            F.sum(F.when(F.col("suspiciousActivity") == True, 1).otherwise(0)).alias("suspicious_events"),
            F.sum(F.when(F.col("connectionAllowed") == False, 1).otherwise(0)).alias("blocked_connections"),
            F.max("callEventStartTime_ts").alias("last_roaming_ts"),
        )
    )


# ---------------------------------------------------------------------------
# gold_anomaly_summary
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name=_g("gold_anomaly_summary"),
    cluster_by=["user_id"],
    comment="Anomaly signal counts per subscriber across all CDR types",
)
def gold_anomaly_summary():
    def anomalies(table, ts_col, type_label):
        return (
            spark.read.table(_s(table))
            .filter(F.col("is_anomaly") == True)
            .withColumn("event_date", F.to_date(ts_col))
            .groupBy("user_id")
            .agg(
                F.count("*").alias(f"{type_label}_anomaly_count"),
                F.max("event_date").alias(f"{type_label}_last_anomaly_date"),
                F.first("anomaly_type").alias(f"{type_label}_anomaly_type_sample"),
            )
        )

    voice   = anomalies("silver_voice_cdrs",       "callEventStartTime_ts", "voice")
    data    = anomalies("silver_data_cdrs",         "recordOpeningTime_ts",  "data")
    sms     = anomalies("silver_sms_cdrs",          "eventTimestamp_ts",     "sms")
    voip    = anomalies("silver_voip_cdrs",         "sessionStartTime_ts",   "voip")
    ims     = anomalies("silver_ims_cdrs",          "sessionStartTime_ts",   "ims")
    mms     = anomalies("silver_mms_cdrs",          "eventTimestamp_ts",     "mms")
    roaming = anomalies("silver_roaming_cdrs",      "callEventStartTime_ts", "roaming")
    wifi    = anomalies("silver_wifi_calling_cdrs", "sessionStartTime_ts",   "wifi")

    users = spark.read.table(_s("silver_users")).select("user_id", "msisdn", "plan_name")

    joined = (
        users
        .join(voice,   "user_id", "left")
        .join(data,    "user_id", "left")
        .join(sms,     "user_id", "left")
        .join(voip,    "user_id", "left")
        .join(ims,     "user_id", "left")
        .join(mms,     "user_id", "left")
        .join(roaming, "user_id", "left")
        .join(wifi,    "user_id", "left")
    )

    anomaly_cols = [
        "voice_anomaly_count", "data_anomaly_count", "sms_anomaly_count",
        "voip_anomaly_count",  "ims_anomaly_count",  "mms_anomaly_count",
        "roaming_anomaly_count", "wifi_anomaly_count",
    ]

    total_expr = functools.reduce(
        lambda a, b: a + b,
        [F.coalesce(F.col(c), F.lit(0)) for c in anomaly_cols],
    )

    return joined.withColumn("total_anomaly_count", total_expr)
