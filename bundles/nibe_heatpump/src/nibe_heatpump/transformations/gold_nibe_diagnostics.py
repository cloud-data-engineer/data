"""
NIBE Heat Pump Log — Gold Layer (AI Diagnostics)
==================================================
Three tables optimised for agentic AI diagnosis and efficiency analysis:

  gold_nibe_hourly_stats    (materialized view)
    Hourly aggregations of all key metrics — the primary input for
    trend analysis, anomaly detection, and AI reporting.

  gold_nibe_alarm_events    (materialized view)
    Every 5-second snapshot where an alarm was active.
    Use for alarm frequency, duration analysis, and AI root-cause diagnosis.

  gold_nibe_current_state   (materialized view)
    Latest sensor snapshot per log date — gives an AI agent immediate
    context about the current operational state of the heat pump.

Design notes for AI / agentic use:
  - gold_nibe_hourly_stats contains all features an LLM agent needs to
    answer questions like "Was the compressor running efficiently last night?"
    or "Is the refrigerant charge correct?".
  - superheat_k and degree_minutes trends are the two most important signals
    for early fault detection.
  - compr_running_fraction (0.0–1.0) combined with avg_outdoor_temp_c gives
    a proxy for COP: lower outdoor temp + high run fraction = harder working unit.
  - gp12_fan_pct is the outdoor unit FAN speed (air-source unit, NOT brine pump).
  - gp10_circ_pump_on is the indoor floor heating circulation pump.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Layer catalog shortcuts ───────────────────────────────────────────────────
_env = spark.conf.get("env_scope")   # "dev" or "prod"
_SL  = f"{_env}_silver.nibe"         # silver layer catalog.schema
_GL  = f"{_env}_gold.nibe"           # gold layer catalog.schema


# ── Hourly Aggregations ───────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_hourly_stats",
    comment=(
        "Hourly statistics aggregated from silver_nibe_logs. "
        "Primary input for AI-based efficiency analysis, anomaly detection, "
        "trend dashboards, and LLM agent context. "
        "One row per hour per sw_version observed in that hour."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["hour_start"],
)
def gold_nibe_hourly_stats():
    return (
        spark.read.table(f"{_SL}.silver_nibe_logs")
        .withColumn("hour_start", F.date_trunc("hour", F.col("logged_at")))
        .groupBy("hour_start", "sw_version")
        .agg(
            F.count("*").alias("sample_count"),  # expected ~720/hour at 5-second intervals

            # ── Outdoor temperature ───────────────────────────────────────────
            F.avg("bt1_outdoor_temp_c").alias("avg_outdoor_temp_c"),
            F.min("bt1_outdoor_temp_c").alias("min_outdoor_temp_c"),
            F.max("bt1_outdoor_temp_c").alias("max_outdoor_temp_c"),

            # ── Return manifold temperature ───────────────────────────────────
            # BT71 is installed on the return manifold of the floor heating circuit.
            F.avg("bt71_return_c").alias("avg_return_c"),
            F.min("bt71_return_c").alias("min_return_c"),
            F.max("bt71_return_c").alias("max_return_c"),

            # ── Domestic hot water ────────────────────────────────────────────
            F.avg("bt6_dhw_top_c").alias("avg_hw_top_c"),
            F.min("bt6_dhw_top_c").alias("min_hw_top_c"),    # lowest = just before DHW cycle
            F.max("bt7_dhw_charge_c").alias("max_hw_charge_c"),

            # ── Supply temperature vs setpoint ────────────────────────────────
            F.avg("bt25_supply_c").alias("avg_supply_c"),
            F.avg("calc_supply_c").alias("avg_calc_supply_c"),
            # Positive deviation = supply exceeds setpoint; negative = shortfall
            F.avg("supply_deviation_k").alias("avg_supply_deviation_k"),
            F.stddev("supply_deviation_k").alias("stddev_supply_deviation_k"),

            # ── Compressor operation ──────────────────────────────────────────
            F.avg(F.col("compr_running").cast("int")).alias("compr_running_fraction"),
            F.avg("compr_freq_act_hz").alias("avg_compr_freq_hz"),
            F.max("compr_freq_act_hz").alias("max_compr_freq_hz"),
            F.min(
                F.when(F.col("compr_running"), F.col("compr_freq_act_hz"))
            ).alias("min_running_compr_freq_hz"),
            F.sum(F.col("compr_protection_mode").cast("int")).alias("protection_mode_samples"),

            # ── Degree Minutes ────────────────────────────────────────────────
            F.avg("degree_minutes").alias("avg_degree_minutes"),
            F.min("degree_minutes").alias("min_degree_minutes"),

            # ── Refrigerant circuit ───────────────────────────────────────────
            # BT3: compressor return/condenser inlet (renamed from bt3_discharge_c)
            F.avg("bt3_return_cond_c").alias("avg_return_cond_c"),
            F.max("bt3_return_cond_c").alias("max_return_cond_c"),
            # BT16: air evaporator temperature (renamed from bt16_evaporator_c)
            F.avg("bt16_evap_c").alias("avg_evap_c"),

            # Suction superheat: pre-computed BT17 − BT16.
            # avg < 3 K → flooding risk; avg > 15 K → undercharge / expansion fault.
            F.avg("superheat_k").alias("avg_superheat_k"),
            F.min("superheat_k").alias("min_superheat_k"),
            F.max("superheat_k").alias("max_superheat_k"),
            F.stddev("superheat_k").alias("stddev_superheat_k"),

            F.avg("discharge_superheat_k").alias("avg_discharge_superheat_k"),

            # ── Pressure ──────────────────────────────────────────────────────
            F.avg("bp4_hp_bar").alias("avg_hp_bar"),
            F.max("bp4_hp_bar").alias("max_hp_bar"),
            F.avg("lp_raw").alias("avg_lp_raw"),

            # ── Fan / circulation pump ────────────────────────────────────────
            # gp12_fan_pct: outdoor unit FAN speed (air-source, NOT brine pump)
            F.avg("gp12_fan_pct").alias("avg_gp12_fan_pct"),
            # gp10_circ_pump_on: indoor floor heating circulation pump
            F.avg(F.col("gp10_circ_pump_on").cast("int")).alias("gp10_circ_pump_fraction"),

            # ── Backup heater ─────────────────────────────────────────────────
            # tot_int_add_kw = instantaneous stage power (0/3/6/9 kW)
            F.max("tot_int_add_kw").alias("max_tot_int_add_kw"),

            # ── Alarms ────────────────────────────────────────────────────────
            F.max(F.col("alarm_active").cast("int")).alias("had_alarm"),
            F.collect_set("alarm_number").alias("alarm_codes"),
        )
    )


# ── Alarm Events ──────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_alarm_events",
    comment=(
        "Every 5-second snapshot where an alarm was active (alarm_number > 0). "
        "Provides full operational context at alarm time: "
        "temperatures, pressures, compressor state, superheat — exactly what an "
        "AI agent needs to root-cause a fault. "
        "NIBE alarm codes reference: see NIBE installer manual appendix."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["alarm_start"],
)
def gold_nibe_alarm_events():
    return (
        spark.read.table(f"{_SL}.silver_nibe_logs")
        .filter(F.col("alarm_active"))
        .select(
            F.col("logged_at").alias("alarm_start"),
            "alarm_number",
            # Outdoor conditions at alarm time
            "bt1_outdoor_temp_c",
            # Supply temperatures — deviation from setpoint helps classify the fault
            "bt25_supply_c",
            "calc_supply_c",
            "supply_deviation_k",
            # Compressor state at alarm time
            "compr_state",
            "compr_freq_act_hz",
            "compr_protection_mode",
            # Refrigerant circuit — most alarms are pressure or temperature related
            "bt3_return_cond_c",
            "bt16_evap_c",
            "bt17_suction_c",
            "superheat_k",
            "bp4_hp_bar",
            "lp_raw",
            # DHW state
            "bt6_dhw_top_c",
            "bt7_dhw_charge_c",
            # Control state
            "degree_minutes",
            "priority_mode",
            # Source traceability
            "source_filename",
            "ingested_at",
        )
    )


# ── Current State (latest reading per log date) ───────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_current_state",
    comment=(
        "Most-recent sensor snapshot for each log date. "
        "Gives an AI agent immediate operational context without needing to "
        "query the full time series. Contains all fields relevant for a "
        "first-pass diagnostic: temperatures, compressor status, refrigerant "
        "health indicators, alarm state, and degree-minutes trend."
    ),
    table_properties={"quality": "gold"},
)
def gold_nibe_current_state():
    # Window to pick the latest row per day
    w = Window.partitionBy("log_date_parsed").orderBy(F.col("logged_at").desc())

    return (
        spark.read.table(f"{_SL}.silver_nibe_logs")
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "logged_at",
            "log_date_parsed",
            "sw_version",
            # Outdoor & return manifold
            "bt1_outdoor_temp_c",
            "bt71_return_c",
            # DHW
            "bt6_dhw_top_c",
            # Supply
            "bt25_supply_c",
            "calc_supply_c",
            "supply_deviation_k",
            # Compressor
            "compr_state",
            "compr_running",
            "compr_freq_act_hz",
            "compr_protection_mode",
            # Refrigerant health (key diagnostic signals)
            "bt3_return_cond_c",
            "bt16_evap_c",
            "bt17_suction_c",
            "superheat_k",
            "bp4_hp_bar",
            "lp_raw",
            # Control
            "degree_minutes",
            "priority_mode",
            # Backup heater
            "tot_int_add_kw",
            # Alarm
            "alarm_number",
            "alarm_active",
            # Source
            "source_filename",
        )
    )
