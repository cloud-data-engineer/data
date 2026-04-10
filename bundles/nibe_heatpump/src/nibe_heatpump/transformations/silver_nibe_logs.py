"""
NIBE Heat Pump Log — Silver Layer
===================================
Converts raw string registers from Bronze into physical units, combines
Date+Time into a timestamp, and adds pre-computed diagnostic columns used
by every Gold materialized view and agent tool.

Divisor map (from .LOG file Row 1, applied per register):
  ÷ 1   (integers as-is):
    sw_version, r_version, alarm_number, relays_pca_base,
    compr_freq_calc, compr_state, compr_protection_mode,
    gp12_fan (fan speed %), gp10_circ_pump (boolean), lp_raw
  ÷ 10  (→ °C / Hz / bar):
    bt1, bt6, bt7, bt25, bt71, bt63, calc_supply, degree_minutes,
    bt1_average, compr_freq_act, bt3, bt12, bt14, bt15, bt16,
    bt17, bt28, bp4, bp4_adc, saturation_temp, sat_temp_sent
  ÷ 100 (→ kW, instantaneous stage power):
    tot_int_add (raw 0/300/600/900 → 0/3/6/9 kW)

Special cases:
  - saturation_temp / saturation_temp_sent: raw 99 → 9.9 °C is firmware
    "sensor not available". Never use for subcooling — use bt12_condenser_c − bt15_liquid_c.
  - defrost_active requires BOTH bt16 > 10 °C AND gp12_fan > 30 %. bt16 alone fires false
    positives for hours on warm days (outdoor > 10 °C) when the unit is in standby.
  - bt14 (discharge): firmware clamps at 440 raw (44.0 °C) in some builds.
  - gp10_on_off: file divisor=10 but the register is boolean (0=off, ≥1=on).
    Cast to boolean directly, ignoring the stated divisor.
  - lp_raw (low-pressure sensor, reg 44700): .LOG file Divisors row shows ÷1,
    not ÷10. Stored as raw integer; not converted to bar.

Hardware notes (air-source, NOT ground-source):
  - GP12 = outdoor unit FAN speed (÷1 → %). NOT a brine/circulation pump.
  - GP10 = indoor floor heating circulation pump (boolean).
  - BT28 = outdoor temperature sensor on compressor module (second outdoor sensor).
  - BT16 = air evaporator temperature (NOT ground heat exchanger).
  - BT71 = return manifold sensor. (bt25_supply_c − bt71_return_c) = floor ΔT.
  - Defrost = hot-gas reversal; BT16 spikes to 16–27 °C during defrost.
  - Priority: 10 = standby/defrost, 20 = DHW, 30 = space heating.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ── Layer catalog shortcuts ───────────────────────────────────────────────────
_env = spark.conf.get("env_scope")   # "dev" or "prod"
_BL  = f"{_env}_bronze.nibe"         # bronze layer catalog.schema
_SL  = f"{_env}_silver.nibe"         # silver layer catalog.schema


@dp.materialized_view(
    name=f"{_SL}.silver_nibe_logs",
    comment=(
        "Parsed NIBE heat-pump time series with physical units and pre-computed diagnostics. "
        "Temperatures in °C, high-side pressure in bar, compressor frequency in Hz, "
        "backup heater as instantaneous stage power in kW (0/3/6/9). "
        "Computed columns: delta_t_k (flow quality), superheat_k (refrigerant health), "
        "negative_superheat, defrost_active, bt1_bt28_divergence_k, mode_name, heater_stage."
    ),
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "quality": "silver",
    },
    cluster_by=["logged_at"],
)
@dp.expect_or_drop("valid_timestamp",    "logged_at IS NOT NULL")
@dp.expect_or_drop("valid_outdoor_temp", "bt1_outdoor_temp_c BETWEEN -40 AND 50")
@dp.expect_or_drop("valid_supply_temp",  "bt25_supply_c BETWEEN 10 AND 65")
@dp.expect_or_drop("valid_freq",         "compr_freq_act_hz BETWEEN 0 AND 130")
def silver_nibe_logs():
    df = spark.read.table(f"{_BL}.bronze_nibe_logs")

    return (
        df

        # ── Timestamp ────────────────────────────────────────────────────────
        # NIBE logs local time; no timezone adjustment applied here.
        .withColumn(
            "logged_at",
            F.to_timestamp(F.concat_ws(" ", F.col("log_date"), F.col("log_time")))
        )
        .withColumn("log_date_parsed", F.to_date("log_date"))

        # ── Firmware / hardware version ──────────────────────────────────────
        .withColumn("sw_version", F.col("version_raw").cast("int"))    # reg 43001 — firmware build
        .withColumn("r_version",  F.col("r_version_raw").cast("int"))  # reg 44331 — HW revision

        # ── Outdoor temperatures ─────────────────────────────────────────────
        # BT1: primary outdoor air temperature; input to the heating curve
        .withColumn("bt1_outdoor_temp_c",  F.col("bt1_raw").cast("int") / 10.0)
        # BT1 rolling average: smoothed outdoor temp used by the control algorithm
        .withColumn("bt1_average_c",       F.col("bt1_average_raw").cast("int") / 10.0)
        # BT28: outdoor temperature sensor on compressor module (second outdoor sensor).
        # Used to detect wind/snow divergence vs BT1.
        .withColumn("bt28_outdoor_module_c", F.col("eb101_bt28_raw").cast("int") / 10.0)

        # ── Return manifold / room temperature ──────────────────────────────
        # BT71: return manifold sensor on the floor heating circuit.
        # delta_T = bt25_supply_c − bt71_return_c is the primary flow-quality metric.
        .withColumn("bt71_return_c",       F.col("bt71_raw").cast("int") / 10.0)

        # ── Domestic hot-water temperatures ─────────────────────────────────
        .withColumn("bt6_dhw_top_c",       F.col("bt6_raw").cast("int") / 10.0)   # tank top
        .withColumn("bt7_dhw_charge_c",    F.col("bt7_raw").cast("int") / 10.0)   # tank mid/charging

        # ── Heating circuit temperatures ─────────────────────────────────────
        # BT25: supply-line temperature to heating circuit (post heat pump)
        .withColumn("bt25_supply_c",       F.col("bt25_raw").cast("int") / 10.0)
        # BT63: heat-medium supply from HP body (used for compressor control)
        .withColumn("bt63_hw_supply_c",    F.col("bt63_raw").cast("int") / 10.0)
        # Calculated supply: heating-curve target for the supply line
        .withColumn("calc_supply_c",       F.col("calc_supply_raw").cast("int") / 10.0)

        # ── Refrigerant circuit temperatures ─────────────────────────────────
        # BT3: compressor return/condenser inlet temperature
        .withColumn("bt3_return_cond_c",   F.col("eb101_bt3_raw").cast("int") / 10.0)
        # BT12: condenser temperature
        .withColumn("bt12_condenser_c",    F.col("eb101_bt12_raw").cast("int") / 10.0)
        # BT14: discharge (hot-gas after compressor). Firmware may clamp at 44.0 °C.
        .withColumn("bt14_discharge_c",    F.col("eb101_bt14_raw").cast("int") / 10.0)
        # BT15: liquid-line temperature before expansion valve
        .withColumn("bt15_liquid_c",       F.col("eb101_bt15_raw").cast("int") / 10.0)
        # BT16: air evaporator temperature (NOT ground HX). Spikes during defrost.
        .withColumn("bt16_evap_c",         F.col("eb101_bt16_raw").cast("int") / 10.0)
        # BT17: suction-gas temperature (evaporator outlet). Used to calculate superheat.
        .withColumn("bt17_suction_c",      F.col("eb101_bt17_raw").cast("int") / 10.0)
        # saturation_temp / sat_temp_sent: always reads 9.9 °C (raw 99) — firmware "sensor N/A".
        # DO NOT use for subcooling. Use bt12_condenser_c − bt15_liquid_c instead.
        .withColumn("saturation_temp_c",       F.col("saturation_temp_raw").cast("int") / 10.0)
        .withColumn("saturation_temp_sent_c",  F.col("sat_temp_sent_raw").cast("int") / 10.0)

        # ── Pressure sensors ─────────────────────────────────────────────────
        # High-side pressure (÷10 → bar). Normal range: ~10–25 bar.
        .withColumn("bp4_hp_bar",      F.col("eb101_bp4_raw").cast("int") / 10.0)
        .withColumn("bp4_hp_raw_bar",  F.col("eb101_bp4_adc_raw").cast("int") / 10.0)
        # Low-pressure sensor: file Divisors row confirms ÷1 (not ÷10 as guide suggests).
        # Stored as raw integer; use for relative trend analysis only.
        .withColumn("lp_raw",          F.col("eb101_low_pressure_raw").cast("int"))

        # ── Compressor ───────────────────────────────────────────────────────
        .withColumn("compr_freq_calc_hz",    F.col("compr_freq_calc_raw").cast("int"))
        .withColumn("compr_freq_act_hz",     F.col("compr_freq_act_raw").cast("int") / 10.0)
        .withColumn("compr_state",           F.col("compr_state_raw").cast("int"))
        # compr_running: true when compressor is actively running (state == 2)
        .withColumn("compr_running",         F.col("compr_state_raw").cast("int") == 2)
        .withColumn("compr_protection_mode", F.col("compr_protection_mode_raw").cast("int"))

        # ── Pumps ─────────────────────────────────────────────────────────────
        # GP12: outdoor unit FAN speed (÷1 → %). Air-source unit — NOT brine pump.
        .withColumn("gp12_fan_pct",     F.col("gp12_speed_raw").cast("int"))
        # GP10: indoor circulation pump. File divisor=10 is ignored — register is boolean.
        .withColumn("gp10_circ_pump_on", F.col("gp10_on_off_raw").cast("int") > 0)

        # ── Control ───────────────────────────────────────────────────────────
        # Degree Minutes: NIBE integrating heat-demand controller.
        # Starts at 0; grows negative as building cools below setpoint.
        # Compressor starts when DM crosses threshold (typically −120 DM).
        .withColumn("degree_minutes",  F.col("degree_minutes_raw").cast("int") / 10.0)
        .withColumn("relays_pca_base", F.col("relays_pca_base_raw").cast("int"))
        # Priority mode: 10=standby/defrost, 20=DHW, 30=space heating
        .withColumn("priority_mode",   F.col("prio_raw").cast("int"))

        # ── Backup heater ─────────────────────────────────────────────────────
        # Instantaneous stage power: raw 0/300/600/900 ÷ 100 = 0/3/6/9 kW.
        # Multiply by (5/3600) to get kWh per 5-second sample.
        .withColumn("tot_int_add_kw",  F.col("tot_int_add_raw").cast("int") / 100.0)

        # ── Alarms ────────────────────────────────────────────────────────────
        .withColumn("alarm_number", F.col("alarm_number_raw").cast("int"))
        .withColumn("alarm_active", F.col("alarm_number_raw").cast("int") > 0)

        # ── COMPUTED DIAGNOSTIC COLUMNS ──────────────────────────────────────

        # Flow quality: ΔT (supply − return manifold).
        # Target: 3–5 K. >5 K = flow issue. >7 K = critical (compressor stress).
        .withColumn(
            "delta_t_k",
            (F.col("bt25_raw").cast("int") - F.col("bt71_raw").cast("int")) / 10.0
        )

        # Suction superheat: BT17 (suction-gas) − BT16 (air evaporator).
        # Target: 3–7 K. <0 K = liquid slugging risk (dangerous for compressor).
        .withColumn(
            "superheat_k",
            (F.col("eb101_bt17_raw").cast("int") - F.col("eb101_bt16_raw").cast("int")) / 10.0
        )

        # Negative superheat flag: compressor running AND superheat < 0.
        # True = liquid refrigerant risk at this 5-second sample.
        .withColumn(
            "negative_superheat",
            (F.col("compr_freq_act_raw").cast("int") / 10.0 > 0) &
            ((F.col("eb101_bt17_raw").cast("int") - F.col("eb101_bt16_raw").cast("int")) < 0)
        )

        # Defrost detection: BT16 (air evaporator) > 10 °C AND outdoor fan running (GP12 > 30%).
        # BT16 alone is insufficient — in warm weather (outdoor > 10°C) BT16 drifts above 10°C
        # naturally during standby (fan stopped), producing false positives lasting hours.
        # Real defrost: hot-gas reversal spikes BT16 to 16–27 °C while fan keeps running to aid melt.
        .withColumn(
            "defrost_active",
            (F.col("eb101_bt16_raw").cast("int") / 10.0 > 10.0) &
            (F.col("gp12_speed_raw").cast("int") > 30)
        )

        # BT1 vs BT28 temperature divergence: |outdoor_air − compressor_module|.
        # Normal: <1 K. >3 K = possible snow/wind obstruction. >5 K = confirmed issue.
        .withColumn(
            "bt1_bt28_divergence_k",
            F.abs(
                (F.col("bt1_raw").cast("int") - F.col("eb101_bt28_raw").cast("int")) / 10.0
            )
        )

        # Human-readable priority mode name
        .withColumn(
            "mode_name",
            F.when(F.col("prio_raw").cast("int") == 30, "HEATING")
             .when(F.col("prio_raw").cast("int") == 20, "DHW")
             .when(F.col("prio_raw").cast("int") == 10, "STANDBY")
             .otherwise("UNKNOWN")
        )

        # Human-readable backup heater stage
        .withColumn(
            "heater_stage",
            F.when(F.col("tot_int_add_raw").cast("int") == 0,   "OFF")
             .when(F.col("tot_int_add_raw").cast("int") == 300, "3kW")
             .when(F.col("tot_int_add_raw").cast("int") == 600, "6kW")
             .when(F.col("tot_int_add_raw").cast("int") == 900, "9kW")
             .otherwise("UNKNOWN")
        )

        # Supply temperature deviation: actual (BT63) − calculated setpoint.
        # Positive → overshooting; Negative → undershooting (demand not met).
        .withColumn(
            "supply_deviation_k",
            (F.col("bt63_raw").cast("int") - F.col("calc_supply_raw").cast("int")) / 10.0
        )

        # Discharge superheat: BT14 (discharge) − BT25 (supply). Compressor work indicator.
        .withColumn(
            "discharge_superheat_k",
            (F.col("eb101_bt14_raw").cast("int") - F.col("bt25_raw").cast("int")) / 10.0
        )

        # ── Source metadata (passed through from Bronze) ──────────────────────
        .withColumn("source_file",     F.col("_source_file"))
        .withColumn("source_filename", F.col("_source_filename"))
        .withColumn("ingested_at",     F.col("_ingested_at"))

        # ── Final column selection ────────────────────────────────────────────
        .select(
            # Time
            "logged_at", "log_date_parsed",
            # Firmware
            "sw_version", "r_version",
            # Outdoor temperatures
            "bt1_outdoor_temp_c", "bt1_average_c", "bt28_outdoor_module_c",
            # Return manifold
            "bt71_return_c",
            # DHW
            "bt6_dhw_top_c", "bt7_dhw_charge_c",
            # Heating circuit
            "bt25_supply_c", "bt63_hw_supply_c", "calc_supply_c", "supply_deviation_k",
            # Refrigerant circuit
            "bt3_return_cond_c", "bt12_condenser_c", "bt14_discharge_c",
            "bt15_liquid_c", "bt16_evap_c", "bt17_suction_c",
            "saturation_temp_c", "saturation_temp_sent_c",
            # Pressure
            "bp4_hp_bar", "bp4_hp_raw_bar", "lp_raw",
            # Compressor
            "compr_freq_calc_hz", "compr_freq_act_hz",
            "compr_state", "compr_running", "compr_protection_mode",
            # Pumps / fans
            "gp12_fan_pct", "gp10_circ_pump_on",
            # Control
            "degree_minutes", "relays_pca_base", "priority_mode",
            # Backup heater
            "tot_int_add_kw",
            # Alarms
            "alarm_number", "alarm_active",
            # Computed diagnostics
            "delta_t_k", "superheat_k", "negative_superheat", "defrost_active",
            "bt1_bt28_divergence_k", "mode_name", "heater_stage",
            "discharge_superheat_k",
            # Metadata
            "source_file", "source_filename", "ingested_at",
        )
    )
