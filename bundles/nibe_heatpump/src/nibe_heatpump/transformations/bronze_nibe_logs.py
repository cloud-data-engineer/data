"""
NIBE Heat Pump Log — Bronze Layer
==================================
Ingests raw NIBE .LOG files from a Unity Catalog Volume via Auto Loader.

System: NIBE air-source heat pump with underfloor heating.
Location: Poland. Data sampled every 5 seconds (~17,000 rows/day).

File format (tab-separated, UTF-8):
  Row 1 — "Divisors" label followed by per-column integer divisors.
           These divisors convert raw register integers to physical units
           and are applied in the Silver layer (not here).
  Row 2 — Column headers: human-readable name + NIBE register ID in ()
  Row 3+ — One record every ~5 seconds:
           Date (YYYY-MM-DD), Time (HH:mm:ss), then 33 integer registers.

Hardware identification for this specific unit:
  GP12 (reg 44396) = OUTDOOR UNIT FAN speed (%, 0-100)
                     NOT a brine/circulation pump — this is air-source!
  GP10 (reg 43189) = INDOOR CIRCULATION PUMP on/off
                     (0=off, 1=on; file divisor 10 is ignored — it is boolean)
  BT28 (reg 44362) = OUTDOOR TEMPERATURE SENSOR on compressor module
                     (second outdoor sensor; NOT refrigerant liquid line)
  BT16 (reg 44363) = AIR EVAPORATOR temperature (NOT ground heat exchanger)

Divisor map (Row 1 of each .LOG file):
  ÷ 1   : version, r_version, alarm_number, relays_pca_base,
           compr_freq_calc, compr_state, compr_protection_mode,
           gp12_speed, gp10_on_off, eb101_low_pressure, prio
  ÷ 10  : bt1, bt6, bt7, bt25, bt71, bt63, calc_supply, degree_minutes,
           bt1_average, compr_freq_act, bt3, bt12, bt14, bt15, bt16, bt17,
           bt28, bp4, bp4_adc, saturation_temp, sat_temp_sent
  ÷ 100 : tot_int_add (raw 0/300/600/900 → 0/3/6/9 kW backup heater power)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType

# ── Pipeline parameters (set in nibe_heatpump.pipeline.yml) ─────────────────
_env             = spark.conf.get("env_scope")         # "dev" or "prod"
_source_path     = spark.conf.get("source_volume_path")
_schema_location = spark.conf.get("schema_location")
_BL              = f"{_env}_bronze.nibe"               # target catalog.schema

# ── Schema ───────────────────────────────────────────────────────────────────
# All columns defined as STRING so we can:
#   1. filter the two non-data header rows (Divisors + column-name rows)
#   2. catch any unexpected/malformed values before casting in Silver
#
# Column order mirrors the .LOG file header row exactly.
# Register IDs in comments (e.g. reg 40004) are NIBE Modbus register numbers.
_BRONZE_SCHEMA = StructType([
    StructField("log_date",                   StringType(), True),
    StructField("log_time",                   StringType(), True),
    StructField("version_raw",                StringType(), True),  # reg 43001 — firmware build number          (÷1)
    StructField("r_version_raw",              StringType(), True),  # reg 44331 — hardware revision              (÷1)
    StructField("bt1_raw",                    StringType(), True),  # reg 40004 — outdoor air temperature        (÷10 → °C)
    StructField("bt6_raw",                    StringType(), True),  # reg 40014 — DHW tank top                   (÷10 → °C)
    StructField("bt7_raw",                    StringType(), True),  # reg 40013 — DHW tank charging              (÷10 → °C)
    StructField("bt25_raw",                   StringType(), True),  # reg 40071 — supply-line to heating circuit (÷10 → °C)
    StructField("bt71_raw",                   StringType(), True),  # reg 40152 — return manifold sensor         (÷10 → °C)
    StructField("bt63_raw",                   StringType(), True),  # reg 40121 — heat-medium supply (HW side)   (÷10 → °C)
    StructField("tot_int_add_raw",            StringType(), True),  # reg 43084 — backup heater stage: 0/300/600/900 (÷100 → 0/3/6/9 kW)
    StructField("alarm_number_raw",           StringType(), True),  # reg 45001 — active alarm code; 0 = none   (÷1)
    StructField("calc_supply_raw",            StringType(), True),  # reg 43009 — calculated supply setpoint     (÷10 → °C)
    StructField("degree_minutes_raw",         StringType(), True),  # reg 40940 — degree-minutes controller      (÷10 → DM)
    StructField("bt1_average_raw",            StringType(), True),  # reg 40067 — BT1 rolling average            (÷10 → °C)
    StructField("relays_pca_base_raw",        StringType(), True),  # reg 43514 — relay status bitmask           (÷1)
    StructField("compr_freq_calc_raw",        StringType(), True),  # reg 44775 — calculated compressor freq     (÷1  → Hz)
    StructField("compr_freq_act_raw",         StringType(), True),  # reg 44701 — actual inverter output freq    (÷10 → Hz)
    StructField("compr_state_raw",            StringType(), True),  # reg 44457 — state: 0=off 1=start 2=run 3=stop (÷1)
    StructField("compr_protection_mode_raw",  StringType(), True),  # reg 44702 — protection curtailment flag    (÷1)
    StructField("gp12_speed_raw",             StringType(), True),  # reg 44396 — OUTDOOR UNIT FAN speed (÷1 → %) — air-source, NOT brine pump
    StructField("gp10_on_off_raw",            StringType(), True),  # reg 43189 — INDOOR CIRCULATION PUMP on/off (file says ÷10, treat as boolean)
    StructField("eb101_bt3_raw",              StringType(), True),  # reg 44055 — compressor discharge/return-gas temp (÷10 → °C)
    StructField("eb101_bt12_raw",             StringType(), True),  # reg 44058 — condenser temperature          (÷10 → °C)
    StructField("eb101_bt14_raw",             StringType(), True),  # reg 44059 — hot-gas / discharge temp       (÷10 → °C)
    StructField("eb101_bt15_raw",             StringType(), True),  # reg 44060 — liquid-line temperature        (÷10 → °C)
    StructField("eb101_bt16_raw",             StringType(), True),  # reg 44363 — AIR EVAPORATOR temperature     (÷10 → °C)
    StructField("eb101_bt17_raw",             StringType(), True),  # reg 44061 — suction-gas temperature        (÷10 → °C)
    StructField("eb101_bt28_raw",             StringType(), True),  # reg 44362 — OUTDOOR TEMP on compressor module (÷10 → °C) — NOT liquid line
    StructField("eb101_bp4_raw",              StringType(), True),  # reg 44699 — high-side pressure sensor      (÷10 → bar)
    StructField("eb101_bp4_adc_raw",          StringType(), True),  # reg 44698 — HP sensor ADC raw              (÷10)
    StructField("saturation_temp_raw",        StringType(), True),  # reg 34000 — condensing saturation temp     (÷10 → °C); 99 = N/A
    StructField("sat_temp_sent_raw",          StringType(), True),  # reg 34001 — saturation temp sent to comp   (÷10 → °C); 99 = N/A
    StructField("eb101_low_pressure_raw",     StringType(), True),  # reg 44700 — low-pressure sensor raw        (÷1, file divisor confirmed)
    StructField("prio_raw",                   StringType(), True),  # reg 43086 — priority: 10=standby 20=DHW 30=heating (÷1)
])


@dp.table(
    name=f"{_BL}.bronze_nibe_logs",
    comment=(
        "Raw NIBE heat-pump sensor records ingested from .LOG files via Auto Loader. "
        "Each row is one ~5-second snapshot. All 33 register values stored as raw "
        "strings with no unit conversion — divisors applied in silver_nibe_logs. "
        "Header rows (Divisors and column-name rows) are filtered out here."
    ),
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "quality": "bronze",
    },
    cluster_by=["log_date"],
)
def bronze_nibe_logs():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("sep", "\t")
        .option("header", "false")
        .option("cloudFiles.schemaLocation", _schema_location)
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(_BRONZE_SCHEMA)
        .load(_source_path)
        # Filter out the two .LOG header rows:
        # Row 1 has "Divisors" in log_date; row 2 has "Date"
        .filter(~F.col("log_date").isin("Divisors", "Date"))
        .filter(F.col("log_date").rlike(r"^\d{4}-\d{2}-\d{2}$"))
        # Source file metadata
        .withColumn("_source_file",     F.col("_metadata.file_path"))
        .withColumn("_source_filename", F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+)$", 1))
        .withColumn("_ingested_at",     F.current_timestamp())
    )
