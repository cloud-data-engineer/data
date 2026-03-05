# NIBE Heat Pump Diagnostic Pipeline — Week 1 Implementation

## Context for LLM

You are helping build a diagnostic data pipeline for a NIBE **air-source** heat pump (NOT ground-source — no brine circuit). The system uses underfloor heating with a manifold. Location: Poland.

**Tech stack:** Databricks, Unity Catalog, Spark Declarative Pipelines (DLT), Auto Loader for ingestion. Schema: `dev_raw.nibe`. All code should use **Databricks SQL or PySpark declarative style** — no classic DLT `@dlt.table` decorators, use `CREATE OR REFRESH STREAMING TABLE` / `CREATE OR REFRESH MATERIALIZED VIEW` syntax.

**Important hardware facts (verified from real log analysis):**
- GP12 = outdoor unit FAN (not brine pump — this is air-source)
- Backup heater is 3-stage: 3 kW / 6 kW / 9 kW. Raw log values are 0, 300, 600, 900. Divisor is 100, so 900/100 = 9.0 kW (NOT 900W!)
- BT16 = air evaporator temperature (not ground heat exchanger)
- BT28 = outdoor temperature sensor on compressor module (second outdoor sensor)
- Data is sampled every 5 seconds, ~17,000 rows per day
- Priority modes: 10 = standby/defrost, 20 = DHW, 30 = heating

---

## Week 1 Goal

Build the foundation: **Bronze → Silver pipeline** with all columns correctly parsed, and **gold_nibe_daily_summary** materialized view with the key diagnostic metrics we've proven matter.

---

## Task 1: Bronze — Auto Loader Ingestion

Raw `.LOG` files are tab-separated with 2 header rows:
- Row 1: divisors (tab-separated integers, used to scale raw values)
- Row 2: column names (tab-separated)
- Row 3+: data

The files land in a cloud storage path (configure as needed). Use Auto Loader to ingest them as raw text first, then parse in silver.

### File format example:
```
Divisors	1	1	10	10	10	10	10	10	100	1	10	10	10	1	1	10	1	1	1	1	10	10	10	10	10	10	10	10	10	10	10	1
Date	Time	version(43001)	R-version(44331)	BT1(40004)	BT6(40014)	BT7(40013)	BT25(40071)	BT71(40152)	BT63(40121)	Tot.Int.Add(43084)	Alarm number(45001)	Calc. Supply(43009)	Degree Minutes(40940)	BT1-Average(40067)	Relays PCA-Base(43514)	compr. freq. calc(44775)	compr. freq. act(44701)	compr. state(44457)	compr. protection mode(44702)	GP12-speed(44396)	GP10-on/off(43189)	EB101-BT3(44055)	EB101-BT12(44058)	EB101-BT14(44059)	EB101-BT15(44060)	EB101-BT16(44363)	EB101-BT17(44061)	EB101-BT28(44362)	EB101-BP4(44699)	EB101-BP4-raw(44698)	Saturation temp.(34000)	Sat. temp. sent(34001)	EB101-low pressure sensor(44700)	Prio(43086)
2011-04-12	18:37:01	8932	222	-165	459	273	294	247	284	0	0	307	-7	-161	0	0	297	20	0	0	0	246	264	280	133	-199	-188	-166	177	177	-179	-172	44	30
```

### Create bronze table:

```sql
-- Notebook: 01_bronze_nibe_logs
-- Bronze: raw ingestion via Auto Loader
-- Reads .LOG files as text, stores raw lines for silver parsing

CREATE OR REFRESH STREAMING TABLE bronze_nibe_logs
COMMENT 'Raw NIBE log lines ingested via Auto Loader'
AS SELECT
  *,
  _metadata.file_name AS source_file,
  _metadata.file_modification_time AS file_modified_at
FROM STREAM read_files(
  '${source_path}',                    -- Set this pipeline parameter
  format => 'text',
  header => 'false',
  wholeText => 'false'
);
```

---

## Task 2: Silver — Parsed and Typed

Apply divisors, cast types, add computed columns. This is where all the domain knowledge lives.

```sql
-- Notebook: 02_silver_nibe_logs
-- Silver: parsed, typed, with computed diagnostic columns

CREATE OR REFRESH MATERIALIZED VIEW silver_nibe_logs
COMMENT 'Parsed NIBE logs with correct units and computed diagnostics'
AS SELECT
  -- Timestamps
  CAST(`Date` AS DATE) AS log_date_parsed,
  TO_TIMESTAMP(CONCAT(`Date`, ' ', `Time`), 'yyyy-MM-dd HH:mm:ss') AS logged_at,

  -- Outdoor temperatures
  CAST(`BT1(40004)` AS DOUBLE) / 10 AS bt1_outdoor_temp_c,
  CAST(`BT1-Average(40067)` AS DOUBLE) / 10 AS bt1_average_c,
  CAST(`EB101-BT28(44362)` AS DOUBLE) / 10 AS bt28_outdoor_module_c,

  -- Heating circuit
  CAST(`BT25(40071)` AS DOUBLE) / 10 AS bt25_supply_c,
  CAST(`BT71(40152)` AS DOUBLE) / 10 AS bt71_return_c,
  CAST(`BT63(40121)` AS DOUBLE) / 10 AS bt63_hw_supply_c,
  CAST(`Calc. Supply(43009)` AS DOUBLE) / 10 AS calc_supply_c,

  -- DHW
  CAST(`BT6(40014)` AS DOUBLE) / 10 AS bt6_dhw_top_c,
  CAST(`BT7(40013)` AS DOUBLE) / 10 AS bt7_dhw_charge_c,

  -- Compressor
  CAST(`compr. freq. act(44701)` AS DOUBLE) / 10 AS compr_freq_act_hz,
  CAST(`compr. freq. calc(44775)` AS INT) AS compr_freq_calc_hz,
  CAST(`compr. state(44457)` AS INT) AS compr_state,
  CAST(`compr. protection mode(44702)` AS INT) AS compr_protection_mode,

  -- Refrigerant circuit (critical for diagnostics)
  CAST(`EB101-BT3(44055)` AS DOUBLE) / 10 AS bt3_return_cond_c,
  CAST(`EB101-BT12(44058)` AS DOUBLE) / 10 AS bt12_condenser_c,
  CAST(`EB101-BT14(44059)` AS DOUBLE) / 10 AS bt14_discharge_c,
  CAST(`EB101-BT15(44060)` AS DOUBLE) / 10 AS bt15_liquid_c,
  CAST(`EB101-BT16(44363)` AS DOUBLE) / 10 AS bt16_evap_c,
  CAST(`EB101-BT17(44061)` AS DOUBLE) / 10 AS bt17_suction_c,

  -- Pressures
  CAST(`EB101-BP4(44699)` AS DOUBLE) / 10 AS bp4_hp_bar,
  CAST(`EB101-BP4-raw(44698)` AS DOUBLE) / 10 AS bp4_hp_raw_bar,
  CAST(`EB101-low pressure sensor(44700)` AS DOUBLE) / 10 AS lp_bar,
  CAST(`Saturation temp.(34000)` AS DOUBLE) / 10 AS saturation_temp_c,
  CAST(`Sat. temp. sent(34001)` AS DOUBLE) / 10 AS saturation_temp_sent_c,

  -- Fan (air-source: GP12 = outdoor unit fan, NOT brine pump)
  CAST(`GP12-speed(44396)` AS INT) AS gp12_fan_pct,
  CAST(`GP10-on/off(43189)` AS INT) AS gp10_circ_pump_on,

  -- Backup heater: raw values 0/300/600/900, divisor 100 → 0/3/6/9 kW
  CAST(`Tot.Int.Add(43084)` AS DOUBLE) / 100 AS tot_int_add_kw,

  -- Control
  CAST(`Degree Minutes(40940)` AS DOUBLE) / 10 AS degree_minutes,
  CAST(`Prio(43086)` AS INT) AS priority_mode,
  CAST(`Relays PCA-Base(43514)` AS INT) AS relays_pca_base,

  -- Alarms
  CAST(`Alarm number(45001)` AS INT) AS alarm_number,
  CAST(`Alarm number(45001)` AS INT) != 0 AS alarm_active,

  -- System info
  CAST(`version(43001)` AS INT) AS sw_version,
  CAST(`R-version(44331)` AS INT) AS r_version,

  -- ==========================================
  -- COMPUTED DIAGNOSTIC COLUMNS
  -- ==========================================

  -- Flow quality: delta T (supply - return)
  (CAST(`BT25(40071)` AS DOUBLE) / 10) - (CAST(`BT71(40152)` AS DOUBLE) / 10)
    AS delta_t_k,

  -- Superheat: BT17 (suction) - BT16 (evaporator)
  (CAST(`EB101-BT17(44061)` AS DOUBLE) / 10) - (CAST(`EB101-BT16(44363)` AS DOUBLE) / 10)
    AS superheat_k,

  -- Negative superheat flag (liquid slugging risk — compressor must be running)
  CASE
    WHEN CAST(`compr. freq. act(44701)` AS DOUBLE) / 10 > 0
     AND ((CAST(`EB101-BT17(44061)` AS DOUBLE) / 10) - (CAST(`EB101-BT16(44363)` AS DOUBLE) / 10)) < 0
    THEN TRUE ELSE FALSE
  END AS negative_superheat,

  -- Defrost detection: BT16 spikes above 10°C during compressor operation
  CASE
    WHEN CAST(`EB101-BT16(44363)` AS DOUBLE) / 10 > 10 THEN TRUE ELSE FALSE
  END AS defrost_active,

  -- BT1 vs BT28 divergence: indicates wind/snow on outdoor unit
  ABS((CAST(`BT1(40004)` AS DOUBLE) / 10) - (CAST(`EB101-BT28(44362)` AS DOUBLE) / 10))
    AS bt1_bt28_divergence_k,

  -- Priority mode readable name
  CASE CAST(`Prio(43086)` AS INT)
    WHEN 30 THEN 'HEATING'
    WHEN 20 THEN 'DHW'
    WHEN 10 THEN 'STANDBY'
    ELSE 'UNKNOWN'
  END AS mode_name,

  -- Heater stage readable
  CASE CAST(`Tot.Int.Add(43084)` AS INT)
    WHEN 0 THEN 'OFF'
    WHEN 300 THEN '3kW'
    WHEN 600 THEN '6kW'
    WHEN 900 THEN '9kW'
    ELSE 'UNKNOWN'
  END AS heater_stage,

  -- Source tracking
  source_file

FROM LIVE.bronze_nibe_logs
WHERE `Date` IS NOT NULL
  AND `Date` != 'Date';  -- Filter out re-ingested header rows
```

### Data quality expectations on silver:

```sql
-- Add to pipeline as expectations
CONSTRAINT valid_timestamp EXPECT (logged_at IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_outdoor_temp EXPECT (bt1_outdoor_temp_c BETWEEN -40 AND 50) ON VIOLATION DROP ROW
CONSTRAINT valid_supply_temp EXPECT (bt25_supply_c BETWEEN 10 AND 65) ON VIOLATION DROP ROW
CONSTRAINT valid_freq EXPECT (compr_freq_act_hz BETWEEN 0 AND 130 OR compr_freq_act_hz IS NULL) ON VIOLATION DROP ROW
```

---

## Task 3: Gold — Daily Summary

The most important aggregate. One row per day with all diagnostic metrics.

```sql
-- Notebook: 03_gold_daily_summary
-- Gold: daily diagnostic summary — the agent's primary data source

CREATE OR REFRESH MATERIALIZED VIEW gold_nibe_daily_summary
COMMENT 'Daily heat pump performance summary with diagnostic metrics'
AS SELECT
  log_date_parsed AS log_date,

  -- OUTDOOR CONDITIONS
  ROUND(MIN(bt1_outdoor_temp_c), 1) AS outdoor_min_c,
  ROUND(MAX(bt1_outdoor_temp_c), 1) AS outdoor_max_c,
  ROUND(AVG(bt1_outdoor_temp_c), 1) AS outdoor_avg_c,

  -- HEATING CIRCUIT (compressor running + heating mode only)
  ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 AND priority_mode = 30
            THEN bt25_supply_c END), 1) AS supply_avg_c,
  ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 AND priority_mode = 30
            THEN bt71_return_c END), 1) AS return_avg_c,
  ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 AND priority_mode = 30
            THEN calc_supply_c END), 1) AS calc_supply_avg_c,

  -- FLOW QUALITY: delta T — PRIMARY diagnostic metric
  -- Target: 3-5K. Above 5K = flow issue. Above 7K = critical.
  ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 AND priority_mode = 30
            THEN delta_t_k END), 1) AS delta_t_avg_k,
  ROUND(MAX(CASE WHEN compr_freq_act_hz > 0
            THEN delta_t_k END), 1) AS delta_t_max_k,

  -- CURVE TRACKING: does actual supply reach target?
  ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 AND priority_mode = 30
            THEN bt25_supply_c - calc_supply_c END), 1) AS supply_vs_calc_offset_k,

  -- DEGREE MINUTES — demand satisfaction
  -- Above -200 = OK. Below -400 = struggling. -540 = saturated.
  ROUND(AVG(degree_minutes), 0) AS dm_avg,
  ROUND(MIN(degree_minutes), 0) AS dm_min,
  ROUND(MAX(degree_minutes), 0) AS dm_max,

  -- COMPRESSOR
  ROUND(SUM(CASE WHEN compr_freq_act_hz > 0 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 1) AS compr_duty_pct,
  ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
            THEN compr_freq_act_hz END), 0) AS compr_freq_avg_hz,
  ROUND(MAX(compr_freq_act_hz), 0) AS compr_freq_max_hz,
  ROUND(SUM(CASE WHEN compr_freq_act_hz > 0 THEN 1 ELSE 0 END) * 5.0
        / 3600, 1) AS compr_running_hours,

  -- BACKUP HEATER — cost indicator
  -- Any heater above -10°C outdoor = likely flow or curve problem
  ROUND(SUM(CASE WHEN tot_int_add_kw > 0 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 1) AS heater_duty_pct,
  ROUND(SUM(tot_int_add_kw * 5.0 / 3600), 1) AS heater_energy_kwh,
  MAX(tot_int_add_kw) AS heater_max_kw,

  -- SUPERHEAT — compressor health indicator
  -- Target: 3-7K. Negative = liquid slugging (dangerous).
  ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
            THEN superheat_k END), 1) AS superheat_avg_k,
  ROUND(MIN(CASE WHEN compr_freq_act_hz > 0
            THEN superheat_k END), 1) AS superheat_min_k,
  ROUND(SUM(CASE WHEN negative_superheat THEN 1 ELSE 0 END) * 100.0
        / NULLIF(SUM(CASE WHEN compr_freq_act_hz > 0 THEN 1 ELSE 0 END), 0),
        1) AS neg_superheat_pct,

  -- PRESSURES
  ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 THEN bp4_hp_bar END), 1) AS hp_avg_bar,
  ROUND(MAX(bp4_hp_bar), 1) AS hp_max_bar,
  ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 THEN lp_bar END), 1) AS lp_avg_bar,

  -- DISCHARGE TEMPERATURE
  ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 THEN bt14_discharge_c END), 1) AS discharge_avg_c,
  ROUND(MAX(bt14_discharge_c), 1) AS discharge_max_c,

  -- ALARMS
  SUM(CASE WHEN alarm_active THEN 1 ELSE 0 END) AS alarm_samples,
  ROUND(SUM(CASE WHEN alarm_active THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 1) AS alarm_pct,

  -- DEFROST (air-source specific)
  SUM(CASE WHEN defrost_active THEN 1 ELSE 0 END) AS defrost_samples,

  -- BT1 vs BT28 DIVERGENCE — wind/snow indicator
  -- Normal: <1K. Above 3K = possible snow/wind. Above 5K = confirmed issue.
  ROUND(AVG(bt1_bt28_divergence_k), 1) AS bt1_bt28_avg_divergence_k,
  ROUND(MAX(bt1_bt28_divergence_k), 1) AS bt1_bt28_max_divergence_k,

  -- MODE SPLIT
  ROUND(SUM(CASE WHEN priority_mode = 30 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 0) AS heating_pct,
  ROUND(SUM(CASE WHEN priority_mode = 20 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 0) AS dhw_pct,

  -- DHW
  ROUND(AVG(bt6_dhw_top_c), 1) AS dhw_avg_c,
  ROUND(MIN(bt6_dhw_top_c), 1) AS dhw_min_c,

  -- DATA QUALITY
  COUNT(*) AS total_samples,
  ROUND(COUNT(*) * 5.0 / 3600, 1) AS total_hours,
  SUM(CASE WHEN bt1_outdoor_temp_c IS NULL THEN 1 ELSE 0 END) AS null_bt1_count,
  SUM(CASE WHEN compr_freq_act_hz IS NULL THEN 1 ELSE 0 END) AS null_freq_count

FROM LIVE.silver_nibe_logs
GROUP BY log_date_parsed;
```

---

## Task 4: Gold — Flow Quality by Frequency Band

ΔT broken down by compressor load. This is the #1 diagnostic for flow problems. Proven: flow improvement from 0.5→1 l/min eliminated heater usage completely.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_nibe_flow_quality
COMMENT 'Delta T by compressor frequency band — primary flow diagnostic'
AS SELECT
  log_date_parsed AS log_date,
  CASE
    WHEN compr_freq_act_hz BETWEEN 1 AND 40 THEN '01_low_20-40Hz'
    WHEN compr_freq_act_hz BETWEEN 40 AND 70 THEN '02_mid_40-70Hz'
    WHEN compr_freq_act_hz BETWEEN 70 AND 100 THEN '03_high_70-100Hz'
    WHEN compr_freq_act_hz > 100 THEN '04_max_100+Hz'
  END AS freq_band,

  ROUND(AVG(delta_t_k), 1) AS delta_t_avg_k,
  ROUND(MAX(delta_t_k), 1) AS delta_t_max_k,
  ROUND(AVG(bp4_hp_bar), 1) AS hp_avg_bar,
  ROUND(AVG(bt14_discharge_c), 1) AS discharge_avg_c,
  ROUND(AVG(superheat_k), 1) AS superheat_avg_k,
  ROUND(SUM(CASE WHEN tot_int_add_kw > 0 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 0) AS heater_pct,
  COUNT(*) AS samples

FROM LIVE.silver_nibe_logs
WHERE compr_freq_act_hz > 0 AND priority_mode = 30
GROUP BY log_date_parsed,
  CASE
    WHEN compr_freq_act_hz BETWEEN 1 AND 40 THEN '01_low_20-40Hz'
    WHEN compr_freq_act_hz BETWEEN 40 AND 70 THEN '02_mid_40-70Hz'
    WHEN compr_freq_act_hz BETWEEN 70 AND 100 THEN '03_high_70-100Hz'
    WHEN compr_freq_act_hz > 100 THEN '04_max_100+Hz'
  END;
```

---

## Task 5: Gold — Heating Curve Performance

Performance at each outdoor temperature degree. Shows where the curve is too high/low, where heater kicks in.

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_nibe_heating_curve
COMMENT 'Heating curve analysis: performance per outdoor temperature degree'
AS SELECT
  log_date_parsed AS log_date,
  ROUND(bt1_outdoor_temp_c, 0) AS bt1_rounded_c,

  ROUND(AVG(calc_supply_c), 1) AS calc_supply_c,
  ROUND(AVG(bt25_supply_c), 1) AS actual_supply_c,
  ROUND(AVG(bt71_return_c), 1) AS return_c,
  ROUND(AVG(delta_t_k), 1) AS delta_t_k,
  ROUND(AVG(degree_minutes), 0) AS dm_avg,
  ROUND(AVG(compr_freq_act_hz), 0) AS freq_avg_hz,
  ROUND(SUM(CASE WHEN tot_int_add_kw > 0 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 0) AS heater_pct,
  ROUND(AVG(bp4_hp_bar), 1) AS hp_avg_bar,
  COUNT(*) AS samples

FROM LIVE.silver_nibe_logs
WHERE compr_freq_act_hz > 0 AND priority_mode = 30
GROUP BY log_date_parsed, ROUND(bt1_outdoor_temp_c, 0)
HAVING COUNT(*) > 20;
```

---

## Task 6: Pipeline Configuration

### Pipeline parameters:
```json
{
  "source_path": "/Volumes/dev_raw/nibe/raw_logs/",
  "target_catalog": "dev_raw",
  "target_schema": "nibe"
}
```

### Pipeline settings:
- **Mode:** Triggered (not continuous — logs arrive in batches)
- **Schedule:** Daily at 01:00 (after midnight data is complete)
- **Cluster:** Single node, smallest available (data is small)
- **Channel:** Current

### Expected pipeline DAG:
```
Auto Loader (raw .LOG files)
  └─► bronze_nibe_logs (streaming table)
        └─► silver_nibe_logs (materialized view — all 34 cols parsed + computed)
              ├─► gold_nibe_daily_summary (materialized view — 1 row/day)
              ├─► gold_nibe_flow_quality (materialized view — 4 rows/day)
              └─► gold_nibe_heating_curve (materialized view — ~30 rows/day)
```

---

## Task 7: Validation Queries

After pipeline runs, verify with these:

```sql
-- Check silver has all columns
SELECT * FROM dev_raw.nibe.silver_nibe_logs LIMIT 5;

-- Verify heater values are kW not watts
SELECT DISTINCT tot_int_add_kw FROM dev_raw.nibe.silver_nibe_logs ORDER BY 1;
-- Expected: 0.0, 3.0, 6.0, 9.0

-- Verify superheat computation
SELECT
  ROUND(AVG(superheat_k), 1) AS avg_superheat,
  ROUND(SUM(CASE WHEN negative_superheat THEN 1 ELSE 0 END) * 100.0
    / SUM(CASE WHEN compr_freq_act_hz > 0 THEN 1 ELSE 0 END), 1) AS neg_pct
FROM dev_raw.nibe.silver_nibe_logs
WHERE compr_freq_act_hz > 0;
-- Expected neg_pct: ~30% based on our analysis

-- Check daily summary looks reasonable
SELECT
  log_date, outdoor_avg_c, delta_t_avg_k, dm_avg,
  heater_duty_pct, heater_energy_kwh, neg_superheat_pct,
  alarm_pct, bt1_bt28_max_divergence_k
FROM dev_raw.nibe.gold_nibe_daily_summary
ORDER BY log_date;

-- Check flow quality: ΔT should rise with freq if flow is restricted
SELECT * FROM dev_raw.nibe.gold_nibe_flow_quality
ORDER BY log_date, freq_band;
```

---

## Week 1 Definition of Done

- [ ] Pipeline runs end-to-end: raw .LOG → bronze → silver → 3 gold views
- [ ] Silver has all 34 columns + 7 computed columns (delta_t, superheat, etc.)
- [ ] Heater values confirmed as 0/3/6/9 kW (not raw integers)
- [ ] gold_nibe_daily_summary produces one row per day with all metrics
- [ ] gold_nibe_flow_quality shows ΔT by freq band per day
- [ ] gold_nibe_heating_curve shows performance per outdoor temp degree
- [ ] Validation queries return expected results
- [ ] Pipeline scheduled daily at 01:00

---

## Known Issues / Decisions for Week 2

1. **Compressor starts counting** — requires window function (LAG) which may need streaming table instead of materialized view for silver. If needed, move cycle detection to a separate gold table.
2. **COP** — no energy counters in USB logs. Will add proxy metric later or integrate with myUplink/Modbus.
3. **Alarm episodes** — needs window functions for episode grouping. Planned for Week 2.
4. **Hourly detail table** — simple GROUP BY hour, planned for Week 2.
5. **Agent tools (UC functions)** — Week 2-3.
