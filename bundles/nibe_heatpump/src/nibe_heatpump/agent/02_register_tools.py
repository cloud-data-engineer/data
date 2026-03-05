# Databricks notebook source
# MAGIC %md
# MAGIC # NIBE Diagnostic Agent — Register UC Tool Functions
# MAGIC
# MAGIC Registers 12 SQL functions for use as AI agent tools.
# MAGIC These functions are called by the LLM agent (via AI Playground or model serving)
# MAGIC to retrieve pre-aggregated diagnostic data from the gold tables.
# MAGIC
# MAGIC **Prerequisites:** The gold tables are managed by the `nibe_heatpump` SDP pipeline.
# MAGIC Run or trigger the pipeline before registering functions,
# MAGIC then run this notebook once. Re-run only when function definitions change.

# COMMAND ----------

# Environment-based catalog routing (dev/prod)
_env = spark.conf.get("env_scope", "dev")  # "dev" or "prod"
_GL = f"{_env}_gold.nibe"  # gold layer catalog.schema

print(f"Registering UC functions in: {_GL}")

# COMMAND ----------

# DBTITLE 1,1. get_daily_summary
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_daily_summary(
  lookback_days INT DEFAULT 7
)
RETURNS STRING
COMMENT 'Returns daily performance summary for the last N days. START HERE after get_system_health flags issues. Returns JSON with: outdoor temps, delta_T (flow quality, target 3-5K, >7K critical), degree_minutes (demand, target >-200, <-400 struggling), compressor duty/freq/starts/hours, heater usage (target 0 kWh), superheat (BT17-BT16, target 3-7K, negative=dangerous), pressures, discharge temp, alarms (alarm_codes array, alarm_excl_183_pct for actionable alarms), wind/snow divergence.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(NAMED_STRUCT(
    'log_date', log_date,
    'outdoor_min_c', outdoor_min_c,
    'outdoor_avg_c', outdoor_avg_c,
    'delta_t_avg_k', delta_t_avg_k,
    'dm_avg', dm_avg,
    'dm_min', dm_min,
    'compr_duty_pct', compr_duty_pct,
    'compr_freq_avg_hz', compr_freq_avg_hz,
    'compr_starts', compr_starts,
    'heater_duty_pct', heater_duty_pct,
    'heater_energy_kwh', heater_energy_kwh,
    'superheat_avg_k', superheat_avg_k,
    'neg_superheat_pct', neg_superheat_pct,
    'hp_avg_bar', hp_avg_bar,
    'discharge_max_c', discharge_max_c,
    'alarm_pct', alarm_pct,
    'alarm_excl_183_pct', alarm_excl_183_pct,
    'alarm_codes', alarm_codes,
    'bt1_bt28_max_divergence_k', bt1_bt28_max_divergence_k,
    'supply_vs_calc_offset_k', supply_vs_calc_offset_k,
    'total_hours', total_hours
  )))
  FROM (
    SELECT * FROM {_GL}.gold_nibe_daily_summary
    WHERE log_date >= DATE_SUB(CURRENT_DATE(), lookback_days)
    ORDER BY log_date
  )
)
""")
print("get_daily_summary: registered")

# COMMAND ----------

# DBTITLE 1,2. get_flow_diagnosis
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_flow_diagnosis(
  lookback_days INT DEFAULT 7
)
RETURNS STRING
COMMENT 'Diagnoses underfloor heating flow quality. Returns delta_T by compressor frequency band per day. KEY DIAGNOSTIC: if delta_T rises above 5K at high frequencies (100+ Hz), flow rate is too low. Includes heater % per band — heater at high freq + high delta_T = confirmed flow problem. Thresholds: 3-5K good, 5-7K warning, >7K critical.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(NAMED_STRUCT(
    'log_date', log_date,
    'freq_band', freq_band,
    'delta_t_avg_k', delta_t_avg_k,
    'delta_t_max_k', delta_t_max_k,
    'hp_avg_bar', hp_avg_bar,
    'discharge_avg_c', discharge_avg_c,
    'superheat_avg_k', superheat_avg_k,
    'heater_pct', heater_pct,
    'samples', samples
  )))
  FROM (
    SELECT * FROM {_GL}.gold_nibe_flow_quality
    WHERE log_date >= DATE_SUB(CURRENT_DATE(), lookback_days)
    ORDER BY log_date, freq_band
  )
)
""")
print("get_flow_diagnosis: registered")

# COMMAND ----------

# DBTITLE 1,3. get_superheat_status
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_superheat_status(
  lookback_days INT DEFAULT 7
)
RETURNS STRING
COMMENT 'Analyzes refrigerant superheat — primary compressor health indicator. Superheat = BT17 (suction) minus BT16 (evaporator). Positive 3-7K = healthy dry gas. Negative = liquid slugging risk (compressor wear). Causes: EEV over-opening, sensor fault, refrigerant charge issue. Requires qualified technician for diagnosis and repair.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(NAMED_STRUCT(
    'log_date', log_date,
    'superheat_avg_k', superheat_avg_k,
    'superheat_min_k', superheat_min_k,
    'neg_superheat_pct', neg_superheat_pct,
    'compr_freq_avg_hz', compr_freq_avg_hz,
    'hp_avg_bar', hp_avg_bar,
    'discharge_avg_c', discharge_avg_c,
    'discharge_max_c', discharge_max_c
  )))
  FROM (
    SELECT * FROM {_GL}.gold_nibe_daily_summary
    WHERE log_date >= DATE_SUB(CURRENT_DATE(), lookback_days)
    ORDER BY log_date
  )
)
""")
print("get_superheat_status: registered")

# COMMAND ----------

# DBTITLE 1,4. get_heater_analysis
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_heater_analysis(
  lookback_days INT DEFAULT 7
)
RETURNS STRING
COMMENT 'Analyzes backup electric heater usage. Heater is 3-stage: 3/6/9 kW. Any usage means the heat pump alone could not meet demand. Common causes: insufficient underfloor flow, heating curve set too high, extreme cold, wind/snow on outdoor unit. Heater above -10°C outdoor usually indicates a flow or curve problem rather than raw capacity shortage. Do not estimate COP from heater data.'
RETURN (
  SELECT TO_JSON(NAMED_STRUCT(
    'daily', (
      SELECT COLLECT_LIST(NAMED_STRUCT(
        'log_date', log_date,
        'outdoor_min_c', outdoor_min_c,
        'outdoor_avg_c', outdoor_avg_c,
        'heater_duty_pct', heater_duty_pct,
        'heater_energy_kwh', heater_energy_kwh,
        'heater_max_kw', heater_max_kw,
        'delta_t_avg_k', delta_t_avg_k,
        'dm_avg', dm_avg,
        'dm_min', dm_min
      ))
      FROM (
        SELECT * FROM {_GL}.gold_nibe_daily_summary
        WHERE log_date >= DATE_SUB(CURRENT_DATE(), lookback_days)
        ORDER BY log_date
      )
    ),
    'by_outdoor_temp', (
      SELECT COLLECT_LIST(NAMED_STRUCT(
        'bt1_rounded_c', bt1_rounded_c,
        'heater_pct', heater_pct,
        'freq_avg_hz', freq_avg_hz,
        'delta_t_k', delta_t_k,
        'samples', samples
      ))
      FROM (
        SELECT bt1_rounded_c,
          ROUND(AVG(heater_pct), 0) AS heater_pct,
          ROUND(AVG(freq_avg_hz), 0) AS freq_avg_hz,
          ROUND(AVG(delta_t_k), 1) AS delta_t_k,
          SUM(samples) AS samples
        FROM {_GL}.gold_nibe_heating_curve
        WHERE log_date >= DATE_SUB(CURRENT_DATE(), lookback_days)
        GROUP BY bt1_rounded_c
        HAVING SUM(samples) > 50
        ORDER BY bt1_rounded_c
      )
    )
  ))
)
""")
print("get_heater_analysis: registered")

# COMMAND ----------

# DBTITLE 1,5. get_alarm_episodes
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_alarm_episodes(
  lookback_days INT DEFAULT 7
)
RETURNS STRING
COMMENT 'Returns alarm episodes with operating context. Reference: 183=defrost in progress (normal operation, not a fault), 220=high pressure (CRITICAL), 221=low pressure (CRITICAL), 162=high discharge temp (CRITICAL), 163=high condenser inlet (CRITICAL), 224=fan alarm (WARNING), 254=communication error (WARNING), 271=cold outdoor cutoff (WARNING), 272=hot outdoor cutoff (WARNING). For unlisted codes, advise checking the NIBE manual or contacting installer.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(NAMED_STRUCT(
    'log_date', log_date,
    'alarm_number', alarm_number,
    'start_time', start_time,
    'duration_min', duration_min,
    'outdoor_avg_c', outdoor_avg_c,
    'freq_avg_hz', freq_avg_hz,
    'hp_avg_bar', hp_avg_bar,
    'discharge_avg_c', discharge_avg_c,
    'superheat_avg_k', superheat_avg_k,
    'delta_t_avg_k', delta_t_avg_k,
    'heater_avg_kw', heater_avg_kw,
    'dm_avg', dm_avg
  )))
  FROM (
    SELECT * FROM {_GL}.gold_nibe_alarm_episodes
    WHERE log_date >= DATE_SUB(CURRENT_DATE(), lookback_days)
    ORDER BY start_time
  )
)
""")
print("get_alarm_episodes: registered")

# COMMAND ----------

# DBTITLE 1,6. get_hourly_detail
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_hourly_detail(
  target_date STRING
)
RETURNS STRING
COMMENT 'Returns hour-by-hour detail for a specific day (format: YYYY-MM-DD). Use for drill-down after daily summary flags an issue. Provides up to 24 rows with: temperatures, flow quality (delta_T), demand (degree_minutes), compressor duty/freq, heater energy/duty, superheat, HP pressure, discharge temp, alarm count, wind/snow indicator, DHW temp, and heating/DHW mode split per hour.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(NAMED_STRUCT(
    'hour', hour,
    'outdoor_avg_c', outdoor_avg_c,
    'supply_avg_c', supply_avg_c,
    'return_avg_c', return_avg_c,
    'delta_t_avg_k', delta_t_avg_k,
    'dm_avg', dm_avg,
    'freq_avg_hz', freq_avg_hz,
    'compr_duty_pct', compr_duty_pct,
    'heater_kwh', heater_kwh,
    'heater_duty_pct', heater_duty_pct,
    'superheat_avg_k', superheat_avg_k,
    'hp_avg_bar', hp_avg_bar,
    'discharge_avg_c', discharge_avg_c,
    'alarm_samples', alarm_samples,
    'heating_pct', heating_pct,
    'dhw_pct', dhw_pct
  )))
  FROM (
    SELECT * FROM {_GL}.gold_nibe_hourly_detail
    WHERE log_date = target_date
    ORDER BY hour
  )
)
""")
print("get_hourly_detail: registered")

# COMMAND ----------

# DBTITLE 1,7. get_heating_curve
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_heating_curve(
  lookback_days INT DEFAULT 7
)
RETURNS STRING
COMMENT 'Analyzes heating curve: calculated vs actual supply temperature per outdoor degree. Detects: curve too high (wastes energy, HP pressure rises), curve too low (house cold, heater needed), supply cannot reach target (flow/capacity issue). Compares calc_supply (what controller wants) vs actual_supply (what system achieves). heater_pct shows at which outdoor temps backup heater kicks in — should be 0% above -10C.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(NAMED_STRUCT(
    'bt1_rounded_c', bt1_rounded_c,
    'calc_supply_c', calc_supply_c,
    'actual_supply_c', actual_supply_c,
    'return_c', return_c,
    'delta_t_k', delta_t_k,
    'dm_avg', dm_avg,
    'heater_pct', heater_pct,
    'samples', samples
  )))
  FROM (
    SELECT bt1_rounded_c,
      ROUND(AVG(calc_supply_c), 1) AS calc_supply_c,
      ROUND(AVG(actual_supply_c), 1) AS actual_supply_c,
      ROUND(AVG(return_c), 1) AS return_c,
      ROUND(AVG(delta_t_k), 1) AS delta_t_k,
      ROUND(AVG(dm_avg), 0) AS dm_avg,
      ROUND(AVG(heater_pct), 0) AS heater_pct,
      SUM(samples) AS samples
    FROM {_GL}.gold_nibe_heating_curve
    WHERE log_date >= DATE_SUB(CURRENT_DATE(), lookback_days)
    GROUP BY bt1_rounded_c
    HAVING SUM(samples) > 50
    ORDER BY bt1_rounded_c
  )
)
""")
print("get_heating_curve: registered")

# COMMAND ----------

# DBTITLE 1,8. get_data_quality
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_data_quality(
  lookback_days INT DEFAULT 7
)
RETURNS STRING
COMMENT 'Data quality check. Run BEFORE trusting diagnostic results. Returns: coverage_pct (target 95%+; full day = 17280 samples at 5s), logged_hours, max_gap_seconds (>60s = interruption, >600s = significant gap), and null sensor counts. If coverage < 80% or max_gap > 600s, warn that diagnosis for that day may be unreliable.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(NAMED_STRUCT(
    'log_date', log_date,
    'coverage_pct', coverage_pct,
    'logged_hours', logged_hours,
    'max_gap_seconds', max_gap_seconds,
    'null_bt1', null_bt1,
    'null_freq', null_freq,
    'null_hp', null_hp
  )))
  FROM (
    SELECT * FROM {_GL}.gold_nibe_data_quality
    WHERE log_date >= DATE_SUB(CURRENT_DATE(), lookback_days)
    ORDER BY log_date
  )
)
""")
print("get_data_quality: registered")

# COMMAND ----------

# DBTITLE 1,9. get_period_comparison
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_period_comparison(
  period1_start STRING,
  period1_end STRING,
  period2_start STRING,
  period2_end STRING
)
RETURNS STRING
COMMENT 'Compares two time periods side by side. Use for before/after analysis (e.g. before vs after service visit, curve adjustment, flow improvement). Returns aggregated metrics for both periods. IMPORTANT: always check outdoor_avg_c — if temps differ by more than 3K, improvements may be weather-driven. Never claim efficiency gain without comparable outdoor conditions.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(period_data))
  FROM (
    SELECT NAMED_STRUCT(
      'period', 'period_1',
      'start_date', MIN(log_date),
      'end_date', MAX(log_date),
      'days', COUNT(DISTINCT log_date),
      'outdoor_avg_c', ROUND(AVG(outdoor_avg_c), 1),
      'outdoor_min_c', ROUND(MIN(outdoor_min_c), 1),
      'delta_t_avg_k', ROUND(AVG(delta_t_avg_k), 1),
      'dm_avg', ROUND(AVG(dm_avg), 0),
      'compr_duty_pct', ROUND(AVG(compr_duty_pct), 1),
      'compr_freq_avg_hz', ROUND(AVG(compr_freq_avg_hz), 0),
      'heater_duty_pct', ROUND(AVG(heater_duty_pct), 1),
      'heater_total_kwh', ROUND(SUM(heater_energy_kwh), 1),
      'neg_superheat_pct', ROUND(AVG(neg_superheat_pct), 1),
      'superheat_avg_k', ROUND(AVG(superheat_avg_k), 1),
      'hp_avg_bar', ROUND(AVG(hp_avg_bar), 1),
      'alarm_pct', ROUND(AVG(alarm_pct), 1),
      'alarm_excl_183_pct', ROUND(AVG(alarm_excl_183_pct), 1),
      'bt1_bt28_avg_divergence_k', ROUND(AVG(bt1_bt28_avg_divergence_k), 1)
    ) AS period_data
    FROM {_GL}.gold_nibe_daily_summary
    WHERE log_date BETWEEN period1_start AND period1_end

    UNION ALL

    SELECT NAMED_STRUCT(
      'period', 'period_2',
      'start_date', MIN(log_date),
      'end_date', MAX(log_date),
      'days', COUNT(DISTINCT log_date),
      'outdoor_avg_c', ROUND(AVG(outdoor_avg_c), 1),
      'outdoor_min_c', ROUND(MIN(outdoor_min_c), 1),
      'delta_t_avg_k', ROUND(AVG(delta_t_avg_k), 1),
      'dm_avg', ROUND(AVG(dm_avg), 0),
      'compr_duty_pct', ROUND(AVG(compr_duty_pct), 1),
      'compr_freq_avg_hz', ROUND(AVG(compr_freq_avg_hz), 0),
      'heater_duty_pct', ROUND(AVG(heater_duty_pct), 1),
      'heater_total_kwh', ROUND(SUM(heater_energy_kwh), 1),
      'neg_superheat_pct', ROUND(AVG(neg_superheat_pct), 1),
      'superheat_avg_k', ROUND(AVG(superheat_avg_k), 1),
      'hp_avg_bar', ROUND(AVG(hp_avg_bar), 1),
      'alarm_pct', ROUND(AVG(alarm_pct), 1),
      'alarm_excl_183_pct', ROUND(AVG(alarm_excl_183_pct), 1),
      'bt1_bt28_avg_divergence_k', ROUND(AVG(bt1_bt28_avg_divergence_k), 1)
    ) AS period_data
    FROM {_GL}.gold_nibe_daily_summary
    WHERE log_date BETWEEN period2_start AND period2_end
  )
)
""")
print("get_period_comparison: registered")

# COMMAND ----------

# DBTITLE 1,10. get_system_health
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_system_health(
  lookback_days INT DEFAULT 7
)
RETURNS STRING
COMMENT 'Single-call system health check — always call this FIRST. Returns overall status (OK/WARNING/CRITICAL), anomaly score, top issue per day, and per-subsystem status flags. Uses alarm_excl_183_pct (excludes defrost alarm 183 which is normal operation). Scoring combines: flow, superheat, heater, demand, alarms (excl. defrost), discharge, wind/snow, and cycling metrics.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(NAMED_STRUCT(
    'log_date', log_date,
    'overall_status', overall_status,
    'anomaly_score', anomaly_score,
    'top_issue', top_issue,
    'flow_status', flow_status,
    'superheat_status', superheat_status,
    'heater_status', heater_status,
    'demand_status', demand_status,
    'alarm_status', alarm_status,
    'discharge_status', discharge_status,
    'wind_snow_status', wind_snow_status,
    'cycling_status', cycling_status,
    'data_quality_status', data_quality_status
  )))
  FROM (
    SELECT * FROM {_GL}.gold_nibe_anomaly_rules
    WHERE log_date >= DATE_SUB(CURRENT_DATE(), lookback_days)
    ORDER BY log_date
  )
)
""")
print("get_system_health: registered")

# COMMAND ----------

# DBTITLE 1,11. get_cycle_analysis
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_cycle_analysis(
  lookback_days INT DEFAULT 7
)
RETURNS STRING
COMMENT 'Compressor cycle statistics — wear and stability indicators. A cycle = one continuous compressor run. Short cycles (<5 min) indicate control instability or wrong DM settings. High freq_volatility = compressor hunting/instability. cycles_with_alarm = forced termination. avg_dt_spread_k shows flow degradation under sustained load. Healthy system: <20 cycles/day, avg_cycle_min >15, short_cycle_pct <10%, avg_freq_volatility <15.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(NAMED_STRUCT(
    'log_date', log_date,
    'total_cycles', total_cycles,
    'cycles_per_day', cycles_per_day,
    'avg_cycle_min', avg_cycle_min,
    'short_cycle_pct', short_cycle_pct,
    'avg_freq_volatility', avg_freq_volatility,
    'cycles_with_alarm', cycles_with_alarm,
    'cycles_with_bad_superheat', cycles_with_bad_superheat,
    'cycles_with_high_dt', cycles_with_high_dt,
    'avg_dt_spread_k', avg_dt_spread_k,
    'heating_cycles', heating_cycles,
    'dhw_cycles', dhw_cycles
  )))
  FROM (
    SELECT * FROM {_GL}.gold_nibe_cycle_summary
    WHERE log_date >= DATE_SUB(CURRENT_DATE(), lookback_days)
    ORDER BY log_date
  )
)
""")
print("get_cycle_analysis: registered")

# COMMAND ----------

# DBTITLE 1,12. get_worst_cycles
spark.sql(f"""
CREATE OR REPLACE FUNCTION {_GL}.get_worst_cycles(
  target_date STRING,
  top_n INT DEFAULT 10
)
RETURNS STRING
COMMENT 'Returns the N worst compressor cycles for a given day, ranked by a combined problem score (alarm presence=30pts, neg_superheat>30%=20pts, delta_T>7K=15pts, short duration<5min=10pts, HP>28bar=10pts, discharge>95C=10pts, heater>50%=5pts). Use for drill-down after get_cycle_analysis flags issues on a specific day to reveal the exact failure pattern.'
RETURN (
  SELECT TO_JSON(COLLECT_LIST(cycle_data))
  FROM (
    SELECT NAMED_STRUCT(
      'cycle_id', cycle_id,
      'start_time', start_time,
      'duration_min', duration_min,
      'freq_avg_hz', freq_avg_hz,
      'delta_t_avg_k', delta_t_avg_k,
      'superheat_avg_k', superheat_avg_k,
      'neg_superheat_pct', neg_superheat_pct,
      'hp_max_bar', hp_max_bar,
      'discharge_max_c', discharge_max_c,
      'had_alarm', had_alarm,
      'alarm_code', alarm_code,
      'heater_pct', heater_pct,
      'outdoor_avg_c', outdoor_avg_c,
      'dominant_mode_name', dominant_mode_name
    ) AS cycle_data
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (ORDER BY problem_score DESC) AS rn
      FROM (
        SELECT *,
               (  CASE WHEN had_alarm = 1           THEN 30 ELSE 0 END
                + CASE WHEN neg_superheat_pct > 30  THEN 20 ELSE 0 END
                + CASE WHEN delta_t_avg_k > 7       THEN 15 ELSE 0 END
                + CASE WHEN duration_min < 5        THEN 10 ELSE 0 END
                + CASE WHEN hp_max_bar > 28         THEN 10 ELSE 0 END
                + CASE WHEN discharge_max_c > 95    THEN 10 ELSE 0 END
                + CASE WHEN heater_pct > 50         THEN  5 ELSE 0 END
               ) AS problem_score
        FROM {_GL}.gold_nibe_cycles
        WHERE log_date = target_date
      )
    )
    WHERE rn <= top_n
  )
)
""")
print("get_worst_cycles: registered")

# COMMAND ----------

# DBTITLE 1,Verify All 12 Functions
display(spark.sql(f"""
  SELECT routine_name, comment
  FROM {_env}_gold.information_schema.routines
  WHERE routine_schema = 'nibe'
    AND routine_name IN (
      'get_system_health', 'get_daily_summary', 'get_flow_diagnosis',
      'get_superheat_status', 'get_heater_analysis', 'get_alarm_episodes',
      'get_heating_curve', 'get_hourly_detail', 'get_data_quality',
      'get_period_comparison', 'get_cycle_analysis', 'get_worst_cycles'
    )
  ORDER BY routine_name
"""))

# COMMAND ----------

# DBTITLE 1,Quick Smoke Tests
import json

# 1. System health (new primary entry point)
result = spark.sql(f"SELECT {_GL}.get_system_health(7) AS r").collect()[0]['r']
data = json.loads(result)
print(f"get_system_health(7): {len(data)} days")
for d in data:
    print(f"  {d.get('log_date')}: {d.get('overall_status')} | score={d.get('anomaly_score')} | {d.get('top_issue')}")

# 2. Daily summary
result = spark.sql(f"SELECT {_GL}.get_daily_summary(7) AS r").collect()[0]['r']
data = json.loads(result)
print(f"\nget_daily_summary(7): {len(data)} days")
if data:
    d = data[-1]
    print(f"  Latest {d.get('log_date')}: delta_T={d.get('delta_t_avg_k')}K | SH={d.get('superheat_avg_k')}K | neg_SH={d.get('neg_superheat_pct')}% | heater={d.get('heater_energy_kwh')}kWh")

# 3. Flow diagnosis
result = spark.sql(f"SELECT {_GL}.get_flow_diagnosis(7) AS r").collect()[0]['r']
data = json.loads(result)
print(f"\nget_flow_diagnosis(7): {len(data)} band/day rows")
for row in data[:4]:
    print(f"  {row.get('log_date')} {row.get('freq_band')}: delta_T={row.get('delta_t_avg_k')}K | heater={row.get('heater_pct')}%")

# 4. Alarm episodes
result = spark.sql(f"SELECT {_GL}.get_alarm_episodes(30) AS r").collect()[0]['r']
data = json.loads(result)
print(f"\nget_alarm_episodes(30): {len(data)} episodes")
for ep in data[:3]:
    print(f"  Alarm {ep.get('alarm_number')}: {ep.get('start_time')} | {ep.get('duration_min')} min | delta_T={ep.get('delta_t_avg_k')}K")

# 5. Data quality
result = spark.sql(f"SELECT {_GL}.get_data_quality(7) AS r").collect()[0]['r']
data = json.loads(result)
print(f"\nget_data_quality(7): {len(data)} days")
for d in data:
    print(f"  {d.get('log_date')}: coverage={d.get('coverage_pct')}% | max_gap={d.get('max_gap_seconds')}s")

# 6. Cycle analysis (new)
result = spark.sql(f"SELECT {_GL}.get_cycle_analysis(7) AS r").collect()[0]['r']
data = json.loads(result)
print(f"\nget_cycle_analysis(7): {len(data)} days")
for d in data:
    print(f"  {d.get('log_date')}: {d.get('total_cycles')} cycles | avg={d.get('avg_cycle_min')}min | short={d.get('short_cycle_pct')}% | alarms={d.get('cycles_with_alarm')}")
