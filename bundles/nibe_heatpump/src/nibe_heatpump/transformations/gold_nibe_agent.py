"""
NIBE Heat Pump Log — Gold Layer (Diagnostic Agent Tables)
=========================================================
Eight materialized views pre-aggregated for LLM-based diagnostics.

Raw silver is ~17,000 rows/day × 45+ columns — far too large for LLM context.
These tables reduce each agent tool call to 50–200 rows covering the key
diagnostic signals, keeping token cost near $0.01 per full diagnosis.

Table purposes:
  gold_nibe_daily_summary    One row per day — agent entry point
  gold_nibe_hourly_detail    24 rows per day — intra-day drill-down
  gold_nibe_flow_quality     ΔT by compressor frequency band — flow diagnosis
  gold_nibe_heating_curve    Performance per outdoor °C — curve calibration
  gold_nibe_alarm_episodes   Alarm episodes with operational context
  gold_nibe_data_quality     Daily data coverage & gap metrics — reliability check
  gold_nibe_defrost_cycles   Defrost episode detection — air-source #1 issue
  gold_nibe_dhw_cycles       DHW heating episode detection — tank health

Key silver columns used here:
  bt71_return_c      : return manifold sensor — (bt25_supply_c − bt71_return_c) = floor ΔT
  delta_t_k          : pre-computed supply − return; primary flow-quality metric
  superheat_k        : pre-computed BT17 − BT16; primary refrigerant-health metric
  negative_superheat : pre-computed flag (compr running AND superheat < 0)
  defrost_active     : pre-computed flag (BT16 > 10 °C = hot-gas reversal)
  bt1_bt28_divergence_k : pre-computed |BT1 − BT28|; wind/snow indicator
  tot_int_add_kw     : instantaneous stage power 0/3/6/9 kW (raw ÷100)
  priority_mode      : 10=standby/defrost, 20=DHW, 30=space heating
  saturation_temp_c  : condensing saturation temp (from HP tables)
  bt15_liquid_c      : liquid-line temp before expansion valve
  bt7_dhw_charge_c   : DHW tank mid/charging sensor
  gp10_circ_pump_on  : indoor circulation pump (boolean)
  gp12_fan_pct       : outdoor unit fan speed (%)
  bt1_average_c      : rolling average of BT1 outdoor temp

Implementation notes:
  - All time/energy accumulations use gap-aware intervals: each row's contribution
    is LEAST(gap_seconds, 10) instead of a fixed 5 s, so data gaps do not inflate
    running hours or energy totals. The cap of 10 s allows for minor jitter while
    treating genuine outages (>10 s) as a single 10 s sample at most.
  - ORDER BY is omitted from all materialized views — Delta tables ignore row
    ordering. Use cluster_by for physical layout instead.
"""

from pyspark import pipelines as dp

# ── Layer catalog shortcuts ───────────────────────────────────────────────────
_env = spark.conf.get("env_scope")   # "dev" or "prod"
_SL  = f"{_env}_silver.nibe"         # silver layer catalog.schema
_GL  = f"{_env}_gold.nibe"           # gold layer catalog.schema

# ── Reusable SQL fragment: gap-aware interval ────────────────────────────────
# Produces `gap_interval` column — seconds this row represents, capped at 10 s.
# First row of each day has NULL lag → COALESCE to 5 (nominal interval).
_GAP_CTE = """
    base AS (
        SELECT
            *,
            COALESCE(
                LEAST(
                    TIMESTAMPDIFF(SECOND,
                        LAG(logged_at) OVER (PARTITION BY log_date_parsed ORDER BY logged_at),
                        logged_at
                    ),
                    10
                ),
                5
            ) AS gap_interval
        FROM {silver}.silver_nibe_logs
    )
"""


# ── 1. Daily Summary ──────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_daily_summary",
    comment=(
        "One row per calendar day. Primary entry point for the diagnostic agent. "
        "Covers: outdoor temps, supply/return temps, delta_T (flow-quality indicator), "
        "degree-minutes (demand controller), compressor duty/freq/starts/hours, "
        "heater energy kWh and duty, suction superheat (BT17 − BT16), "
        "condenser subcooling (saturation − BT15, charge indicator), "
        "high-side pressure, discharge temp, alarm summary, defrost count, "
        "BT1/BT28 divergence (wind/snow indicator), sensor drift, "
        "standby parasitic (pump/fan when idle), short-cycling count, and mode split. "
        "Time/energy metrics are gap-aware (capped at 10 s per sample interval)."
    ),
    table_properties={"quality": "gold", "agent.purpose": "daily_diagnostics"},
    cluster_by=["log_date"],
)
def gold_nibe_daily_summary():
    return spark.sql(f"""
        WITH
        {_GAP_CTE.format(silver=_SL)},
        starts AS (
            SELECT log_date_parsed AS log_date, SUM(is_start) AS compr_starts
            FROM (
                SELECT
                    log_date_parsed,
                    CASE
                        WHEN compr_freq_act_hz > 0
                         AND LAG(compr_freq_act_hz, 1, 0)
                               OVER (PARTITION BY log_date_parsed ORDER BY logged_at) = 0
                        THEN 1 ELSE 0
                    END AS is_start
                FROM base
            )
            GROUP BY log_date_parsed
        ),
        defrost AS (
            SELECT log_date_parsed AS log_date, SUM(is_defrost_start) AS defrost_cycles
            FROM (
                SELECT
                    log_date_parsed,
                    CASE
                        WHEN defrost_active
                         AND NOT COALESCE(
                               LAG(defrost_active)
                                 OVER (PARTITION BY log_date_parsed ORDER BY logged_at),
                               FALSE)
                        THEN 1 ELSE 0
                    END AS is_defrost_start
                FROM base
            )
            GROUP BY log_date_parsed
        ),
        -- Short-cycling: starts where the preceding off-period was < 5 minutes.
        -- Rapid cycling stresses relays and causes refrigerant migration.
        short_cycles AS (
            SELECT log_date_parsed AS log_date, SUM(is_short_cycle) AS short_cycle_count
            FROM (
                SELECT
                    log_date_parsed,
                    CASE
                        WHEN compr_freq_act_hz > 0
                         AND LAG(compr_freq_act_hz, 1, 0)
                               OVER (PARTITION BY log_date_parsed ORDER BY logged_at) = 0
                         AND TIMESTAMPDIFF(SECOND, off_start, logged_at) < 300
                        THEN 1 ELSE 0
                    END AS is_short_cycle
                FROM (
                    SELECT *,
                        -- Track when compressor last turned off
                        LAST_VALUE(CASE WHEN compr_freq_act_hz = 0
                                         AND LAG(compr_freq_act_hz, 1, 0)
                                               OVER (PARTITION BY log_date_parsed ORDER BY logged_at) > 0
                                        THEN logged_at END, TRUE)
                            OVER (PARTITION BY log_date_parsed ORDER BY logged_at) AS off_start
                    FROM base
                )
            )
            GROUP BY log_date_parsed
        ),
        daily AS (
            SELECT
                log_date_parsed AS log_date,

                -- Outdoor conditions
                ROUND(MIN(bt1_outdoor_temp_c), 1)  AS outdoor_min_c,
                ROUND(MAX(bt1_outdoor_temp_c), 1)  AS outdoor_max_c,
                ROUND(AVG(bt1_outdoor_temp_c), 1)  AS outdoor_avg_c,

                -- Supply / return (heating mode, compressor running only)
                ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 AND priority_mode = 30
                               THEN bt25_supply_c     END), 1) AS supply_avg_c,
                ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 AND priority_mode = 30
                               THEN bt71_return_c     END), 1) AS return_avg_c,

                -- FLOW QUALITY: delta_T (pre-computed supply − return manifold).
                -- Target: 3–5 K. >5 K = flow issue. >7 K = critical.
                ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 AND priority_mode = 30
                               THEN delta_t_k END), 1) AS delta_t_avg_k,
                ROUND(MAX(CASE WHEN compr_freq_act_hz > 0
                               THEN delta_t_k END), 1) AS delta_t_max_k,

                -- Heating curve: calculated setpoint vs actual
                ROUND(AVG(CASE WHEN priority_mode = 30 THEN calc_supply_c END), 1) AS calc_supply_avg_c,
                ROUND(AVG(CASE WHEN compr_freq_act_hz > 0 AND priority_mode = 30
                               THEN bt25_supply_c - calc_supply_c END), 1) AS supply_vs_calc_offset_k,

                -- DEGREE MINUTES: integrating demand controller.
                -- Target: > −200. −540 = saturated (maximum deficit).
                ROUND(AVG(degree_minutes), 0)  AS dm_avg,
                MIN(degree_minutes)            AS dm_min,
                MAX(degree_minutes)            AS dm_max,

                -- Compressor
                ROUND(SUM(CASE WHEN compr_freq_act_hz > 0 THEN 1 ELSE 0 END) * 100.0
                      / COUNT(*), 1)                                            AS compr_duty_pct,
                ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                               THEN compr_freq_act_hz END), 0)                 AS compr_freq_avg_hz,
                ROUND(MAX(compr_freq_act_hz), 0)                               AS compr_freq_max_hz,

                -- Gap-aware running hours: use actual interval per row, capped at 10 s
                ROUND(SUM(CASE WHEN compr_freq_act_hz > 0
                               THEN gap_interval ELSE 0 END) / 3600.0, 1)     AS compr_running_hours,
                ROUND(SUM(gap_interval) / 3600.0, 1)                           AS total_hours,

                -- BACKUP HEATER — cost indicator.
                -- tot_int_add_kw = instantaneous stage power (0/3/6/9 kW).
                ROUND(SUM(CASE WHEN tot_int_add_kw > 0 THEN 1 ELSE 0 END) * 100.0
                      / COUNT(*), 1)                                            AS heater_duty_pct,
                -- Gap-aware energy: kW × actual interval
                ROUND(SUM(tot_int_add_kw * gap_interval / 3600.0), 1)         AS heater_energy_kwh,
                MAX(tot_int_add_kw)                                             AS heater_max_kw,

                -- SUPERHEAT: pre-computed BT17 − BT16.
                -- Target: 3–7 K. <0 K = liquid slugging → compressor damage.
                ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                               THEN superheat_k END), 1)                        AS superheat_avg_k,
                ROUND(MIN(CASE WHEN compr_freq_act_hz > 0
                               THEN superheat_k END), 1)                        AS superheat_min_k,
                ROUND(
                    SUM(CASE WHEN negative_superheat THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(SUM(CASE WHEN compr_freq_act_hz > 0 THEN 1 ELSE 0 END), 0),
                1)                                                               AS neg_superheat_pct,

                -- Pressures
                ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                               THEN bp4_hp_bar END), 1)                         AS hp_avg_bar,
                ROUND(MAX(bp4_hp_bar), 1)                                       AS hp_max_bar,
                ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                               THEN lp_raw END), 1)                             AS lp_avg_raw,

                -- Discharge temperature (BT14)
                ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                               THEN bt14_discharge_c END), 1)                   AS discharge_avg_c,
                ROUND(MAX(bt14_discharge_c), 1)                                 AS discharge_max_c,

                -- Alarms
                SUM(CASE WHEN alarm_active THEN 1 ELSE 0 END)                  AS alarm_samples,
                ROUND(SUM(CASE WHEN alarm_active THEN 1 ELSE 0 END) * 100.0
                      / COUNT(*), 1)                                             AS alarm_pct,
                -- FIX: FILTER avoids NULL contamination from non-alarm rows
                ARRAY_DISTINCT(COLLECT_LIST(alarm_number)
                    FILTER (WHERE alarm_active AND alarm_number > 0))           AS alarm_codes,
                -- Actionable alarms only — excludes 183 (defrost = normal operation)
                ROUND(SUM(CASE WHEN alarm_active AND alarm_number != 183 THEN 1 ELSE 0 END) * 100.0
                      / COUNT(*), 1)                                             AS alarm_excl_183_pct,

                -- Defrost raw sample count (for duty % calculation)
                SUM(CASE WHEN defrost_active THEN 1 ELSE 0 END)                AS defrost_samples,

                -- BT1 vs BT28 divergence — wind/snow obstruction on outdoor unit.
                -- Normal: <1 K. >3 K = possible obstruction. >5 K = confirmed issue.
                ROUND(AVG(bt1_bt28_divergence_k), 1)                           AS bt1_bt28_avg_divergence_k,
                ROUND(MAX(bt1_bt28_divergence_k), 1)                           AS bt1_bt28_max_divergence_k,

                -- Mode split (priority_mode 30=heating, 20=DHW)
                ROUND(SUM(CASE WHEN priority_mode = 30 THEN 1 ELSE 0 END) * 100.0
                      / COUNT(*), 0)                                             AS heating_pct,
                ROUND(SUM(CASE WHEN priority_mode = 20 THEN 1 ELSE 0 END) * 100.0
                      / COUNT(*), 0)                                             AS dhw_pct,

                -- DHW tank
                ROUND(AVG(bt6_dhw_top_c), 1)  AS dhw_avg_c,
                ROUND(MIN(bt6_dhw_top_c), 1)  AS dhw_min_c,

                -- CONDENSER SUBCOOLING: BT12 (condensing temp) − BT15 (liquid-line).
                -- saturation_temp_c is always 9.9 °C (firmware N/A sentinel) — do not use.
                -- BT12 is the actual condenser refrigerant temperature sensor.
                -- Charge indicator. Target: 3–8 K. <2 K = undercharge. >10 K = overcharge/fouling.
                ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                               THEN bt12_condenser_c - bt15_liquid_c END), 1)  AS subcooling_avg_k,
                ROUND(MIN(CASE WHEN compr_freq_act_hz > 0
                               THEN bt12_condenser_c - bt15_liquid_c END), 1)  AS subcooling_min_k,

                -- STANDBY PARASITIC: pump/fan running when system idle.
                -- Catches stuck actuators or control logic issues.
                ROUND(SUM(CASE WHEN compr_freq_act_hz = 0 AND tot_int_add_kw = 0
                                AND priority_mode = 10 AND gp10_circ_pump_on
                               THEN 1 ELSE 0 END) * 100.0
                      / NULLIF(SUM(CASE WHEN compr_freq_act_hz = 0 AND tot_int_add_kw = 0
                                             AND priority_mode = 10
                                        THEN 1 ELSE 0 END), 0), 1) AS standby_circ_pump_pct,
                ROUND(AVG(CASE WHEN compr_freq_act_hz = 0 AND tot_int_add_kw = 0
                                AND priority_mode = 10
                               THEN gp12_fan_pct END), 1)                      AS standby_fan_avg_pct,

                -- OUTDOOR SENSOR DRIFT: BT1 instantaneous vs BT1 rolling average.
                -- Persistent divergence > 2 K = noisy or drifting sensor.
                ROUND(AVG(ABS(bt1_outdoor_temp_c - bt1_average_c)), 1)         AS bt1_vs_avg_divergence_k,

                COUNT(*) AS total_samples

            FROM base
            GROUP BY log_date_parsed
        )
        SELECT
            d.*,
            s.compr_starts,
            df.defrost_cycles,
            sc.short_cycle_count
        FROM daily d
        LEFT JOIN starts       s  ON d.log_date = s.log_date
        LEFT JOIN defrost      df ON d.log_date = df.log_date
        LEFT JOIN short_cycles sc ON d.log_date = sc.log_date
    """)


# ── 2. Hourly Detail ──────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_hourly_detail",
    comment=(
        "Hour-by-hour breakdown — up to 24 rows per calendar day. "
        "Used by the agent to drill down into a specific day flagged by the daily summary. "
        "Covers all key metrics per hour: temperatures, delta_T, degree-minutes, "
        "compressor duty/freq, heater energy/duty, superheat (avg + P10/P50/P90), "
        "subcooling, HP pressure, discharge temp, alarms, wind/snow divergence, "
        "DHW, and mode split. "
        "Heater energy is gap-aware (capped at 10 s per sample interval)."
    ),
    table_properties={"quality": "gold", "agent.purpose": "hourly_drilldown"},
    cluster_by=["log_date"],
)
def gold_nibe_hourly_detail():
    return spark.sql(f"""
        WITH
        {_GAP_CTE.format(silver=_SL)}
        SELECT
            log_date_parsed                                                                AS log_date,
            HOUR(logged_at)                                                                AS hour,

            -- Temperatures
            ROUND(AVG(bt1_outdoor_temp_c), 1)                                             AS outdoor_avg_c,
            ROUND(MIN(bt1_outdoor_temp_c), 1)                                             AS outdoor_min_c,
            ROUND(AVG(bt25_supply_c), 1)                                                   AS supply_avg_c,
            ROUND(AVG(bt71_return_c), 1)                                                   AS return_avg_c,
            ROUND(AVG(calc_supply_c), 1)                                                   AS calc_supply_avg_c,

            -- Flow quality
            ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                           THEN delta_t_k END), 1)                                        AS delta_t_avg_k,

            -- Demand
            ROUND(AVG(degree_minutes), 0)                                                  AS dm_avg,
            ROUND(MIN(degree_minutes), 0)                                                  AS dm_min,

            -- Compressor
            ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                           THEN compr_freq_act_hz END), 0)                                AS freq_avg_hz,
            ROUND(SUM(CASE WHEN compr_freq_act_hz > 0 THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 0)                                                           AS compr_duty_pct,

            -- Backup heater — gap-aware energy
            ROUND(SUM(tot_int_add_kw * gap_interval / 3600.0), 2)                        AS heater_kwh,
            ROUND(SUM(CASE WHEN tot_int_add_kw > 0 THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 0)                                                           AS heater_duty_pct,

            -- Refrigerant health — superheat with percentile breakdown
            ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                           THEN superheat_k END), 1)                                      AS superheat_avg_k,
            -- P10/P50/P90 catch intermittent liquid slugging that averages away
            ROUND(PERCENTILE_APPROX(
                CASE WHEN compr_freq_act_hz > 0 THEN superheat_k END,
                0.1), 1)                                                                   AS superheat_p10_k,
            ROUND(PERCENTILE_APPROX(
                CASE WHEN compr_freq_act_hz > 0 THEN superheat_k END,
                0.5), 1)                                                                   AS superheat_p50_k,
            ROUND(PERCENTILE_APPROX(
                CASE WHEN compr_freq_act_hz > 0 THEN superheat_k END,
                0.9), 1)                                                                   AS superheat_p90_k,

            -- Condenser subcooling — charge indicator (BT12 condenser temp − BT15 liquid-line)
            ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                           THEN bt12_condenser_c - bt15_liquid_c END), 1)                AS subcooling_avg_k,
            ROUND(MIN(CASE WHEN compr_freq_act_hz > 0
                           THEN bt12_condenser_c - bt15_liquid_c END), 1)                AS subcooling_min_k,

            ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                           THEN bp4_hp_bar END), 1)                                       AS hp_avg_bar,
            ROUND(AVG(CASE WHEN compr_freq_act_hz > 0
                           THEN bt14_discharge_c END), 1)                                 AS discharge_avg_c,

            -- Alarms
            SUM(CASE WHEN alarm_active THEN 1 ELSE 0 END)                                 AS alarm_samples,

            -- Wind/snow indicator
            ROUND(AVG(bt1_bt28_divergence_k), 1)                                          AS bt1_bt28_avg_divergence_k,

            -- DHW
            ROUND(AVG(bt6_dhw_top_c), 1)                                                  AS dhw_avg_c,

            -- Mode split
            ROUND(SUM(CASE WHEN priority_mode = 30 THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 0)                                                           AS heating_pct,
            ROUND(SUM(CASE WHEN priority_mode = 20 THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 0)                                                           AS dhw_pct,

            COUNT(*)                                                                       AS samples
        FROM base
        GROUP BY log_date_parsed, HOUR(logged_at)
    """)


# ── 3. Flow Quality (ΔT by Frequency Band) ───────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_flow_quality",
    comment=(
        "Delta_T (supply − return temperature difference) grouped by compressor "
        "frequency band per day. The primary diagnostic for underfloor heating flow problems. "
        "Bands: 01_low_1-39Hz, 02_mid_40-69Hz, 03_high_70-99Hz, 04_max_100+Hz. "
        "Thresholds: delta_t_avg_k 3–5 K = good, 5–7 K = insufficient flow, "
        ">7 K = critical (compressor stress, uneven loop heating). "
        "Heating mode (priority_mode=30), compressor running rows only."
    ),
    table_properties={"quality": "gold", "agent.purpose": "flow_diagnosis"},
    cluster_by=["log_date"],
)
def gold_nibe_flow_quality():
    return spark.sql(f"""
        SELECT
            log_date_parsed AS log_date,
            -- FIX: explicit non-overlapping boundaries
            CASE
                WHEN compr_freq_act_hz >= 1   AND compr_freq_act_hz < 40  THEN '01_low_1-39Hz'
                WHEN compr_freq_act_hz >= 40  AND compr_freq_act_hz < 70  THEN '02_mid_40-69Hz'
                WHEN compr_freq_act_hz >= 70  AND compr_freq_act_hz < 100 THEN '03_high_70-99Hz'
                WHEN compr_freq_act_hz >= 100                             THEN '04_max_100+Hz'
            END AS freq_band,
            ROUND(AVG(delta_t_k), 1)                                         AS delta_t_avg_k,
            ROUND(MAX(delta_t_k), 1)                                         AS delta_t_max_k,
            ROUND(AVG(bp4_hp_bar), 1)                                        AS hp_avg_bar,
            ROUND(AVG(bt14_discharge_c), 1)                                  AS discharge_avg_c,
            ROUND(AVG(superheat_k), 1)                                       AS superheat_avg_k,
            ROUND(SUM(CASE WHEN tot_int_add_kw > 0 THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 0)                                              AS heater_pct,
            COUNT(*)                                                          AS samples
        FROM {_SL}.silver_nibe_logs
        WHERE compr_freq_act_hz > 0 AND priority_mode = 30
        GROUP BY
            log_date_parsed,
            CASE
                WHEN compr_freq_act_hz >= 1   AND compr_freq_act_hz < 40  THEN '01_low_1-39Hz'
                WHEN compr_freq_act_hz >= 40  AND compr_freq_act_hz < 70  THEN '02_mid_40-69Hz'
                WHEN compr_freq_act_hz >= 70  AND compr_freq_act_hz < 100 THEN '03_high_70-99Hz'
                WHEN compr_freq_act_hz >= 100                             THEN '04_max_100+Hz'
            END
    """)


# ── 4. Heating Curve (Performance by Outdoor °C) ─────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_heating_curve",
    comment=(
        "Heating curve analysis: calculated vs actual supply temperature, delta_T, "
        "and heater usage grouped by outdoor temperature (rounded to 1 °C) per day. "
        "Requires >20 samples per outdoor-degree bucket to suppress noise. "
        "Use to detect: curve too high (energy waste, high HP pressure), "
        "curve too low (cold house, heater dependency), "
        "or supply unable to reach target (flow/capacity constraint). "
        "Heating mode (priority_mode=30), compressor running rows only."
    ),
    table_properties={"quality": "gold", "agent.purpose": "heating_curve"},
    cluster_by=["log_date"],
)
def gold_nibe_heating_curve():
    return spark.sql(f"""
        SELECT
            log_date_parsed                                     AS log_date,
            ROUND(bt1_outdoor_temp_c, 0)                        AS bt1_rounded_c,
            ROUND(AVG(calc_supply_c), 1)                        AS calc_supply_c,
            ROUND(AVG(bt25_supply_c), 1)                        AS actual_supply_c,
            ROUND(AVG(bt71_return_c), 1)                        AS return_c,
            ROUND(AVG(delta_t_k), 1)                            AS delta_t_k,
            ROUND(AVG(degree_minutes), 0)                       AS dm_avg,
            ROUND(AVG(compr_freq_act_hz), 0)                    AS freq_avg_hz,
            ROUND(SUM(CASE WHEN tot_int_add_kw > 0 THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 0)                                AS heater_pct,
            ROUND(AVG(bp4_hp_bar), 1)                           AS hp_avg_bar,
            COUNT(*)                                            AS samples
        FROM {_SL}.silver_nibe_logs
        WHERE compr_freq_act_hz > 0 AND priority_mode = 30
        GROUP BY log_date_parsed, ROUND(bt1_outdoor_temp_c, 0)
        HAVING COUNT(*) > 20
    """)


# ── 5. Alarm Episodes ─────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_alarm_episodes",
    comment=(
        "Alarm episodes grouped per day with operational context. "
        "Episodes are detected within each day: a new episode begins when alarm_number changes "
        "from the previous row (including transitions from/to no alarm). "
        "Each row: start/end time, duration, outdoor temp, compressor freq, "
        "HP pressure, discharge temp, superheat, delta_T, heater power, DM. "
        "Known codes: 183 = defrost cycle (normal operation), "
        "271 = low-temp cutoff (<−20 °C, normal safety), "
        "162/163 = high-discharge-pressure emergency stop. "
        "episode_id is unique within a day — combine with log_date for a global key."
    ),
    table_properties={"quality": "gold", "agent.purpose": "alarm_analysis"},
    cluster_by=["log_date"],
)
def gold_nibe_alarm_episodes():
    return spark.sql(f"""
        WITH all_rows_with_alarm AS (
            SELECT
                *,
                CASE WHEN alarm_active THEN alarm_number ELSE 0 END AS alarm_for_grouping
            FROM {_SL}.silver_nibe_logs
        ),
        numbered AS (
            SELECT
                *,
                CASE
                    WHEN alarm_for_grouping != LAG(alarm_for_grouping, 1, 0) OVER (
                        PARTITION BY log_date_parsed ORDER BY logged_at
                    ) THEN 1 ELSE 0
                END AS new_episode
            FROM all_rows_with_alarm
        ),
        with_episode_id AS (
            SELECT
                *,
                SUM(new_episode) OVER (
                    PARTITION BY log_date_parsed ORDER BY logged_at
                ) AS episode_id
            FROM numbered
            WHERE alarm_active
        )
        SELECT
            log_date_parsed                                              AS log_date,
            episode_id,
            alarm_number,
            MIN(logged_at)                                              AS start_time,
            MAX(logged_at)                                              AS end_time,
            -- FIX: floor at 5 s so single-sample episodes don't show 0.0 min
            ROUND(GREATEST(
                TIMESTAMPDIFF(SECOND, MIN(logged_at), MAX(logged_at)), 5
            ) / 60.0, 1)                                                AS duration_min,
            ROUND(AVG(bt1_outdoor_temp_c), 1)                          AS outdoor_avg_c,
            ROUND(AVG(compr_freq_act_hz), 0)                           AS freq_avg_hz,
            ROUND(AVG(bp4_hp_bar), 1)                                  AS hp_avg_bar,
            ROUND(AVG(bt14_discharge_c), 1)                            AS discharge_avg_c,
            ROUND(AVG(superheat_k), 1)                                 AS superheat_avg_k,
            ROUND(AVG(delta_t_k), 1)                                   AS delta_t_avg_k,
            ROUND(AVG(tot_int_add_kw), 1)                              AS heater_avg_kw,
            ROUND(AVG(degree_minutes), 0)                              AS dm_avg,
            ROUND(AVG(bt1_bt28_divergence_k), 1)                       AS bt1_bt28_divergence_k,
            COUNT(*)                                                   AS samples
        FROM with_episode_id
        GROUP BY log_date_parsed, episode_id, alarm_number
    """)


# ── 6. Data Quality ───────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_data_quality",
    comment=(
        "Daily data quality metrics. Check before trusting diagnostic results. "
        "coverage_pct: target 95%+. Expected full day = 17,280 samples (24h × 720/h at 5s). "
        "max_gap_seconds: gaps >60s indicate data interruptions. >600s = significant gap. "
        "gaps_over_60s / gaps_over_600s: count of interruptions by severity. "
        "Null counts: non-zero counts indicate sensor faults or communication errors."
    ),
    table_properties={"quality": "gold", "agent.purpose": "data_quality"},
    cluster_by=["log_date"],
)
def gold_nibe_data_quality():
    return spark.sql(f"""
        SELECT
            log_date_parsed AS log_date,
            COUNT(*)                                                      AS total_samples,
            -- Expected 17,280 samples for a full 24h day at 5-second intervals
            ROUND(COUNT(*) * 100.0 / 17280, 1)                           AS coverage_pct,
            ROUND(COUNT(*) * 5.0 / 3600, 1)                              AS logged_hours,

            -- Null sensor counts — non-zero indicates sensor fault or DQ drop
            SUM(CASE WHEN bt1_outdoor_temp_c IS NULL THEN 1 ELSE 0 END)  AS null_bt1,
            SUM(CASE WHEN bt25_supply_c IS NULL THEN 1 ELSE 0 END)       AS null_bt25,
            SUM(CASE WHEN compr_freq_act_hz IS NULL THEN 1 ELSE 0 END)   AS null_freq,
            SUM(CASE WHEN bp4_hp_bar IS NULL THEN 1 ELSE 0 END)          AS null_hp,
            SUM(CASE WHEN bt16_evap_c IS NULL THEN 1 ELSE 0 END)         AS null_bt16,

            -- Gap distribution — gives the agent severity context, not just the worst case
            MAX(gap_seconds)                                              AS max_gap_seconds,
            SUM(CASE WHEN gap_seconds > 60  THEN 1 ELSE 0 END)          AS gaps_over_60s,
            SUM(CASE WHEN gap_seconds > 600 THEN 1 ELSE 0 END)          AS gaps_over_600s

        FROM (
            SELECT
                *,
                TIMESTAMPDIFF(SECOND,
                    LAG(logged_at) OVER (PARTITION BY log_date_parsed ORDER BY logged_at),
                    logged_at
                ) AS gap_seconds
            FROM {_SL}.silver_nibe_logs
        )
        GROUP BY log_date_parsed
    """)


# ── 7. Defrost Cycles ─────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_defrost_cycles",
    comment=(
        "Defrost episode detection — the #1 air-source performance issue in cold climates. "
        "Episode = continuous period where defrost_active is TRUE (BT16 > 10 °C = hot-gas reversal). "
        "Tracks per episode: start/end time, duration, outdoor temp, evaporator temps "
        "(BT16 at start and end), fan speed during defrost, compressor freq, HP pressure, "
        "and whether an alarm occurred during the defrost. "
        "Normal: 5–15 cycles/day in winter, 3–8 min each. "
        "Excessive: >20 cycles/day or avg > 10 min = possible coil fouling, fan fault, "
        "or refrigerant issue. Wet-frost zone (−5 to +3 °C outdoor) is worst. "
        "episode_id is unique within a day."
    ),
    table_properties={"quality": "gold", "agent.purpose": "defrost_analysis"},
    cluster_by=["log_date"],
)
def gold_nibe_defrost_cycles():
    return spark.sql(f"""
        WITH transitions AS (
            SELECT
                *,
                CASE
                    WHEN defrost_active
                     AND NOT COALESCE(
                           LAG(defrost_active)
                             OVER (PARTITION BY log_date_parsed ORDER BY logged_at),
                           FALSE)
                    THEN 1 ELSE 0
                END AS new_episode
            FROM {_SL}.silver_nibe_logs
        ),
        with_episode_id AS (
            SELECT
                *,
                SUM(new_episode) OVER (
                    PARTITION BY log_date_parsed ORDER BY logged_at
                ) AS episode_id
            FROM transitions
            WHERE defrost_active
        )
        SELECT
            log_date_parsed                                              AS log_date,
            episode_id,
            MIN(logged_at)                                              AS start_time,
            MAX(logged_at)                                              AS end_time,
            ROUND(GREATEST(
                TIMESTAMPDIFF(SECOND, MIN(logged_at), MAX(logged_at)), 5
            ) / 60.0, 1)                                                AS duration_min,

            -- Outdoor conditions during defrost
            ROUND(AVG(bt1_outdoor_temp_c), 1)                          AS outdoor_avg_c,
            ROUND(MIN(bt1_outdoor_temp_c), 1)                          AS outdoor_min_c,

            -- Evaporator temp: start vs end shows defrost effectiveness
            ROUND(MIN(STRUCT(logged_at, bt16_evap_c)).bt16_evap_c, 1)  AS bt16_start_c,
            ROUND(MAX(STRUCT(logged_at, bt16_evap_c)).bt16_evap_c, 1)  AS bt16_end_c,
            ROUND(AVG(bt16_evap_c), 1)                                 AS bt16_avg_c,

            -- Fan behaviour during defrost
            ROUND(AVG(gp12_fan_pct), 0)                                AS fan_avg_pct,

            -- Compressor state during defrost
            ROUND(AVG(compr_freq_act_hz), 0)                           AS freq_avg_hz,
            ROUND(AVG(bp4_hp_bar), 1)                                  AS hp_avg_bar,
            ROUND(AVG(bt14_discharge_c), 1)                            AS discharge_avg_c,

            -- Did an alarm occur during this defrost?
            MAX(CASE WHEN alarm_active AND alarm_number != 183 THEN 1 ELSE 0 END) AS had_non_defrost_alarm,

            COUNT(*)                                                   AS samples
        FROM with_episode_id
        GROUP BY log_date_parsed, episode_id
    """)


# ── 8. DHW Cycles ─────────────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_dhw_cycles",
    comment=(
        "DHW (domestic hot water) heating episode detection. "
        "Episode = continuous period where priority_mode = 20 (DHW priority). "
        "Tracks per episode: start/end time, duration, tank temperatures "
        "(BT6 top and BT7 charging at start and end), compressor freq, "
        "heater usage during DHW, and superheat. "
        "Normal: 1–3 cycles/day, 20–45 min each, end temp ≥ 48 °C. "
        "Long cycles (>60 min) or falling end-temps signal scaling, "
        "failing tank sensor, or insufficient compressor capacity. "
        "Heater running during DHW above −5 °C outdoor is unusual. "
        "episode_id is unique within a day."
    ),
    table_properties={"quality": "gold", "agent.purpose": "dhw_analysis"},
    cluster_by=["log_date"],
)
def gold_nibe_dhw_cycles():
    return spark.sql(f"""
        WITH transitions AS (
            SELECT
                *,
                CASE
                    WHEN priority_mode = 20
                     AND COALESCE(
                           LAG(priority_mode)
                             OVER (PARTITION BY log_date_parsed ORDER BY logged_at),
                           0) != 20
                    THEN 1 ELSE 0
                END AS new_episode
            FROM {_SL}.silver_nibe_logs
        ),
        with_episode_id AS (
            SELECT
                *,
                SUM(new_episode) OVER (
                    PARTITION BY log_date_parsed ORDER BY logged_at
                ) AS episode_id
            FROM transitions
            WHERE priority_mode = 20
        )
        SELECT
            log_date_parsed                                              AS log_date,
            episode_id,
            MIN(logged_at)                                              AS start_time,
            MAX(logged_at)                                              AS end_time,
            ROUND(GREATEST(
                TIMESTAMPDIFF(SECOND, MIN(logged_at), MAX(logged_at)), 5
            ) / 60.0, 1)                                                AS duration_min,

            -- Tank temperatures: start vs end shows heating effectiveness
            ROUND(MIN(STRUCT(logged_at, bt6_dhw_top_c)).bt6_dhw_top_c, 1)   AS bt6_start_c,
            ROUND(MAX(STRUCT(logged_at, bt6_dhw_top_c)).bt6_dhw_top_c, 1)   AS bt6_end_c,
            ROUND(MIN(STRUCT(logged_at, bt7_dhw_charge_c)).bt7_dhw_charge_c, 1) AS bt7_start_c,
            ROUND(MAX(STRUCT(logged_at, bt7_dhw_charge_c)).bt7_dhw_charge_c, 1) AS bt7_end_c,

            -- Outdoor temp context
            ROUND(AVG(bt1_outdoor_temp_c), 1)                          AS outdoor_avg_c,

            -- Compressor during DHW
            ROUND(AVG(compr_freq_act_hz), 0)                           AS freq_avg_hz,
            ROUND(AVG(bp4_hp_bar), 1)                                  AS hp_avg_bar,
            ROUND(AVG(superheat_k), 1)                                 AS superheat_avg_k,

            -- Heater usage during DHW — should be 0 in mild weather
            ROUND(SUM(CASE WHEN tot_int_add_kw > 0 THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 0)                                        AS heater_duty_pct,
            ROUND(SUM(tot_int_add_kw * 5.0 / 3600), 2)                AS heater_kwh,

            -- Alarm during DHW
            MAX(CASE WHEN alarm_active THEN 1 ELSE 0 END)             AS had_alarm,

            COUNT(*)                                                   AS samples
        FROM with_episode_id
        GROUP BY log_date_parsed, episode_id
    """)
