"""
NIBE Heat Pump Log — Cycle Detection + ML Layer
=========================================================
Four materialized views:

  gold_nibe_cycles          Per-compressor-cycle diagnostics (starts/stops detected via LAG)
  gold_nibe_cycle_summary   Daily aggregation of cycle stats — wear & stability indicators
  gold_nibe_ml_features     ML-ready feature table joining all gold sources + trend windows
  gold_nibe_anomaly_rules   Rule-based 🟢/🟡/🔴 health scoring from ml_features

Technical notes:
  - PERCENTILE_APPROX replaces PERCENTILE (Spark SQL aggregate syntax)
  - FIRST/LAST value via MIN/MAX(struct(logged_at, col)).col — aggregate-safe ordering
  - LIVE. prefix omitted; SDP resolves unqualified names to the pipeline target catalog/schema
  - GREATEST(..., 1) guards cycles_per_day division from zero when only 1 cycle per day
"""

from pyspark import pipelines as dp

# ── Layer catalog shortcuts ───────────────────────────────────────────────────
_env = spark.conf.get("env_scope")   # "dev" or "prod"
_SL  = f"{_env}_silver.nibe"         # silver layer catalog.schema
_GL  = f"{_env}_gold.nibe"           # gold layer catalog.schema


# ── 1. Compressor Cycles ──────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_cycles",
    comment=(
        "Per-compressor-cycle diagnostics. A cycle = one continuous compressor run from "
        "start (freq 0→>0) to stop (freq >0→0). Minimum cycle length: 6 samples (30 seconds). "
        "Reveals problems invisible in daily averages: short-cycling, ΔT drift during a run, "
        "superheat collapse late in cycle, alarm-terminated runs. "
        "cycle_id is a global monotonic counter across all days."
    ),
    table_properties={"quality": "gold", "agent.purpose": "cycle_analysis"},
    cluster_by=["log_date"],
)
def gold_nibe_cycles():
    return spark.sql(f"""
        WITH with_flags AS (
            SELECT
                *,
                CASE
                    WHEN compr_freq_act_hz > 0
                     AND (LAG(compr_freq_act_hz, 1, 0)
                           OVER (PARTITION BY log_date_parsed ORDER BY logged_at)) = 0
                    THEN 1 ELSE 0
                END AS is_cycle_start
            FROM {_SL}.silver_nibe_logs
            WHERE compr_freq_act_hz IS NOT NULL
        ),
        with_cycle_id AS (
            SELECT
                *,
                SUM(is_cycle_start) OVER (ORDER BY logged_at) AS cycle_id
            FROM with_flags
            WHERE compr_freq_act_hz > 0
        )
        SELECT
            cycle_id,
            MIN(log_date_parsed)                                                 AS log_date,

            -- Timing
            MIN(logged_at)                                                        AS start_time,
            MAX(logged_at)                                                        AS end_time,
            ROUND(TIMESTAMPDIFF(SECOND, MIN(logged_at), MAX(logged_at)) / 60.0, 1)
                                                                                  AS duration_min,

            -- Compressor behaviour
            ROUND(AVG(compr_freq_act_hz), 0)                                     AS freq_avg_hz,
            ROUND(MAX(compr_freq_act_hz), 0)                                     AS freq_max_hz,
            ROUND(STDDEV(compr_freq_act_hz), 1)                                  AS freq_stddev,

            -- Operating mode (most frequent value within cycle)
            MODE(priority_mode)                                                   AS dominant_mode,
            MODE(mode_name)                                                       AS dominant_mode_name,

            -- Flow quality through the cycle
            ROUND(AVG(delta_t_k), 1)                                             AS delta_t_avg_k,
            ROUND(MAX(delta_t_k), 1)                                             AS delta_t_max_k,
            -- ΔT spread: P90 − P10 → does flow worsen as cycle progresses?
            ROUND(
                PERCENTILE_APPROX(delta_t_k, 0.9) - PERCENTILE_APPROX(delta_t_k, 0.1), 1
            )                                                                     AS delta_t_spread_k,

            -- Superheat through the cycle
            ROUND(AVG(superheat_k), 1)                                           AS superheat_avg_k,
            ROUND(MIN(superheat_k), 1)                                           AS superheat_min_k,
            ROUND(SUM(CASE WHEN negative_superheat THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 0)                                                  AS neg_superheat_pct,

            -- Pressures
            ROUND(AVG(bp4_hp_bar), 1)                                            AS hp_avg_bar,
            ROUND(MAX(bp4_hp_bar), 1)                                            AS hp_max_bar,
            ROUND(AVG(bt14_discharge_c), 1)                                      AS discharge_avg_c,
            ROUND(MAX(bt14_discharge_c), 1)                                      AS discharge_max_c,

            -- Temperatures
            ROUND(AVG(bt1_outdoor_temp_c), 1)                                    AS outdoor_avg_c,
            ROUND(AVG(bt25_supply_c), 1)                                         AS supply_avg_c,
            ROUND(AVG(bt71_return_c), 1)                                         AS return_avg_c,

            -- Heater involvement
            ROUND(SUM(CASE WHEN tot_int_add_kw > 0 THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 0)                                                  AS heater_pct,

            -- Alarm during cycle
            MAX(CASE WHEN alarm_active THEN 1 ELSE 0 END)                        AS had_alarm,
            MAX(CASE WHEN alarm_active THEN alarm_number ELSE NULL END)           AS alarm_code,

            -- Degree minutes at start and end — uses struct trick for ordered aggregation
            MIN(struct(logged_at, degree_minutes)).degree_minutes                 AS dm_at_start,
            MAX(struct(logged_at, degree_minutes)).degree_minutes                 AS dm_at_end,

            -- Wind/snow indicator
            ROUND(AVG(bt1_bt28_divergence_k), 1)                                 AS bt1_bt28_avg_divergence_k,

            COUNT(*)                                                              AS samples

        FROM with_cycle_id
        GROUP BY cycle_id
        HAVING COUNT(*) >= 6
    """)


# ── 2. Daily Cycle Summary ────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_cycle_summary",
    comment=(
        "Daily compressor cycle statistics — wear and stability indicators. "
        "Derived from gold_nibe_cycles. "
        "Healthy system: <20 cycles/day, avg cycle >15 min, short_cycle_pct <10%, "
        "avg_freq_volatility <15. High cycles_with_alarm or cycles_with_bad_superheat "
        "indicate specific compressor health concerns."
    ),
    table_properties={"quality": "gold", "agent.purpose": "cycle_summary"},
    cluster_by=["log_date"],
)
def gold_nibe_cycle_summary():
    return spark.sql(f"""
        SELECT
            log_date,

            -- Cycle counts
            COUNT(*)                                                              AS total_cycles,
            -- GREATEST(..., 1) prevents division by zero when all cycles are in the same hour
            ROUND(COUNT(*) * 24.0
                  / GREATEST(TIMESTAMPDIFF(HOUR, MIN(start_time), MAX(end_time)), 1), 1)
                                                                                  AS cycles_per_day,

            -- Duration distribution
            ROUND(AVG(duration_min), 1)                                           AS avg_cycle_min,
            ROUND(MIN(duration_min), 1)                                           AS min_cycle_min,
            ROUND(MAX(duration_min), 1)                                           AS max_cycle_min,
            ROUND(PERCENTILE_APPROX(duration_min, 0.5), 1)                       AS median_cycle_min,

            -- Short cycles (<5 min) = short-cycling / control instability indicator
            SUM(CASE WHEN duration_min < 5 THEN 1 ELSE 0 END)                    AS short_cycles,
            ROUND(SUM(CASE WHEN duration_min < 5 THEN 1 ELSE 0 END) * 100.0
                  / COUNT(*), 0)                                                  AS short_cycle_pct,

            -- Frequency stability (lower = smoother modulation)
            ROUND(AVG(freq_stddev), 1)                                            AS avg_freq_volatility,

            -- Problem cycles
            SUM(had_alarm)                                                        AS cycles_with_alarm,
            SUM(CASE WHEN neg_superheat_pct > 30 THEN 1 ELSE 0 END)              AS cycles_with_bad_superheat,
            SUM(CASE WHEN delta_t_max_k > 7 THEN 1 ELSE 0 END)                   AS cycles_with_high_dt,

            -- ΔT trend within cycles (P90−P10 spread averaged across day)
            ROUND(AVG(delta_t_spread_k), 1)                                       AS avg_dt_spread_k,

            -- Mode distribution
            SUM(CASE WHEN dominant_mode = 30 THEN 1 ELSE 0 END)                  AS heating_cycles,
            SUM(CASE WHEN dominant_mode = 20 THEN 1 ELSE 0 END)                  AS dhw_cycles

        FROM {_GL}.gold_nibe_cycles
        GROUP BY log_date
    """)


# ── 3. ML Feature Table ───────────────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_ml_features",
    comment=(
        "ML-ready feature table for anomaly detection. Each feature was validated against "
        "real diagnostic findings (Jan 12-18 data analysis). "
        "Joins: daily_summary + flow_quality (by freq band) + heating_curve (mild-temp window) "
        "+ cycle_summary + data_quality + self-baseline (weather-normalised residuals). "
        "Includes 7-day rolling averages and day-over-day delta features for trend detection."
    ),
    table_properties={"quality": "gold", "agent.purpose": "ml_features"},
    cluster_by=["log_date"],
)
def gold_nibe_ml_features():
    return spark.sql(f"""
        SELECT
            d.log_date,

            -- ======= FROM DAILY SUMMARY =======
            d.outdoor_avg_c,
            d.outdoor_min_c,
            d.delta_t_avg_k,
            d.dm_avg,
            d.dm_min,
            d.compr_duty_pct,
            d.compr_freq_avg_hz,
            d.heater_duty_pct,
            d.heater_energy_kwh,
            d.neg_superheat_pct,
            d.superheat_avg_k,
            d.hp_avg_bar,
            d.discharge_max_c,
            d.alarm_pct,
            d.alarm_excl_183_pct,
            d.bt1_bt28_max_divergence_k,
            d.supply_vs_calc_offset_k,

            -- ======= FLOW FEATURES =======
            -- ΔT at max compressor load — strongest flow indicator
            f_max.delta_t_avg_k                                                   AS dt_at_max_freq_k,
            f_max.heater_pct                                                       AS heater_at_max_freq_pct,

            -- ΔT gradient across freq bands (max minus low)
            -- Rising ΔT with frequency = flow restriction under load
            COALESCE(f_max.delta_t_avg_k, 0) - COALESCE(f_low.delta_t_avg_k, 0)  AS dt_freq_gradient_k,

            -- ======= HEATER AT MILD TEMPS =======
            -- Heater above -10°C = almost certainly a flow or heating curve problem
            hc_mild.heater_pct                                                     AS heater_pct_above_minus10,

            -- ======= CYCLE FEATURES =======
            c.total_cycles,
            c.cycles_per_day,
            c.avg_cycle_min,
            c.short_cycle_pct,
            c.avg_freq_volatility,
            c.cycles_with_alarm,
            c.cycles_with_bad_superheat,
            c.avg_dt_spread_k,

            -- ======= TREND FEATURES =======
            -- 7-day rolling averages for trend detection
            AVG(d.delta_t_avg_k)       OVER (ORDER BY d.log_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
                                                                                   AS delta_t_7d_avg,
            AVG(d.neg_superheat_pct)   OVER (ORDER BY d.log_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
                                                                                   AS neg_sh_7d_avg,
            AVG(d.heater_energy_kwh)   OVER (ORDER BY d.log_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
                                                                                   AS heater_kwh_7d_avg,

            -- Day-over-day changes
            d.delta_t_avg_k     - LAG(d.delta_t_avg_k)     OVER (ORDER BY d.log_date)
                                                                                   AS delta_t_dod_change,
            d.neg_superheat_pct - LAG(d.neg_superheat_pct) OVER (ORDER BY d.log_date)
                                                                                   AS neg_sh_dod_change,

            -- ======= WEATHER-NORMALISED RESIDUALS =======
            -- Residual: actual ΔT minus historical median for this outdoor temp band
            d.delta_t_avg_k    - baseline.median_dt                               AS dt_residual_k,
            d.heater_duty_pct  - baseline.median_heater                           AS heater_residual_pct,

            -- ======= DATA QUALITY =======
            dq.coverage_pct,
            dq.max_gap_seconds

        FROM {_GL}.gold_nibe_daily_summary d

        -- ΔT at maximum frequency band (>100 Hz)
        LEFT JOIN (
            SELECT log_date, delta_t_avg_k, heater_pct
            FROM {_GL}.gold_nibe_flow_quality
            WHERE freq_band = '04_max_100+Hz'
        ) f_max ON d.log_date = f_max.log_date

        -- ΔT at low frequency band (20-40 Hz)
        LEFT JOIN (
            SELECT log_date, delta_t_avg_k
            FROM {_GL}.gold_nibe_flow_quality
            WHERE freq_band = '01_low_1-39Hz'
        ) f_low ON d.log_date = f_low.log_date

        -- Heater usage at mild outdoor temps (above -10°C) — weighted by samples
        LEFT JOIN (
            SELECT
                log_date,
                ROUND(SUM(CASE WHEN heater_pct > 0 THEN samples ELSE 0 END) * 100.0
                      / SUM(samples), 0)                                           AS heater_pct
            FROM {_GL}.gold_nibe_heating_curve
            WHERE bt1_rounded_c > -10
            GROUP BY log_date
        ) hc_mild ON d.log_date = hc_mild.log_date

        -- Cycle stats
        LEFT JOIN {_GL}.gold_nibe_cycle_summary c ON d.log_date = c.log_date

        -- Data quality flags
        LEFT JOIN {_GL}.gold_nibe_data_quality dq ON d.log_date = dq.log_date

        -- Self-baseline: historical median per outdoor temp bin (2°C resolution)
        LEFT JOIN (
            SELECT
                ROUND(outdoor_avg_c / 2, 0) * 2                                   AS outdoor_bin,
                PERCENTILE_APPROX(delta_t_avg_k, 0.5)                             AS median_dt,
                PERCENTILE_APPROX(heater_duty_pct, 0.5)                           AS median_heater
            FROM {_GL}.gold_nibe_daily_summary
            GROUP BY ROUND(outdoor_avg_c / 2, 0) * 2
        ) baseline ON ROUND(d.outdoor_avg_c / 2, 0) * 2 = baseline.outdoor_bin
    """)


# ── 4. Rule-Based Anomaly Scorer ──────────────────────────────────────────────

@dp.materialized_view(
    name=f"{_GL}.gold_nibe_anomaly_rules",
    comment="Deterministic anomaly detection. Alarm scoring excludes code 183 (defrost = normal).",
    table_properties={"quality": "gold", "agent.purpose": "anomaly_scoring"},
    cluster_by=["log_date"],
)
def gold_nibe_anomaly_rules():
    return spark.sql(f"""
        SELECT
            log_date,

            -- Flow
            CASE WHEN delta_t_avg_k > 7 THEN 'CRITICAL'
                 WHEN delta_t_avg_k > 5 THEN 'WARNING'
                 ELSE 'OK' END AS flow_status,

            -- Superheat
            CASE WHEN neg_superheat_pct > 30 THEN 'CRITICAL'
                 WHEN neg_superheat_pct > 15 THEN 'WARNING'
                 WHEN neg_superheat_pct > 5 THEN 'MONITOR'
                 ELSE 'OK' END AS superheat_status,

            -- Heater
            CASE WHEN heater_duty_pct > 10 AND outdoor_avg_c > -10 THEN 'CRITICAL'
                 WHEN heater_duty_pct > 0 AND outdoor_avg_c > -10 THEN 'WARNING'
                 WHEN heater_duty_pct > 20 THEN 'WARNING'
                 ELSE 'OK' END AS heater_status,

            -- Demand (degree minutes)
            CASE WHEN dm_min < -520 THEN 'CRITICAL'
                 WHEN dm_min < -400 THEN 'WARNING'
                 ELSE 'OK' END AS demand_status,

            -- Alarms — EXCLUDES 183 (defrost is normal operation)
            CASE WHEN alarm_excl_183_pct > 15 THEN 'CRITICAL'
                 WHEN alarm_excl_183_pct > 5 THEN 'WARNING'
                 ELSE 'OK' END AS alarm_status,

            -- Discharge temperature
            CASE WHEN discharge_max_c > 100 THEN 'CRITICAL'
                 WHEN discharge_max_c > 90 THEN 'WARNING'
                 ELSE 'OK' END AS discharge_status,

            -- Wind/snow (BT1 vs BT28 divergence)
            CASE WHEN bt1_bt28_max_divergence_k > 5 THEN 'WARNING'
                 ELSE 'OK' END AS wind_snow_status,

            -- Cycling
            CASE WHEN short_cycle_pct > 20 THEN 'WARNING'
                 ELSE 'OK' END AS cycling_status,

            -- Data quality
            CASE WHEN coverage_pct < 80 THEN 'WARNING'
                 ELSE 'OK' END AS data_quality_status,

            -- Composite score (0 = perfect, higher = worse)
            (CASE WHEN delta_t_avg_k > 7 THEN 30 WHEN delta_t_avg_k > 5 THEN 15 ELSE 0 END
             + CASE WHEN neg_superheat_pct > 30 THEN 25 WHEN neg_superheat_pct > 15 THEN 10 ELSE 0 END
             + CASE WHEN heater_duty_pct > 10 AND outdoor_avg_c > -10 THEN 20
                    WHEN heater_duty_pct > 0 AND outdoor_avg_c > -10 THEN 10 ELSE 0 END
             + CASE WHEN dm_min < -520 THEN 15 WHEN dm_min < -400 THEN 5 ELSE 0 END
             + CASE WHEN alarm_excl_183_pct > 15 THEN 10 WHEN alarm_excl_183_pct > 5 THEN 5 ELSE 0 END
             + CASE WHEN discharge_max_c > 100 THEN 10 ELSE 0 END
             + CASE WHEN bt1_bt28_max_divergence_k > 5 THEN 5 ELSE 0 END
             + CASE WHEN short_cycle_pct > 20 THEN 5 ELSE 0 END
            ) AS anomaly_score,

            -- Overall status
            CASE
                WHEN (delta_t_avg_k > 7 OR neg_superheat_pct > 30 OR dm_min < -520
                      OR (heater_duty_pct > 10 AND outdoor_avg_c > -10) OR discharge_max_c > 100
                      OR alarm_excl_183_pct > 15)
                THEN '🔴 CRITICAL'
                WHEN (delta_t_avg_k > 5 OR neg_superheat_pct > 15 OR dm_min < -400
                      OR alarm_excl_183_pct > 5 OR heater_duty_pct > 20
                      OR discharge_max_c > 90)
                THEN '🟡 WARNING'
                ELSE '🟢 OK'
            END AS overall_status,

            -- Top issue (human-readable)
            CASE
                WHEN delta_t_avg_k > 7 THEN 'Critical flow restriction — ΔT ' || CAST(delta_t_avg_k AS STRING) || 'K at high load'
                WHEN neg_superheat_pct > 30 THEN 'Compressor at risk — negative superheat ' || CAST(neg_superheat_pct AS STRING) || '% of runtime'
                WHEN heater_duty_pct > 10 AND outdoor_avg_c > -10 THEN 'Heater running ' || CAST(heater_duty_pct AS STRING) || '% at ' || CAST(outdoor_avg_c AS STRING) || '°C — possible flow or curve issue'
                WHEN dm_min < -520 THEN 'DM saturated at ' || CAST(dm_min AS STRING) || ' — system cannot meet demand'
                WHEN discharge_max_c > 100 THEN 'High discharge temp ' || CAST(discharge_max_c AS STRING) || '°C — compressor stress'
                WHEN alarm_excl_183_pct > 5 THEN 'Actionable alarms present ' || CAST(alarm_excl_183_pct AS STRING) || '% of time (excl. defrost)'
                WHEN delta_t_avg_k > 5 THEN 'Flow below optimal — ΔT ' || CAST(delta_t_avg_k AS STRING) || 'K'
                WHEN neg_superheat_pct > 15 THEN 'Superheat declining — ' || CAST(neg_superheat_pct AS STRING) || '% negative, monitor trend'
                ELSE 'System operating within normal parameters'
            END AS top_issue

        FROM {_GL}.gold_nibe_ml_features
    """)
