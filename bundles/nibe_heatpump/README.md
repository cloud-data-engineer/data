# NIBE Heat Pump — Databricks Asset Bundle

An end-to-end data pipeline and AI diagnostic agent for NIBE heat pump log analytics, built on Databricks.

## What It Does

NIBE heat pumps generate a `.LOG` file every day — tab-separated sensor readings at 5-second intervals (~17,000 rows/day). This bundle ingests those files, builds a medallion architecture on top of them, and exposes the data as:

- A **Spark Declarative Pipeline** (Bronze → Silver → Gold)
- **AI/BI dashboards** for daily operational monitoring
- **14 Unity Catalog functions** that serve as tools for an LLM diagnostic agent
- A **Streamlit app** for conversational diagnosis and log inspection
- **SQL alerts** for threshold breaches (ΔT, superheat, heater, data gaps)

## Architecture

```
Unity Catalog Volume (.LOG files)
  └─► bronze_nibe_logs          Streaming table — Auto Loader ingestion, raw rows + metadata
        └─► silver_nibe_logs    Materialized view — 34 scaled columns + 7 computed diagnostics
              ├─► gold_nibe_daily_summary      1 row/day — agent entry point (+ subcooling, standby, sensor drift, short-cycling)
              ├─► gold_nibe_hourly_detail      24 rows/day — intra-day drill-down (+ superheat P10/P50/P90, subcooling)
              ├─► gold_nibe_flow_quality       ΔT by compressor frequency band
              ├─► gold_nibe_heating_curve      Performance per outdoor °C
              ├─► gold_nibe_alarm_episodes     Episode detection via window functions
              ├─► gold_nibe_data_quality       Coverage %, gap metrics, gap distribution
              ├─► gold_nibe_defrost_cycles     Defrost episode detection — air-source #1 issue
              ├─► gold_nibe_dhw_cycles         DHW heating episode detection — tank health
              ├─► gold_nibe_cycles             Per-compressor-cycle diagnostics
              └─► gold_nibe_cycle_summary      Daily cycle aggregation

```

## Prerequisites

- Databricks CLI ≥ 0.281.0 (`brew upgrade databricks`)
- A Unity Catalog with `dev_raw` and `prod_raw` catalogs
- A Unity Catalog Volume at the configured path for uploading `.LOG` files
- A SQL Warehouse (ID configured in `databricks.yml`)

## Deploy

```bash
# Validate
databricks bundle validate --profile dev

# Deploy to dev
databricks bundle deploy --profile dev

# Run the pipeline once
databricks bundle run nibe_heatpump --profile dev

# Deploy to prod
databricks bundle deploy -t prod
```

## Configuration

Variables are defined in `databricks.yml` and overridden per target:

| Variable | Dev | Prod |
|---|---|---|
| `catalog` | `dev_raw` | `prod_raw` |
| `schema` | `nibe` | `nibe` |
| `source_volume_path` | `/Volumes/dev_raw/lz/nibe/raw_logs/` | `/Volumes/prod_raw/nibe_raw/raw_logs/` |
| `warehouse_id` | `48f717bd780befa1` | `48f717bd780befa1` |

Update `notifications` in `resources/nibe_heatpump.pipeline.yml` with your email address.

## Post-Deploy Setup

After deploying, register the Unity Catalog functions so the agent can use them:

1. Run `src/nibe_heatpump/agent/02_register_tools.py` in a Databricks notebook
2. Open the **Generative AI Playground**, add all 14 functions from `dev_gold.nibe` as UC tools
3. Paste the contents of `src/nibe_heatpump/agent/agent_system_prompt.md` as the system prompt

The Streamlit app is deployed separately via `databricks bundle run nibe_diagnostic_app`.

## Bundle Structure

```
bundles/nibe_heatpump/
├── databricks.yml                          # Bundle config: variables, dev/prod targets
├── resources/
│   ├── nibe_heatpump.pipeline.yml          # SDP pipeline (serverless, triggered, daily)
│   ├── nibe_heatpump.alerts.yml            # 5 SQL alerts
│   ├── nibe_heatpump.app.yml               # Streamlit app resource
│   ├── nibe_heatpump.dashboard.yml         # AI/BI Dashboards
└── src/nibe_heatpump/
    ├── transformations/
    │   ├── bronze_nibe_logs.py
    │   ├── silver_nibe_logs.py
    │   ├── gold_nibe_agent.py
    │   └── gold_nibe_diagnostics.py
    ├── agent/
    │   ├── 02_register_tools.py            # Register 14 UC functions
    │   └── agent_system_prompt.md          # LLM system prompt
    └── app/
        └── nibe_diagnostic_app.py          # Streamlit app
```

## Diagnostic Metrics Reference

### Gold table metrics and thresholds

| Metric | Table | Target | WARNING | CRITICAL | What it means |
|---|---|---|---|---|---|
| `delta_t_avg_k` | daily_summary | 3–5 K | >5 K | >7 K | Supply − return temp. High = insufficient floor heating flow |
| `superheat_avg_k` | daily_summary | 3–7 K | <1 K | <0 K | BT17 − BT16. Negative = liquid refrigerant reaching compressor |
| `neg_superheat_pct` | daily_summary | <5% | >15% | >30% | % of compressor-running time with negative superheat |
| `subcooling_avg_k` | daily_summary | 3–8 K | <2 K or >10 K | — | Saturation − BT15. Charge indicator (low=undercharge, high=overcharge/fouling) |
| `heater_duty_pct` | daily_summary | 0% | >0% (>−10°C) | >10% (>−10°C) | Backup heater running = HP can't meet demand alone |
| `dm_min` | daily_summary | >−200 | <−400 | <−520 | Degree-minutes floor. −540 = fully saturated demand controller |
| `discharge_max_c` | daily_summary | <85°C | >90°C | >100°C | BT14 hot-gas after compressor |
| `short_cycle_count` | daily_summary | 0 | >3/day | — | Compressor restarts after <5 min off. Relay stress + refrigerant migration |
| `defrost_cycles` | daily_summary | 5–15 | >15 | >20 | Daily defrost count. Excessive = coil fouling, fan, or charge issue |
| `standby_circ_pump_pct` | daily_summary | 0% | >10% | — | Pump running when idle. High = stuck relay or control logic bug |
| `bt1_vs_avg_divergence_k` | daily_summary | <1 K | >2 K | — | Outdoor sensor instantaneous vs rolling average. High = noisy/drifting sensor |
| `superheat_p10_k` | hourly_detail | >1 K | <0 K | — | 10th percentile catches intermittent slugging hidden by the average |

### UC functions (14 total)

| Function | Purpose | Key parameters |
|---|---|---|
| `get_system_health` | Composite health check — **call first** | `lookback_days` (default 7) |
| `get_daily_summary` | Daily metrics with subcooling, standby, sensor drift | `lookback_days` |
| `get_hourly_detail` | Hourly drill-down with superheat P10/P50/P90, subcooling | `target_date` (YYYY-MM-DD) |
| `get_flow_diagnosis` | ΔT by frequency band | `lookback_days` |
| `get_superheat_status` | Refrigerant health trend | `lookback_days` |
| `get_heater_analysis` | Heater usage by outdoor temp | `lookback_days` |
| `get_heating_curve` | Calculated vs actual supply temp | `lookback_days` |
| `get_alarm_episodes` | Alarm episodes with context | `lookback_days` |
| `get_data_quality` | Coverage, gaps, null sensors | `lookback_days` |
| `get_period_comparison` | Before/after analysis | `period1_start/end`, `period2_start/end` |
| `get_cycle_analysis` | Compressor cycle stats | `lookback_days` |
| `get_worst_cycles` | N worst cycles for a day | `target_date`, `top_n` |
| `get_defrost_analysis` | Per-episode defrost details | `lookback_days` |
| `get_dhw_analysis` | Per-episode DHW cycle details | `lookback_days` |

### Dashboard update notes

Both EN and PL dashboards (`nibe_heat_pump_dashboard.lvdash.json`, `nibe_dashboard_pl.lvdash.json`) query `dev_silver.nibe.silver_nibe_logs` directly. To add the new metrics as dashboard visualizations:

1. **Subcooling trend** — add a dataset querying `dev_gold.nibe.gold_nibe_daily_summary` for `subcooling_avg_k` by date. Add as line chart on the Refrigerant Circuit page.
2. **Defrost summary** — add a dataset from `dev_gold.nibe.gold_nibe_defrost_cycles` aggregated by date (count, avg duration). Add as bar+line on a new Defrost page or the Daily Overview.
3. **DHW cycle health** — add a dataset from `dev_gold.nibe.gold_nibe_dhw_cycles` showing end temps and durations. Add on a new DHW page.

These are best added via the Databricks dashboard editor UI rather than editing the JSON directly.
