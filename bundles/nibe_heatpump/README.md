# NIBE Heat Pump — Databricks Asset Bundle

An end-to-end data pipeline and AI diagnostic agent for NIBE heat pump log analytics, built on Databricks.

## What It Does

NIBE heat pumps generate a `.LOG` file every day — tab-separated sensor readings at 5-second intervals (~17,000 rows/day). This bundle ingests those files, builds a medallion architecture on top of them, and exposes the data as:

- A **Spark Declarative Pipeline** (Bronze → Silver → Gold)
- **AI/BI dashboards** for daily operational monitoring
- **12 Unity Catalog functions** that serve as tools for an LLM diagnostic agent
- A **Streamlit app** for conversational diagnosis and log inspection
- **SQL alerts** for threshold breaches (ΔT, superheat, heater, data gaps)

## Architecture

```
Unity Catalog Volume (.LOG files)
  └─► bronze_nibe_logs          Streaming table — Auto Loader ingestion, raw rows + metadata
        └─► silver_nibe_logs    Materialized view — 34 scaled columns + 7 computed diagnostics
              ├─► gold_nibe_daily_summary      1 row/day — agent entry point
              ├─► gold_nibe_hourly_detail      24 rows/day — intra-day drill-down
              ├─► gold_nibe_flow_quality       ΔT by compressor frequency band
              ├─► gold_nibe_heating_curve      Performance per outdoor °C
              ├─► gold_nibe_alarm_episodes     Episode detection via window functions
              ├─► gold_nibe_data_quality       Coverage % and gap metrics
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
2. Open the **Generative AI Playground**, add all 12 functions from `dev_raw.nibe` as UC tools
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
    │   ├── 02_register_tools.py            # Register 12 UC functions
    │   └── agent_system_prompt.md          # LLM system prompt
    └── app/
        └── nibe_diagnostic_app.py          # Streamlit app
```
