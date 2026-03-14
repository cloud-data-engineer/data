# CLAUDE.md — Data Engineering Project Context

## Stack

| Layer | Technology |
|---|---|
| Platform | Databricks (Runtime 15.x+, serverless where possible) |
| Pipelines | Spark Declarative Pipelines (SDP) — the new name for DLT |
| Ingestion | Auto Loader (file-based), Kafka structured streaming, Lakeflow Connectors |
| Storage | Delta Lake on Unity Catalog |
| Orchestration | Databricks Asset Bundles (DAB) + Databricks Jobs |
| SQL | Databricks SQL warehouses, UC functions, materialized views |
| Serving | Databricks Model Serving, Databricks Apps (Streamlit) |
| Language | Python (PySpark), SQL (Spark SQL / DBSQL) |

## Workspace

- **Host**: `https://dbc-6019e7fe-ec9b.cloud.databricks.com`
- **CLI profile**: `--profile dev`
- **Default warehouse**: `48f717bd780befa1`

## Unity Catalog Topology

**One catalog per layer, one schema per data product (bundle).** The catalog communicates the layer — no layer prefix needed in table names.

```
dev_raw            ← landing zone (volumes only — no tables)
  └── {domain}/    ← /Volumes/dev_raw/{domain}/raw/        source files
                      /Volumes/dev_raw/{domain}/checkpoints/ pipeline state
                      /Volumes/dev_raw/{domain}/schema/      Auto Loader schema

dev_bronze         ← raw ingestion tables
  └── {domain}     ← schema = data product (nibe, telco, oracle_erp, …)
      └── {entity}            e.g. dev_bronze.nibe.heat_pump_logs

dev_silver         ← cleansed, typed, deduplicated
  └── {domain}
      └── {entity}            e.g. dev_silver.nibe.heat_pump_logs

dev_gold           ← aggregated, business-ready, MVs, UC functions
  └── {domain}
      ├── {entity}_daily      e.g. dev_gold.nibe.heat_pump_daily
      └── fn_{name}           e.g. dev_gold.nibe.fn_get_system_health
```

**Prod mirrors dev** — same schema/table names, different catalog prefix: `prod_bronze`, `prod_silver`, `prod_gold`, `prod_raw`.

### Bundle = Data Product
Each DAB bundle owns one domain's schema across all layer catalogs:
- `bundles/nibe_heatpump/` owns `{env}_bronze.nibe`, `{env}_silver.nibe`, `{env}_gold.nibe`
- Catalog names are derived from `env_scope`: `${var.env_scope}_bronze`, `${var.env_scope}_silver`, `${var.env_scope}_gold`, `${var.env_scope}_raw` — no separate catalog variables needed

### Naming Rules
- Always 3-level namespace: `catalog.schema.table`
- Table name = entity only — **no layer prefix** (the catalog is the layer)
- Schema name = data product / domain — keep it short and stable (it's part of all FQNs)
- Volumes always in `dev_raw.{domain}` — never store raw files in bronze/silver/gold catalogs

## Repo Structure

```
data/
├── CLAUDE.md
├── bundles/
│   └── {bundle_name}/          ← one DAB bundle per domain
│       ├── databricks.yml
│       ├── resources/          ← pipeline/job YAML definitions
│       └── src/
│           └── {domain}/
│               ├── transformations/   ← SDP pipeline notebooks (.py)
│               └── agent/             ← AI agent notebooks (if applicable)
├── src/                        ← shared source / generators / utilities
│   └── {domain}/
├── dlt_telco/                  ← legacy telco DLT (being migrated to bundles)
└── requirements.txt
```

## Pipeline Language

**Default: SQL.** SDP SQL handles Auto Loader (`read_files()`), Kafka (`read_kafka()`), JDBC (`read_jdbc()`), streaming tables, materialized views, and APPLY CHANGES — all without Python. Use SQL unless you need a loop (dynamic table generation) or imperative logic that can't be expressed declaratively. Never default to Python just because the source is Kafka or cloud files.

## Medallion Architecture

### Bronze (`dev_bronze.{domain}.{entity}`)
- **Goal**: Raw ingestion, no business logic, no type casting
- **Format**: Append-only streaming table
- **Columns always present**: `_ingested_at` (current_timestamp), `_source_file` or `_topic`, raw payload
- **Schema**: nullable everywhere — never fail on schema mismatch
- **Table properties**: `quality=bronze`, `pipelines.reset.allowed=false`
- **Checkpoints**: managed automatically by SDP — do not configure manually

### Silver (`dev_silver.{domain}.{entity}`)
- **Goal**: Cleansed, typed, deduplicated, business-key enforced
- **Pattern**: `APPLY CHANGES INTO ... STORED AS SCD TYPE 1/2` (SQL) or streaming table with expectations. Requires a staging streaming table + the APPLY CHANGES statement.
- **Columns always present**: `_updated_at`, `_source_system`, business key(s)
- **Expectations**: NOT NULL on business keys at minimum; valid range/enum checks
- **Partitioning**: by date column if >10M rows expected

### Gold (`dev_gold.{domain}.{entity}`)
- **Goal**: Aggregated, business-ready, query-optimised
- **Preferred type**: Materialized View for aggregations; streaming table for incremental append
- **UC functions**: `dev_gold.{domain}.fn_{name}` — wrap reusable logic, expose to AI agents

## Ingestion Approach Decision Tree

### Source: Database (Oracle, SQL Server, Postgres, etc.)
1. **Lakeflow Connectors** — prefer when:
   - Ongoing CDC / near-real-time sync needed
   - Source has redo logs (Oracle LogMiner, SQL Server CDC) enabled
   - Team doesn't want to manage JDBC partitioning / parallelism
   - Connector license available
2. **JDBC + SDP** — prefer when:
   - Batch/scheduled loads (hourly or daily) are sufficient
   - No CDC license or redo log access
   - Table is small-medium (<100M rows, manageable parallelism via `partitionColumn`)
   - Pattern: `spark.read.jdbc(...)` inside a `@dp.table` or batch Job notebook
3. **File export (Oracle Data Pump / CSV / Parquet dump)** — prefer when:
   - Source system is air-gapped or export is already a business process
   - Initial historical load that runs once
   - Pattern: dump → SFTP/S3 → Auto Loader → bronze

### Source: Kafka / Event Stream
- Always: SDP streaming table with `readStream` from Kafka
- Bronze: raw bytes / JSON string, minimal parse
- Silver: schema parse + `APPLY CHANGES` for keyed entities

### Source: Files (CSV, JSON, Parquet, JSONL, Avro)
- Always: Auto Loader (`cloudFiles`) into bronze streaming table
- `schema_location` in a UC Volume
- `mergeSchema = true` on Auto Loader options

## Pipeline Topology Options

### Option A — Single SDP (all layers in one pipeline)
- Bronze + Silver + Gold in one `databricks.yml` pipeline definition
- Simpler ops, one update to trigger
- **Use when**: domain is small, all layers run at same cadence

### Option B — Separate SDP per layer (recommended for production)
- `{domain}_bronze` pipeline → triggers `{domain}_silver` pipeline → triggers `{domain}_gold`
- Pipelines linked via Delta table reads (each pipeline writes to UC; downstream pipeline reads it directly)
- **Use when**: layers have different cadences (bronze=continuous, gold=hourly), or team owns layers separately
- Trigger: Job with `pipeline_task` steps in sequence, or event-driven via pipeline completion trigger

### Option C — Bronze SDP + downstream batch Jobs
- SDP for streaming bronze only
- Silver/Gold as scheduled Job notebooks (batch Spark)
- **Use when**: silver/gold logic is complex SQL/Python not suited to SDP declarative style

## DAB Conventions

- Bundle directory: `bundles/{domain}/` — one bundle = one data product
- `databricks.yml` **must** include `include: - resources/*.yml`
- Pipeline glob libraries use `include:` not `path:`:
  ```yaml
  libraries:
    - glob:
        include: ../src/{domain}/transformations/**
  ```
- Always `cd` into bundle dir before `databricks bundle run`
- **Standard variables per bundle** (set per target):
  ```yaml
  variables:
    env_scope:    # dev or prod — used to derive catalog names: ${var.env_scope}_bronze, _silver, _gold, _raw
    domain:       # schema name = data product name (e.g. nibe, telco)
    development:  # true in dev, false in prod
  ```
  Catalog names in pipeline YAML: `${var.env_scope}_bronze`, `${var.env_scope}_silver`, `${var.env_scope}_gold`, `${var.env_scope}_raw`
  Volume paths: `/Volumes/${var.env_scope}_raw/${var.domain}/raw/` (Auto Loader sources), `/Volumes/${var.env_scope}_raw/${var.domain}/schema/` (schema inference)
- Targets: `dev` (mode: development, default: true) and `prod` (mode: production)

## Coding Conventions

### Python / PySpark (SDP)
- **SDP Python API**: `from pyspark import pipelines as dp`
  - `@dp.table` — streaming table (append-only, incremental)
  - `@dp.materialized_view` — recomputed aggregate / enrichment
  - `dp.create_streaming_table` + `dp.create_auto_cdc_flow` — CDC / APPLY CHANGES pattern (Python only; prefer SQL `APPLY CHANGES INTO` instead)
  - Table name inferred from function name by default
  - Return a DataFrame — never call `.write` or `.writeStream` inside a pipeline function
  - SDP manages checkpoints, retries, and state — do not add checkpoint options
- All columns nullable in bronze; enforce NOT NULL in silver via SDP expectations
- Use `from pyspark.sql import functions as F` — never import individual functions at top level
- Secrets via `dbutils.secrets.get(scope=env_scope, key="...")` — never hardcode credentials
- `spark.conf.get("pipeline_var")` to read SDP pipeline config vars inside notebooks
- JSONL serialisation: use `F.to_json(F.struct(*cols))` not manual string formatting
- No bare `except:` — always catch specific exceptions

### SQL (UC / DBSQL)
- Always 3-level namespace in DDL: `CREATE TABLE catalog.schema.table`
- UC functions: `CREATE OR REPLACE FUNCTION catalog.schema.fn_name(...)`
- `PERCENTILE_APPROX()` not `PERCENTILE()` in Spark SQL
- `ROW_NUMBER() OVER (...)` instead of `LIMIT` inside UC function bodies
- `ORDER BY` inside `COLLECT_LIST`: use subquery `FROM (SELECT * FROM t ORDER BY col)`
- GRANTs after every `CREATE`: `GRANT SELECT ON TABLE ... TO ...`

## Streaming Triggers (Python writeStream only — SDP handles this internally)

- **`availableNow=True`** — process all available data then stop; preferred for scheduled batch-style streaming jobs. Replaces deprecated `once=True`.
- **Continuous** — omit trigger for low-latency continuous processing
- Never use `once=True` — it is deprecated

## Delta Streaming Source Rate Limits (Python writeStream only)

When reading from a Delta table as a streaming source, set these options to avoid overwhelming the pipeline on initial load or large backfills:

```python
.option("maxFilesPerTrigger", "10")      # files per micro-batch (default: 1000)
.option("maxBytesPerTrigger", "1g")      # bytes per micro-batch cap
.option("withEventTimeOrder", "true")    # process files in event-time order
.option("ignoreChanges", "true")         # don't fail on updates/merges to source
.option("ignoreDeletes", "true")         # don't fail on deletes to source
```

## PySpark Testing

- Use `@pytest.fixture(scope="session")` for SparkSession — one session shared across all tests
- `get_session(local=True)` DI pattern: `local=True` starts embedded Spark with Delta/Kafka jars; `local=False` returns `SparkSession.builder.getOrCreate()` (Databricks). Same code path in tests and production.
- **Test transforms as pure functions** — streaming apps are essentially lambda operations: input DataFrame (fixed schema) → transform → assert output schema/count/values. Don't test the source/sink plumbing.
- Run tests locally with embedded Spark before deploying to Databricks

## Common Gotchas

- **SDP table references**: in Python use `spark.readStream.table("table_name")` or `spark.read.table("table_name")`; in SQL just use the plain table name — no `LIVE.` prefix (old DLT syntax, not valid in SDP) and no `dlt.read()` (old DLT Python API)
- `APPLY CHANGES INTO` requires a sequence key — use a timestamp or monotonic ID, not row hash
- Auto Loader with JSON: set `cloudFiles.inferColumnTypes = false` at bronze, enforce types at silver
- `mergeSchema` must be set on the **pipeline** level (not just the write), or new columns silently drop
- `mergeSchema: true` on the writeStream sink is also required — omitting it will break the write when new fields arrive
- Lakeflow Connector pipelines cannot share a pipeline with SDP user-defined tables
- MV refresh is triggered by the pipeline update — not by upstream table commits directly
- **Kafka SASL on Serverless**: use `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule` in `kafka.sasl.jaas.config` — the unshaded `org.apache.kafka...` path raises `No LoginModule found` because Serverless uses the shaded Kafka client bundled with Spark's connector
