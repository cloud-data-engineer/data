# Databricks notebook source
# MAGIC %md
# MAGIC # Telco CDR Test Pipeline
# MAGIC
# MAGIC Simple test pipeline to validate the multiplex bronze table has data

# COMMAND ----------

import dlt
from pyspark.sql.functions import count, lit

# COMMAND ----------

# Get env_scope for catalog naming
env_scope = spark.conf.get("env_scope")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test: Validate Multiplex Table Has Data

# COMMAND ----------

@dlt.table(
    name=f"{env_scope}_bronze.cdr.test_multiplex_validation",
    comment="Validation test: multiplex table should have records"
)
@dlt.expect_or_fail("has_records", "record_count > 0")
def test_multiplex_validation():
    """Test that the all_cdrs multiplex table contains data"""
    all_cdrs = dlt.read(f"{env_scope}_bronze.cdr.all_cdrs")
    
    return all_cdrs.selectExpr(
        "count(*) as record_count",
        "current_timestamp() as test_timestamp"
    )
