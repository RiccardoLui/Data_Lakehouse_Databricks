# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - CRM Product
# MAGIC Optimized task: transform + write only. Diagnostics run separately.

# COMMAND ----------

# MAGIC %run ../common/silver_config

# COMMAND ----------

# MAGIC %run ../common/silver_utils

# COMMAND ----------

TABLE_KEY = "bronze_crm_prd_info"
CONFIG = TABLE_CONFIGS[TABLE_KEY]

clean_table(
    TABLE_KEY,
    CONFIG,
)

dbutils.notebook.exit(
    f"OK|{TABLE_KEY}|{CONFIG['target']}"
)
