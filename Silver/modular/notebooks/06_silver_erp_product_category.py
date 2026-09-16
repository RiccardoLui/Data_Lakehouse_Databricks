# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Silver - ERP Product Category
# MAGIC Cleans `bronze_erp_px_cat_g1v2` using the shared config and shared transformation functions.

# COMMAND ----------

# MAGIC %run ../common/silver_config

# COMMAND ----------

# MAGIC %run ../common/silver_utils

# COMMAND ----------

TABLE_KEY = "bronze_erp_px_cat_g1v2"
CONFIG = TABLE_CONFIGS[TABLE_KEY]

df_silver = clean_table(TABLE_KEY, CONFIG)

display(df_silver)

# COMMAND ----------

# Return a simple string when called from the orchestration notebook.
dbutils.notebook.exit(
    f"OK|{TABLE_KEY}|{CONFIG['target']}|rows={df_silver.count()}"
)