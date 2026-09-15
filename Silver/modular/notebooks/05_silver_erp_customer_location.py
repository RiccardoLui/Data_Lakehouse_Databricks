# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - ERP Customer Location
# MAGIC Cleans `bronze_erp_loc_a101` using the shared config and shared transformation functions.

# COMMAND ----------

# MAGIC %run ../common/silver_config

# COMMAND ----------

# MAGIC %run ../common/silver_utils

# COMMAND ----------

TABLE_KEY = "bronze_erp_loc_a101"
CONFIG = TABLE_CONFIGS[TABLE_KEY]

df_silver = clean_table(TABLE_KEY, CONFIG)

display(df_silver)

# COMMAND ----------

# Return a simple string when called from the orchestration notebook.
dbutils.notebook.exit(
    f"OK|{TABLE_KEY}|{CONFIG['target']}|rows={df_silver.count()}"
)
