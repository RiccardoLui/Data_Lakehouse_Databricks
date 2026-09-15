# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - CRM Customer
# MAGIC Cleans `bronze_crm_cust_info` using the shared config and shared transformation functions.

# COMMAND ----------

# MAGIC %run ../common/silver_config

# COMMAND ----------

# MAGIC %run ../common/silver_utils

# COMMAND ----------

TABLE_KEY = "bronze_crm_cust_info"
CONFIG = TABLE_CONFIGS[TABLE_KEY]

df_silver = clean_table(TABLE_KEY, CONFIG)

display(df_silver)

# COMMAND ----------

# Return a simple string when called from the orchestration notebook.
dbutils.notebook.exit(
    f"OK|{TABLE_KEY}|{CONFIG['target']}|rows={df_silver.count()}"
)