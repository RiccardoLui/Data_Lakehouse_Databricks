-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Governance verification queries

-- COMMAND ----------

SELECT *
FROM data_lakehouse_databricks.governance.table_inventory
ORDER BY table_schema, table_name;

-- COMMAND ----------

SELECT *
FROM data_lakehouse_databricks.governance.column_inventory
WHERE comment IS NOT NULL
ORDER BY table_schema, table_name, ordinal_position;

-- COMMAND ----------

SHOW GRANTS ON CATALOG data_lakehouse_databricks;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA data_lakehouse_databricks.gold;

-- COMMAND ----------

DESCRIBE EXTENDED data_lakehouse_databricks.gold.gold_dim_customers;

-- COMMAND ----------

SELECT *
FROM data_lakehouse_databricks.gold.gold_dim_customers_secure
LIMIT 20;
