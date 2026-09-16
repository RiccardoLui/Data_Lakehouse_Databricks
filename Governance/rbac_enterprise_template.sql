-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Enterprise RBAC template
-- MAGIC
-- MAGIC DO NOT run this unchanged in Free Edition.
-- MAGIC Custom account-level groups/service principals require enterprise account administration.
-- MAGIC Create the principals first, then uncomment the grants you need.

-- COMMAND ----------

-- Recommended principals for a real workspace:
--   data_engineers  : builds Bronze/Silver/Gold
--   data_analysts   : read-only access to governed Gold products
--   data_stewards   : metadata/governance inspection
--   ci_cd_sp        : deployment identity used by CI/CD

-- Baseline catalog access
-- GRANT USE CATALOG ON CATALOG data_lakehouse_databricks TO `data_engineers`;
-- GRANT USE CATALOG ON CATALOG data_lakehouse_databricks TO `data_analysts`;
-- GRANT USE CATALOG ON CATALOG data_lakehouse_databricks TO `data_stewards`;

-- Data engineers: write medallion layers and landing/checkpoint volumes.
-- GRANT USE SCHEMA, CREATE TABLE ON SCHEMA data_lakehouse_databricks.bronze TO `data_engineers`;
-- GRANT USE SCHEMA, CREATE TABLE ON SCHEMA data_lakehouse_databricks.silver TO `data_engineers`;
-- GRANT USE SCHEMA, CREATE TABLE ON SCHEMA data_lakehouse_databricks.gold TO `data_engineers`;
-- GRANT SELECT, MODIFY ON SCHEMA data_lakehouse_databricks.bronze TO `data_engineers`;
-- GRANT SELECT, MODIFY ON SCHEMA data_lakehouse_databricks.silver TO `data_engineers`;
-- GRANT SELECT, MODIFY ON SCHEMA data_lakehouse_databricks.gold TO `data_engineers`;
-- GRANT READ VOLUME, WRITE VOLUME ON SCHEMA data_lakehouse_databricks.bronze TO `data_engineers`;

-- Analysts: only curated Gold data. Prefer granting access to secure views instead of raw PII tables.
-- GRANT USE SCHEMA ON SCHEMA data_lakehouse_databricks.gold TO `data_analysts`;
-- GRANT SELECT ON VIEW data_lakehouse_databricks.gold.gold_dim_customers_secure TO `data_analysts`;
-- GRANT SELECT ON TABLE data_lakehouse_databricks.gold.gold_dim_product TO `data_analysts`;
-- GRANT SELECT ON TABLE data_lakehouse_databricks.gold.gold_fact_sales TO `data_analysts`;

-- Stewards: discover metadata without modifying business data.
-- GRANT USE SCHEMA ON SCHEMA data_lakehouse_databricks.governance TO `data_stewards`;
-- GRANT SELECT ON SCHEMA data_lakehouse_databricks.governance TO `data_stewards`;
-- GRANT READ METADATA ON CATALOG data_lakehouse_databricks TO `data_stewards`;

-- CI/CD service principal: give only the workspace/job permissions and UC grants needed by deployment/run.
-- Avoid ALL PRIVILEGES unless it is genuinely required.