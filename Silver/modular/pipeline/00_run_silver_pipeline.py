# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Run Silver pipeline sequentially
# MAGIC Runs each table-specific notebook in the order configured in `silver_config`.
# MAGIC
# MAGIC For a production-style setup, the same notebooks can instead be separate
# MAGIC Lakeflow Job tasks with dependencies.

# COMMAND ----------

# MAGIC %run ../common/silver_config

# COMMAND ----------

from datetime import datetime

# Choose whether the orchestrator should immediately stop on a failed child task.
dbutils.widgets.dropdown(
    "stop_on_error",
    "true",
    ["true", "false"],
    "Stop on first error"
)
STOP_ON_ERROR = dbutils.widgets.get("stop_on_error").lower() == "true"

# 0 = no explicit timeout.
TIMEOUT_SECONDS = 0
results = []

for notebook_path in PIPELINE_NOTEBOOKS:
    started = datetime.now()
    print("\n" + "=" * 100)
    print(f"RUNNING: {notebook_path}")
    print("=" * 100)

    try:
        result = dbutils.notebook.run(
            notebook_path,
            TIMEOUT_SECONDS,
            {}
        )
        results.append(("OK", notebook_path, result))
        print(f"SUCCESS: {result}")

    except Exception as exc:
        results.append(("FAILED", notebook_path, str(exc)))
        print(f"FAILED: {notebook_path}")
        print(str(exc))

        if STOP_ON_ERROR:
            raise

print("\n" + "=" * 100)
print("SILVER PIPELINE SUMMARY")
print("=" * 100)
for status, notebook_path, message in results:
    print(f"{status:>6}  {notebook_path}  ->  {message}")