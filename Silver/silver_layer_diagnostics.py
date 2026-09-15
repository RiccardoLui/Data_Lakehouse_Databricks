# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Diagnostics
# MAGIC
# MAGIC Builds a diagnostic summary table for every Delta table in the Silver layer.
# MAGIC
# MAGIC Output columns:
# MAGIC - `table_name`
# MAGIC - `row_count`
# MAGIC - `column_count`
# MAGIC - `total_null_values`
# MAGIC - `columns_with_nulls`
# MAGIC - `completeness_percentage`
# MAGIC - `null_details`
# MAGIC
# MAGIC The diagnostic table itself is excluded from the scan.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "data_lakehouse_databricks"
SILVER_SCHEMA = "silver"

DIAGNOSTIC_TABLE_NAME = "silver_diagnostics"
DIAGNOSTIC_TABLE = (
    f"{CATALOG}.{SILVER_SCHEMA}.{DIAGNOSTIC_TABLE_NAME}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover Silver tables automatically

# COMMAND ----------

tables = (
    spark.sql(
        f"SHOW TABLES IN {CATALOG}.{SILVER_SCHEMA}"
    )
    .select("tableName")
    .collect()
)

silver_tables = sorted(
    row.tableName
    for row in tables
    if row.tableName.lower() != DIAGNOSTIC_TABLE_NAME.lower()
)

print(f"Found {len(silver_tables)} Silver tables:")
for table_name in silver_tables:
    print(f"  - {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnostic helper

# COMMAND ----------

def diagnose_table(table_name: str) -> dict:
    """
    Compute table-level completeness/null diagnostics.

    One aggregation is used per table to obtain:
      - row count
      - null count for every column

    null_details is stored as a JSON string, for example:

    {
      "end_date": {
        "null_count": 15,
        "null_percentage": 7.5
      }
    }
    """

    full_table_name = (
        f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
    )

    df = spark.table(full_table_name)
    columns = df.columns
    column_count = len(columns)

    # Handle the unusual case of a table with no columns.
    if column_count == 0:
        return {
            "table_name": table_name,
            "row_count": 0,
            "column_count": 0,
            "total_null_values": 0,
            "columns_with_nulls": 0,
            "completeness_percentage": 100.0,
            "null_details": "{}",
        }

    # Use index-based aliases so arbitrary source column names cannot
    # interfere with the aggregation output names.
    agg_exprs = [
        F.count(F.lit(1)).cast("long").alias("_row_count")
    ]

    for i, column_name in enumerate(columns):
        agg_exprs.append(
            F.sum(
                F.when(
                    F.col(column_name).isNull(),
                    F.lit(1)
                ).otherwise(F.lit(0))
            )
            .cast("long")
            .alias(f"_null_{i}")
        )

    metrics = df.agg(*agg_exprs).first()

    row_count = int(metrics["_row_count"])

    null_counts = {
        column_name: int(metrics[f"_null_{i}"] or 0)
        for i, column_name in enumerate(columns)
    }

    total_null_values = sum(null_counts.values())

    columns_with_nulls = sum(
        1 for count in null_counts.values()
        if count > 0
    )

    total_cells = row_count * column_count

    if total_cells == 0:
        completeness_percentage = 100.0
    else:
        completeness_percentage = round(
            (
                (total_cells - total_null_values)
                / total_cells
            ) * 100.0,
            4
        )

    null_details_dict = {}

    for column_name, null_count in null_counts.items():
        if null_count > 0:
            null_percentage = (
                round(
                    (null_count / row_count) * 100.0,
                    4
                )
                if row_count > 0
                else 0.0
            )

            null_details_dict[column_name] = {
                "null_count": null_count,
                "null_percentage": null_percentage,
            }

    return {
        "table_name": table_name,
        "row_count": row_count,
        "column_count": column_count,
        "total_null_values": total_null_values,
        "columns_with_nulls": columns_with_nulls,
        "completeness_percentage": completeness_percentage,
        "null_details": json.dumps(
            null_details_dict,
            sort_keys=True
        ),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run diagnostics for the Silver layer

# COMMAND ----------

diagnostic_rows = []

for table_name in silver_tables:
    print(f"Diagnosing {table_name} ...")
    diagnostic_rows.append(
        diagnose_table(table_name)
    )

diagnostic_rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the diagnostic DataFrame

# COMMAND ----------

diagnostic_schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("row_count", LongType(), False),
    StructField("column_count", LongType(), False),
    StructField("total_null_values", LongType(), False),
    StructField("columns_with_nulls", LongType(), False),
    StructField("completeness_percentage", DoubleType(), False),
    StructField("null_details", StringType(), False),
])

diagnostics_df = spark.createDataFrame(
    diagnostic_rows,
    schema=diagnostic_schema
)

diagnostics_df = diagnostics_df.orderBy(
    "table_name"
)

display(diagnostics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save as a Delta table

# COMMAND ----------

(
    diagnostics_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIAGNOSTIC_TABLE)
)

print(f"Saved diagnostic table: {DIAGNOSTIC_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the saved diagnostic table

# COMMAND ----------

display(
    spark.table(DIAGNOSTIC_TABLE)
         .orderBy("table_name")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_lakehouse_databricks.silver.silver_diagnostics
# MAGIC ORDER BY table_name;
