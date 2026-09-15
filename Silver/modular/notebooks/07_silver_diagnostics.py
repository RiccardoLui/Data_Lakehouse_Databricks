# Databricks notebook source
# MAGIC %md
# MAGIC # Silver diagnostics
# MAGIC Creates one diagnostic row for each configured Silver business table.

# COMMAND ----------

# MAGIC %run ../common/silver_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import json

DIAGNOSTIC_TABLE_NAME = "silver_diagnostics"
DIAGNOSTIC_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.{DIAGNOSTIC_TABLE_NAME}"

# Only diagnose tables defined in TABLE_CONFIGS. This avoids diagnosing the
# diagnostic table itself or unrelated Silver tables.
expected_tables = [cfg["target"] for cfg in TABLE_CONFIGS.values()]
available_tables = {
    row.tableName
    for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{SILVER_SCHEMA}").collect()
}
silver_tables = [name for name in expected_tables if name in available_tables]


def diagnose_table(table_name: str) -> dict:
    df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}")
    columns = df.columns
    column_count = len(columns)

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

    expressions = [F.count(F.lit(1)).cast("long").alias("_row_count")]
    for i, col_name in enumerate(columns):
        expressions.append(
            F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0))
             .cast("long")
             .alias(f"_null_{i}")
        )

    metrics = df.agg(*expressions).first()
    row_count = int(metrics["_row_count"])
    null_counts = {
        col_name: int(metrics[f"_null_{i}"] or 0)
        for i, col_name in enumerate(columns)
    }

    total_null_values = sum(null_counts.values())
    columns_with_nulls = sum(v > 0 for v in null_counts.values())
    total_cells = row_count * column_count
    completeness = (
        100.0 if total_cells == 0
        else round(100.0 * (total_cells - total_null_values) / total_cells, 4)
    )

    details = {
        col_name: {
            "null_count": count,
            "null_percentage": round(100.0 * count / row_count, 4) if row_count else 0.0,
        }
        for col_name, count in null_counts.items()
        if count > 0
    }

    return {
        "table_name": table_name,
        "row_count": row_count,
        "column_count": column_count,
        "total_null_values": total_null_values,
        "columns_with_nulls": columns_with_nulls,
        "completeness_percentage": completeness,
        "null_details": json.dumps(details, sort_keys=True),
    }


rows = [diagnose_table(name) for name in silver_tables]

schema = StructType([
    StructField("table_name", StringType(), False),
    StructField("row_count", LongType(), False),
    StructField("column_count", LongType(), False),
    StructField("total_null_values", LongType(), False),
    StructField("columns_with_nulls", LongType(), False),
    StructField("completeness_percentage", DoubleType(), False),
    StructField("null_details", StringType(), False),
])

diagnostics_df = spark.createDataFrame(rows, schema=schema).orderBy("table_name")

display(diagnostics_df)

(
    diagnostics_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(DIAGNOSTIC_TABLE)
)

dbutils.notebook.exit(f"OK|{DIAGNOSTIC_TABLE}|tables={len(rows)}")