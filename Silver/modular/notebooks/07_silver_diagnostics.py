# Databricks notebook source
# MAGIC %md
# MAGIC # Silver diagnostics — optimized
# MAGIC
# MAGIC Transformation notebooks do not run expensive metrics.
# MAGIC This dedicated task owns:
# MAGIC - Bronze source row count
# MAGIC - Silver row count
# MAGIC - rows removed
# MAGIC - all Silver null counts in one aggregation per table
# MAGIC - exact duplicate rows in the same aggregation
# MAGIC - duplicate business-key groups / extra duplicate rows

# COMMAND ----------

# MAGIC %run ../common/silver_config

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

DIAGNOSTIC_TABLE = (
    f"{CATALOG}.{SILVER_SCHEMA}.silver_diagnostics"
)


def renamed_dedupe_keys(config):
    rename_map = config.get("rename", {})
    keys = (
        config
        .get("dedupe", {})
        .get("keys", [])
    )

    return [
        rename_map.get(key, key)
        for key in keys
    ]


def duplicate_metrics(df, keys):
    if not keys:
        return 0, 0

    grouped = (
        df
        .groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
    )

    metrics = grouped.agg(
        F.count(F.lit(1))
        .cast("long")
        .alias("duplicate_groups"),

        F.coalesce(
            F.sum(
                F.col("count")
                - F.lit(1)
            ),
            F.lit(0),
        )
        .cast("long")
        .alias("duplicate_rows"),
    ).first()

    return (
        int(
            metrics[
                "duplicate_groups"
            ] or 0
        ),
        int(
            metrics[
                "duplicate_rows"
            ] or 0
        ),
    )


rows = []

for source_table, config in TABLE_CONFIGS.items():
    silver_table = config["target"]

    source_full_name = (
        f"{CATALOG}.{BRONZE_SCHEMA}.{source_table}"
    )
    silver_full_name = (
        f"{CATALOG}.{SILVER_SCHEMA}.{silver_table}"
    )

    if not spark.catalog.tableExists(
        source_full_name
    ):
        raise RuntimeError(
            f"Missing Bronze source: {source_full_name}"
        )

    if not spark.catalog.tableExists(
        silver_full_name
    ):
        raise RuntimeError(
            f"Missing Silver table: {silver_full_name}"
        )

    source_df = spark.table(
        source_full_name
    )
    df = spark.table(
        silver_full_name
    )

    columns = df.columns
    column_count = len(columns)

    # Source count is intentionally computed here, not in clean_table().
    source_row_count = (
        source_df
        .agg(
            F.count(F.lit(1))
            .cast("long")
            .alias("row_count")
        )
        .first()["row_count"]
    )

    # One Silver aggregation:
    # row count + all null counts + exact distinct rows.
    expressions = [
        F.count(F.lit(1))
        .cast("long")
        .alias("_row_count")
    ]

    for i, column_name in enumerate(columns):
        expressions.append(
            F.sum(
                F.when(
                    F.col(
                        column_name
                    ).isNull(),
                    F.lit(1),
                ).otherwise(
                    F.lit(0)
                )
            )
            .cast("long")
            .alias(f"_null_{i}")
        )

    if columns:
        expressions.append(
            F.countDistinct(
                F.struct(
                    *[
                        F.col(
                            column_name
                        )
                        for column_name
                        in columns
                    ]
                )
            )
            .cast("long")
            .alias("_distinct_rows")
        )
    else:
        expressions.append(
            F.lit(0)
            .cast("long")
            .alias("_distinct_rows")
        )

    metrics = df.agg(
        *expressions
    ).first()

    row_count = int(
        metrics["_row_count"] or 0
    )
    distinct_rows = int(
        metrics["_distinct_rows"] or 0
    )
    exact_duplicate_rows = (
        row_count
        - distinct_rows
    )

    null_counts = {
        column_name: int(
            metrics[
                f"_null_{i}"
            ] or 0
        )
        for i, column_name
        in enumerate(columns)
    }

    total_null_values = sum(
        null_counts.values()
    )
    columns_with_nulls = sum(
        value > 0
        for value in null_counts.values()
    )

    total_cells = (
        row_count
        * column_count
    )

    completeness = (
        100.0
        if total_cells == 0
        else round(
            100.0
            * (
                total_cells
                - total_null_values
            )
            / total_cells,
            4,
        )
    )

    dedupe_keys = renamed_dedupe_keys(
        config
    )

    duplicate_groups, duplicate_rows = (
        duplicate_metrics(
            df,
            dedupe_keys,
        )
    )

    rows.append({
        "table_name": silver_table,
        "source_table": source_table,
        "source_row_count": int(
            source_row_count
        ),
        "row_count": row_count,
        "rows_removed": (
            int(source_row_count)
            - row_count
        ),
        "column_count": column_count,
        "total_null_values": (
            total_null_values
        ),
        "columns_with_nulls": (
            columns_with_nulls
        ),
        "completeness_percentage": (
            completeness
        ),
        "exact_duplicate_rows": (
            exact_duplicate_rows
        ),
        "duplicate_key_groups": (
            duplicate_groups
        ),
        "duplicate_key_rows": (
            duplicate_rows
        ),
        "dedupe_keys": json.dumps(
            dedupe_keys
        ),
        "null_details": json.dumps(
            {
                column_name: {
                    "null_count": count,
                    "null_percentage": (
                        round(
                            100.0
                            * count
                            / row_count,
                            4,
                        )
                        if row_count
                        else 0.0
                    ),
                }
                for column_name, count
                in null_counts.items()
                if count > 0
            },
            sort_keys=True,
        ),
    })


schema = StructType([
    StructField(
        "table_name",
        StringType(),
        False,
    ),
    StructField(
        "source_table",
        StringType(),
        False,
    ),
    StructField(
        "source_row_count",
        LongType(),
        False,
    ),
    StructField(
        "row_count",
        LongType(),
        False,
    ),
    StructField(
        "rows_removed",
        LongType(),
        False,
    ),
    StructField(
        "column_count",
        LongType(),
        False,
    ),
    StructField(
        "total_null_values",
        LongType(),
        False,
    ),
    StructField(
        "columns_with_nulls",
        LongType(),
        False,
    ),
    StructField(
        "completeness_percentage",
        DoubleType(),
        False,
    ),
    StructField(
        "exact_duplicate_rows",
        LongType(),
        False,
    ),
    StructField(
        "duplicate_key_groups",
        LongType(),
        False,
    ),
    StructField(
        "duplicate_key_rows",
        LongType(),
        False,
    ),
    StructField(
        "dedupe_keys",
        StringType(),
        False,
    ),
    StructField(
        "null_details",
        StringType(),
        False,
    ),
])

diagnostics_df = (
    spark.createDataFrame(
        rows,
        schema=schema,
    )
    .orderBy("table_name")
)

(
    diagnostics_df.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true",
    )
    .saveAsTable(
        DIAGNOSTIC_TABLE
    )
)

print(
    f"Saved Silver diagnostics: "
    f"{DIAGNOSTIC_TABLE} "
    f"({len(rows)} tables)"
)

dbutils.notebook.exit(
    f"OK|{DIAGNOSTIC_TABLE}|"
    f"tables={len(rows)}"
)
