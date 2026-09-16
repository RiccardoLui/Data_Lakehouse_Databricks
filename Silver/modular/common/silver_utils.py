# Databricks notebook source
# MAGIC %md
# MAGIC # Shared Silver cleaning utilities
# MAGIC Transformation-only utilities. Expensive quality metrics are handled by `07_silver_diagnostics.py`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType


def _quoted(column_name: str) -> str:
    return f"`{column_name.replace('`', '``')}`"


def trim_and_nullify_strings(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            name = field.name
            trimmed = F.trim(F.col(name))
            df = df.withColumn(
                name,
                F.when(
                    F.length(trimmed) == 0,
                    F.lit(None),
                ).otherwise(trimmed),
            )
    return df


def safe_cast(df, column_name: str, dtype: str):
    return df.withColumn(
        column_name,
        F.expr(
            f"try_cast({_quoted(column_name)} as {dtype})"
        ),
    )


def safe_to_date(df, column_name: str, fmt=None):
    q = _quoted(column_name)

    if fmt:
        expr = (
            f"try_to_date(cast({q} as string), '{fmt}')"
        )
    else:
        expr = f"try_to_date(cast({q} as string))"

    return df.withColumn(
        column_name,
        F.expr(expr),
    )


def apply_regex_rules(df, rules):
    for column_name, replacements in rules.items():
        if column_name not in df.columns:
            raise ValueError(
                f"Regex rule references missing column: {column_name}"
            )

        for pattern, replacement in replacements:
            df = df.withColumn(
                column_name,
                F.regexp_replace(
                    F.col(column_name),
                    pattern,
                    replacement,
                ),
            )

    return df


def apply_value_maps(df, value_maps):
    for column_name, mapping in value_maps.items():
        if column_name not in df.columns:
            raise ValueError(
                f"Value map references missing column: {column_name}"
            )

        normalized = F.lower(
            F.trim(F.col(column_name))
        )
        result = F.col(column_name)

        for source, target in mapping.items():
            result = (
                F.when(
                    normalized
                    == F.lit(
                        str(source).strip().lower()
                    ),
                    F.lit(target),
                )
                .otherwise(result)
            )

        df = df.withColumn(
            column_name,
            result,
        )

    return df


def apply_special_transform(df, transform_name):
    if transform_name == "split_product_key":
        if "prd_key" not in df.columns:
            raise ValueError(
                "split_product_key requires column prd_key"
            )

        original_key = F.trim(
            F.col("prd_key")
        )

        prefix = F.regexp_extract(
            original_key,
            r"^([A-Za-z0-9]{2}-[A-Za-z0-9]{2})-",
            1,
        )

        df = df.withColumn(
            "category_id",
            F.when(
                F.length(prefix) > 0,
                F.upper(
                    F.regexp_replace(
                        prefix,
                        "-",
                        "_",
                    )
                ),
            ).otherwise(F.lit(None)),
        )

        df = df.withColumn(
            "prd_key",
            F.regexp_replace(
                original_key,
                r"^[A-Za-z0-9]{2}-[A-Za-z0-9]{2}-",
                "",
            ),
        )

        return df

    raise ValueError(
        f"Unknown special transform: {transform_name}"
    )


def apply_post_transform(df, transform_name):
    if transform_name == "derive_product_end_date":
        required = {
            "prd_key",
            "prd_start_dt",
        }
        missing = (
            required
            - set(df.columns)
        )

        if missing:
            raise ValueError(
                "derive_product_end_date missing columns: "
                f"{sorted(missing)}"
            )

        window = (
            Window
            .partitionBy("prd_key")
            .orderBy(
                F.col(
                    "prd_start_dt"
                ).asc_nulls_last()
            )
        )

        next_start = F.lead(
            "prd_start_dt"
        ).over(window)

        return df.withColumn(
            "prd_end_dt",
            F.when(
                next_start.isNotNull(),
                F.date_sub(
                    next_start,
                    1,
                ),
            ).otherwise(
                F.lit(None).cast("date")
            ),
        )

    if transform_name == "validate_birth_date":
        if "BDATE" not in df.columns:
            raise ValueError(
                "validate_birth_date requires BDATE"
            )

        return df.withColumn(
            "BDATE",
            F.when(
                F.col("BDATE")
                > F.current_date(),
                F.lit(None).cast("date"),
            ).otherwise(
                F.col("BDATE")
            ),
        )

    raise ValueError(
        f"Unknown post transform: {transform_name}"
    )


def deduplicate(df, dedupe_config):
    if not dedupe_config:
        return df

    keys = dedupe_config.get(
        "keys",
        [],
    )

    if not keys:
        return df

    missing = [
        c
        for c in keys
        if c not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Dedupe keys missing from DataFrame: {missing}"
        )

    order_by = dedupe_config.get(
        "order_by",
        [],
    )

    if not order_by:
        return df.dropDuplicates(keys)

    order_exprs = []

    for column_name, direction in order_by:
        if column_name not in df.columns:
            raise ValueError(
                "Dedupe order column missing from DataFrame: "
                f"{column_name}"
            )

        if str(direction).lower().startswith("desc"):
            order_exprs.append(
                F.col(
                    column_name
                ).desc_nulls_last()
            )
        else:
            order_exprs.append(
                F.col(
                    column_name
                ).asc_nulls_last()
            )

    window = (
        Window
        .partitionBy(*keys)
        .orderBy(*order_exprs)
    )

    return (
        df
        .withColumn(
            "_silver_row_number",
            F.row_number().over(window),
        )
        .filter(
            F.col(
                "_silver_row_number"
            ) == 1
        )
        .drop("_silver_row_number")
    )


def enforce_required(df, required_columns):
    if not required_columns:
        return df

    missing = [
        column_name
        for column_name in required_columns
        if column_name not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Required columns missing: {missing}"
        )

    condition = F.lit(True)

    for column_name in required_columns:
        condition = (
            condition
            & F.col(
                column_name
            ).isNotNull()
        )

    return df.filter(condition)


def rename_columns(df, rename_map):
    for old_name, new_name in rename_map.items():
        if old_name not in df.columns:
            raise ValueError(
                "Rename rule references missing column: "
                f"{old_name}"
            )

        df = df.withColumnRenamed(
            old_name,
            new_name,
        )

    return df


# COMMAND ----------

def clean_table(source_table_name, config):
    """
    Bronze -> Silver transformation.

    No diagnostic count() actions are executed here.
    The Delta write is the principal action.
    """
    source = (
        f"{CATALOG}.{BRONZE_SCHEMA}.{source_table_name}"
    )
    target = (
        f"{CATALOG}.{SILVER_SCHEMA}.{config['target']}"
    )

    print("\n" + "=" * 90)
    print("BRONZE -> SILVER")
    print(f"Source: {source}")
    print(f"Target: {target}")
    print("=" * 90)

    df = spark.table(source)

    df = trim_and_nullify_strings(df)

    df = apply_regex_rules(
        df,
        config.get(
            "regex_replace",
            {},
        ),
    )

    if config.get("special_transform"):
        df = apply_special_transform(
            df,
            config["special_transform"],
        )

    df = apply_value_maps(
        df,
        config.get(
            "value_maps",
            {},
        ),
    )

    for column_name, fmt in config.get(
        "dates",
        {},
    ).items():
        if column_name not in df.columns:
            raise ValueError(
                "Date configuration references missing "
                f"column: {column_name}"
            )

        df = safe_to_date(
            df,
            column_name,
            fmt,
        )

    for column_name, dtype in config.get(
        "casts",
        {},
    ).items():
        if column_name not in df.columns:
            raise ValueError(
                "Cast configuration references missing "
                f"column: {column_name}"
            )

        df = safe_cast(
            df,
            column_name,
            dtype,
        )

    if config.get("post_transform"):
        df = apply_post_transform(
            df,
            config["post_transform"],
        )

    fillna_config = config.get(
        "fillna",
        {},
    )

    if fillna_config:
        existing_fill = {
            key: value
            for key, value
            in fillna_config.items()
            if key in df.columns
        }

        if existing_fill:
            df = df.fillna(
                existing_fill
            )

    df = deduplicate(
        df,
        config.get("dedupe"),
    )

    df = enforce_required(
        df,
        config.get(
            "required",
            [],
        ),
    )

    df = rename_columns(
        df,
        config.get(
            "rename",
            {},
        ),
    )

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS "
        f"{CATALOG}.{SILVER_SCHEMA}"
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .saveAsTable(target)
    )

    print(f"Saved: {target}")

    return {
        "source": source,
        "target": target,
    }
