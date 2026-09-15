# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze -> Silver cleaning pipeline
# MAGIC
# MAGIC Config-driven cleaning for the CRM and ERP Bronze tables used in the Bike Data Lakehouse.
# MAGIC
# MAGIC Main design choices:
# MAGIC - preserves Bronze source tables unchanged
# MAGIC - keeps dates as Spark `DATE` values (not formatted strings)
# MAGIC - preserves meaningful null dates (especially current product `end_date`)
# MAGIC - keeps business keys as strings so leading zeros are not lost
# MAGIC - normalizes CRM/ERP customer keys to the same representation
# MAGIC - splits the CRM product key into `category_id` + `product_key`
# MAGIC - automatically discovers configured Bronze tables
# MAGIC - writes Delta tables to the Silver schema
# MAGIC - prints basic data-quality and relationship checks

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType

# -----------------------------
# Runtime settings
# -----------------------------

CATALOG = "data_lakehouse_databricks"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# If True, every configured Bronze table that exists will be processed.
RUN_ALL_CONFIGURED_TABLES = True

# Optional: set to one or more source table names to run only selected tables.
# Example:
# ONLY_TABLES = ["bronze_crm_cust_info", "bronze_erp_cust_az12"]
ONLY_TABLES = []

RUN_RELATIONSHIP_CHECKS = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table configuration
# MAGIC
# MAGIC Generic cleaning is handled by reusable functions below.
# MAGIC Business-specific rules stay explicit in this configuration.

# COMMAND ----------

TABLE_CONFIGS = {

    # =========================================================
    # CRM
    # =========================================================

    "bronze_crm_cust_info": {
        "target": "silver_customer_info",

        # Keep the most recent row for duplicate customer IDs.
        "dedupe": {
            "keys": ["cst_id"],
            "order_by": [("cst_create_date", "desc")]
        },

        # Keep customer_key as STRING.
        # AW00011000 -> 00011000
        # AW-00011000 -> 00011000
        "regex_replace": {
            "cst_key": [
                (r"(?i)^AW-?", "")
            ]
        },

        "dates": {
            "cst_create_date": None
        },

        "casts": {
            "cst_id": "int"
        },

        "value_maps": {
            "cst_gndr": {
                "M": "Male",
                "F": "Female",
                "Male": "Male",
                "Female": "Female"
            },
            "cst_marital_status": {
                "M": "Married",
                "S": "Single",
                "Married": "Married",
                "Single": "Single"
            }
        },

        "fillna": {
            "cst_gndr": "missing",
            "cst_marital_status": "missing"
        },

        "required": [
            "cst_id",
            "cst_key"
        ],

        "rename": {
            "cst_id": "customer_id",
            "cst_key": "customer_key",
            "cst_firstname": "firstname",
            "cst_lastname": "lastname",
            "cst_marital_status": "marital_status",
            # Fixes the cst_gndr/cst_gender typo in the old notebook.
            "cst_gndr": "gender",
            "cst_create_date": "creation_date"
        }
    },


    "bronze_crm_prd_info": {
        "target": "silver_product_info",

        "dedupe": {
            "keys": ["prd_id"]
        },

        # AC-HE-HL-U509-R ->
        # category_id = AC_HE
        # prd_key     = HL-U509-R
        "special_transform": "split_product_key",

        "dates": {
            "prd_start_dt": None,
            "prd_end_dt": None
        },

        "casts": {
            "prd_id": "int",
            "prd_cost": "int"
        },

        "fillna": {
            "prd_line": "missing"
        },

        "required": [
            "prd_id",
            "prd_key"
        ],

        # Rebuild end dates from the next version's start date.
        # The latest/current version remains NULL.
        "post_transform": "derive_product_end_date",

        "rename": {
            "prd_id": "product_id",
            "prd_key": "product_key",
            "prd_nm": "product_name",
            "prd_cost": "cost",
            "prd_line": "product_line",
            "prd_start_dt": "start_date",
            "prd_end_dt": "end_date"
        }
    },


    "bronze_crm_sales_details": {
        "target": "silver_sales_details",

        # An order can have multiple products, so order number alone
        # is not a safe deduplication key.
        "dedupe": {
            "keys": ["sls_ord_num", "sls_prd_key"]
        },

        "dates": {
            "sls_order_dt": "yyyyMMdd",
            "sls_ship_dt": "yyyyMMdd",
            "sls_due_dt": "yyyyMMdd"
        },

        "casts": {
            "sls_cust_id": "int",
            "sls_sales": "int",
            "sls_quantity": "int",
            "sls_price": "int"
        },

        "required": [
            "sls_ord_num",
            "sls_prd_key",
            "sls_cust_id",
            "sls_sales",
            "sls_quantity"
        ],

        "rename": {
            "sls_ord_num": "order_number",
            "sls_prd_key": "product_key",
            "sls_cust_id": "customer_id",
            "sls_order_dt": "order_date",
            "sls_ship_dt": "ship_date",
            "sls_due_dt": "due_date",
            "sls_sales": "sales_amount",
            "sls_quantity": "quantity",
            "sls_price": "price"
        }
    },


    # =========================================================
    # ERP
    # =========================================================

    "bronze_erp_cust_az12": {
        "target": "silver_customer_demographics",

        "dedupe": {
            "keys": ["CID"]
        },

        # NASAW00011000 -> 00011000
        # AW00011000    -> 00011000
        "regex_replace": {
            "CID": [
                (r"(?i)^(NAS)?AW-?", "")
            ]
        },

        "dates": {
            "BDATE": None
        },

        "value_maps": {
            "GEN": {
                "M": "Male",
                "F": "Female",
                "Male": "Male",
                "Female": "Female"
            }
        },

        "fillna": {
            "GEN": "missing"
        },

        "required": ["CID"],

        # Future birth dates are invalid and are set to NULL.
        "post_transform": "validate_birth_date",

        "rename": {
            "CID": "customer_key",
            "BDATE": "birth_date",
            "GEN": "gender"
        }
    },


    "bronze_erp_loc_a101": {
        "target": "silver_customer_location",

        "dedupe": {
            "keys": ["CID"]
        },

        # AW-00011000 -> 00011000
        "regex_replace": {
            "CID": [
                (r"(?i)^(NAS)?AW-?", "")
            ]
        },

        "value_maps": {
            "CNTRY": {
                "DE": "Germany",
                "US": "United States",
                "USA": "United States"
            }
        },

        "fillna": {
            "CNTRY": "missing"
        },

        "required": ["CID"],

        "rename": {
            "CID": "customer_key",
            "CNTRY": "country"
        }
    },


    "bronze_erp_px_cat_g1v2": {
        "target": "silver_product_category",

        "dedupe": {
            "keys": ["ID"]
        },

        "required": ["ID"],

        "rename": {
            "ID": "category_id",
            "CAT": "category",
            "SUBCAT": "subcategory",
            "MAINTENANCE": "maintenance"
        }
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generic cleaning helpers

# COMMAND ----------

def _quoted(column_name: str) -> str:
    """Return a safely backtick-quoted Spark SQL identifier."""
    return f"`{column_name.replace('`', '``')}`"


def trim_and_nullify_strings(df):
    """
    Trim every STRING column and turn empty strings into NULL.
    This is safe, generic cleaning that does not require business semantics.
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            name = field.name
            trimmed = F.trim(F.col(name))
            df = df.withColumn(
                name,
                F.when(F.length(trimmed) == 0, F.lit(None)).otherwise(trimmed)
            )
    return df


def safe_cast(df, column_name: str, dtype: str):
    """
    Databricks/Spark SQL try_cast:
    malformed values become NULL instead of crashing the pipeline.
    """
    return df.withColumn(
        column_name,
        F.expr(f"try_cast({_quoted(column_name)} as {dtype})")
    )


def safe_to_date(df, column_name: str, fmt=None):
    """
    Convert a column to Spark DATE.
    Invalid values become NULL.

    fmt=None:
        2025-10-06 -> DATE 2025-10-06

    fmt='yyyyMMdd':
        20110103 -> DATE 2011-01-03
    """
    q = _quoted(column_name)

    if fmt:
        expr = f"try_to_date(cast({q} as string), '{fmt}')"
    else:
        expr = f"try_to_date(cast({q} as string))"

    return df.withColumn(column_name, F.expr(expr))


def apply_regex_rules(df, rules):
    for column_name, replacements in rules.items():
        if column_name not in df.columns:
            raise ValueError(f"Regex rule references missing column: {column_name}")

        for pattern, replacement in replacements:
            df = df.withColumn(
                column_name,
                F.regexp_replace(F.col(column_name), pattern, replacement)
            )
    return df


def apply_value_maps(df, value_maps):
    """
    Case-insensitive exact-value normalization.
    Unmapped values are preserved.
    """
    for column_name, mapping in value_maps.items():
        if column_name not in df.columns:
            raise ValueError(f"Value map references missing column: {column_name}")

        normalized = F.lower(F.trim(F.col(column_name)))
        result = F.col(column_name)

        for source, target in mapping.items():
            result = F.when(
                normalized == F.lit(str(source).strip().lower()),
                F.lit(target)
            ).otherwise(result)

        df = df.withColumn(column_name, result)

    return df


def apply_special_transform(df, transform_name):
    if transform_name == "split_product_key":
        if "prd_key" not in df.columns:
            raise ValueError("split_product_key requires column prd_key")

        original_key = F.trim(F.col("prd_key"))

        # Capture exactly the two leading code groups, e.g. AC-HE.
        prefix = F.regexp_extract(
            original_key,
            r"^([A-Za-z0-9]{2}-[A-Za-z0-9]{2})-",
            1
        )

        df = df.withColumn(
            "category_id",
            F.when(
                F.length(prefix) > 0,
                F.upper(F.regexp_replace(prefix, "-", "_"))
            ).otherwise(F.lit(None))
        )

        df = df.withColumn(
            "prd_key",
            F.regexp_replace(
                original_key,
                r"^[A-Za-z0-9]{2}-[A-Za-z0-9]{2}-",
                ""
            )
        )

        return df

    raise ValueError(f"Unknown special transform: {transform_name}")


def apply_post_transform(df, transform_name):
    if transform_name == "derive_product_end_date":
        required = {"prd_key", "prd_start_dt"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(
                f"derive_product_end_date missing columns: {sorted(missing)}"
            )

        window = Window.partitionBy("prd_key").orderBy(
            F.col("prd_start_dt").asc_nulls_last()
        )

        next_start = F.lead("prd_start_dt").over(window)

        # Ignore the unreliable raw end date and derive a coherent SCD-like
        # validity interval. Current/latest row stays NULL.
        df = df.withColumn(
            "prd_end_dt",
            F.when(
                next_start.isNotNull(),
                F.date_sub(next_start, 1)
            ).otherwise(F.lit(None).cast("date"))
        )
        return df

    if transform_name == "validate_birth_date":
        if "BDATE" not in df.columns:
            raise ValueError("validate_birth_date requires BDATE")

        df = df.withColumn(
            "BDATE",
            F.when(
                F.col("BDATE") > F.current_date(),
                F.lit(None).cast("date")
            ).otherwise(F.col("BDATE"))
        )
        return df

    raise ValueError(f"Unknown post transform: {transform_name}")


def deduplicate(df, dedupe_config):
    if not dedupe_config:
        return df

    keys = dedupe_config.get("keys", [])
    if not keys:
        return df

    missing = [c for c in keys if c not in df.columns]
    if missing:
        raise ValueError(f"Dedupe keys missing from DataFrame: {missing}")

    order_by = dedupe_config.get("order_by", [])

    if not order_by:
        return df.dropDuplicates(keys)

    order_exprs = []
    for column_name, direction in order_by:
        if column_name not in df.columns:
            raise ValueError(
                f"Dedupe order column missing from DataFrame: {column_name}"
            )

        if str(direction).lower().startswith("desc"):
            order_exprs.append(F.col(column_name).desc_nulls_last())
        else:
            order_exprs.append(F.col(column_name).asc_nulls_last())

    window = Window.partitionBy(*keys).orderBy(*order_exprs)

    return (
        df.withColumn("_silver_row_number", F.row_number().over(window))
          .filter(F.col("_silver_row_number") == 1)
          .drop("_silver_row_number")
    )


def enforce_required(df, required_columns):
    for column_name in required_columns:
        if column_name not in df.columns:
            raise ValueError(f"Required column missing: {column_name}")

        df = df.filter(F.col(column_name).isNotNull())

    return df


def rename_columns(df, rename_map):
    for old_name, new_name in rename_map.items():
        if old_name not in df.columns:
            raise ValueError(
                f"Rename rule references missing column: {old_name}"
            )

        df = df.withColumnRenamed(old_name, new_name)

    return df


def duplicate_count(df, keys):
    if not keys:
        return 0

    return (
        df.groupBy(*keys)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Main table-cleaning function

# COMMAND ----------

def clean_table(source_table_name, config):
    source = f"{CATALOG}.{BRONZE_SCHEMA}.{source_table_name}"
    target = f"{CATALOG}.{SILVER_SCHEMA}.{config['target']}"

    print("\n" + "=" * 90)
    print(f"BRONZE -> SILVER")
    print(f"Source: {source}")
    print(f"Target: {target}")
    print("=" * 90)

    df = spark.table(source)

    original_count = df.count()
    print(f"Raw rows: {original_count:,}")

    # 1) Generic string cleanup.
    df = trim_and_nullify_strings(df)

    # 2) Explicit business-key normalization.
    df = apply_regex_rules(
        df,
        config.get("regex_replace", {})
    )

    # 3) Explicit source-specific transformation.
    if config.get("special_transform"):
        df = apply_special_transform(
            df,
            config["special_transform"]
        )

    # 4) Normalize categorical values.
    df = apply_value_maps(
        df,
        config.get("value_maps", {})
    )

    # 5) Parse dates, preserving DATE type and invalid values as NULL.
    for column_name, fmt in config.get("dates", {}).items():
        if column_name not in df.columns:
            raise ValueError(
                f"Date configuration references missing column: {column_name}"
            )
        df = safe_to_date(df, column_name, fmt)

    # 6) Safe numeric/type casting.
    for column_name, dtype in config.get("casts", {}).items():
        if column_name not in df.columns:
            raise ValueError(
                f"Cast configuration references missing column: {column_name}"
            )
        df = safe_cast(df, column_name, dtype)

    # 7) Table-specific rules that depend on cleaned/cast columns.
    if config.get("post_transform"):
        df = apply_post_transform(
            df,
            config["post_transform"]
        )

    # 8) Fill only explicitly configured categorical nulls.
    #    We intentionally DO NOT fill date nulls with artificial dates.
    fillna_config = config.get("fillna", {})
    if fillna_config:
        existing_fill = {
            k: v for k, v in fillna_config.items()
            if k in df.columns
        }
        if existing_fill:
            df = df.fillna(existing_fill)

    # 9) Deduplicate after types/dates have been standardized.
    df = deduplicate(
        df,
        config.get("dedupe")
    )

    # 10) Remove records that cannot form a usable Silver entity/fact.
    df = enforce_required(
        df,
        config.get("required", [])
    )

    cleaned_count = df.count()

    # 11) Friendly Silver column names.
    df = rename_columns(
        df,
        config.get("rename", {})
    )

    # 12) Final simple quality check on renamed dedupe keys.
    dedupe_keys = config.get("dedupe", {}).get("keys", [])
    rename_map = config.get("rename", {})
    silver_dedupe_keys = [
        rename_map.get(k, k) for k in dedupe_keys
    ]

    remaining_duplicate_groups = duplicate_count(
        df,
        silver_dedupe_keys
    )

    print(f"Silver rows before write: {cleaned_count:,}")
    print(f"Rows removed:             {original_count - cleaned_count:,}")
    print(f"Duplicate key groups:     {remaining_duplicate_groups:,}")

    # 13) Delta write.
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(target)
    )

    print(f"Saved: {target}")

    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover and process the Bronze tables

# COMMAND ----------

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}"
)

available_rows = spark.sql(
    f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}"
).collect()

available_tables = {
    row.tableName.lower(): row.tableName
    for row in available_rows
}

print("Available Bronze tables:")
for name in sorted(available_tables.values()):
    print(f"  - {name}")

if ONLY_TABLES:
    requested = {name.lower() for name in ONLY_TABLES}
else:
    requested = set(TABLE_CONFIGS.keys())

processed = []
skipped = []

for configured_name, config in TABLE_CONFIGS.items():
    key = configured_name.lower()

    if key not in requested:
        continue

    if key not in available_tables:
        skipped.append(configured_name)
        print(f"\nSKIP: configured table not found in Bronze: {configured_name}")
        continue

    actual_name = available_tables[key]
    clean_table(actual_name, config)
    processed.append(configured_name)

print("\n" + "=" * 90)
print("PIPELINE SUMMARY")
print("=" * 90)
print(f"Processed ({len(processed)}):")
for name in processed:
    print(f"  - {name}")

if skipped:
    print(f"Skipped because missing ({len(skipped)}):")
    for name in skipped:
        print(f"  - {name}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Relationship / referential checks
# MAGIC
# MAGIC These checks do not delete rows. They show whether the standardized keys
# MAGIC actually connect the Silver CRM and ERP tables as intended.

# COMMAND ----------

def table_exists(qualified_name):
    try:
        spark.table(qualified_name)
        return True
    except Exception:
        return False


def count_missing_fk(child_df, child_col, parent_df, parent_col):
    parent_keys = (
        parent_df
        .select(F.col(parent_col).alias("_parent_key"))
        .where(F.col("_parent_key").isNotNull())
        .distinct()
    )

    return (
        child_df
        .where(F.col(child_col).isNotNull())
        .join(
            parent_keys,
            child_df[child_col] == parent_keys["_parent_key"],
            "left_anti"
        )
        .count()
    )


if RUN_RELATIONSHIP_CHECKS:
    silver = lambda table: f"{CATALOG}.{SILVER_SCHEMA}.{table}"

    required_targets = {
        "customer": silver("silver_customer_info"),
        "demographics": silver("silver_customer_demographics"),
        "location": silver("silver_customer_location"),
        "product": silver("silver_product_info"),
        "category": silver("silver_product_category"),
        "sales": silver("silver_sales_details"),
    }

    existing = {
        name: table_exists(path)
        for name, path in required_targets.items()
    }

    print("\n" + "=" * 90)
    print("RELATIONSHIP CHECKS")
    print("=" * 90)

    if existing["sales"] and existing["customer"]:
        sales = spark.table(required_targets["sales"])
        customers = spark.table(required_targets["customer"])

        missing = count_missing_fk(
            sales, "customer_id",
            customers, "customer_id"
        )
        print(
            "Sales customer_id values missing from customer_info: "
            f"{missing:,}"
        )

    if existing["sales"] and existing["product"]:
        sales = spark.table(required_targets["sales"])
        products = spark.table(required_targets["product"])

        missing = count_missing_fk(
            sales, "product_key",
            products, "product_key"
        )
        print(
            "Sales product_key values missing from product_info: "
            f"{missing:,}"
        )

    if existing["product"] and existing["category"]:
        products = spark.table(required_targets["product"])
        categories = spark.table(required_targets["category"])

        missing = count_missing_fk(
            products, "category_id",
            categories, "category_id"
        )
        print(
            "Product category_id values missing from product_category: "
            f"{missing:,}"
        )

    if existing["customer"] and existing["demographics"]:
        customers = spark.table(required_targets["customer"])
        demographics = spark.table(required_targets["demographics"])

        missing = count_missing_fk(
            customers, "customer_key",
            demographics, "customer_key"
        )
        print(
            "Customer customer_key values missing from demographics: "
            f"{missing:,}"
        )

    if existing["customer"] and existing["location"]:
        customers = spark.table(required_targets["customer"])
        locations = spark.table(required_targets["location"])

        missing = count_missing_fk(
            customers, "customer_key",
            locations, "customer_key"
        )
        print(
            "Customer customer_key values missing from location: "
            f"{missing:,}"
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional inspection
# MAGIC
# MAGIC Uncomment any display you want after the pipeline runs.

# COMMAND ----------

# display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_customer_info"))
# display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_product_info"))
# display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_sales_details"))
# display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_customer_demographics"))
# display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_customer_location"))
# display(spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_product_category"))