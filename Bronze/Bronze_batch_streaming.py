# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC
# MAGIC - `batch`: all CSV files are loaded in batch mode exactly as in the original notebook.
# MAGIC - `streaming`: `cust_info` and `sales_details` are loaded with Auto Loader.
# MAGIC   All other CSV files are still loaded in batch mode.

# COMMAND ----------

# DBTITLE 1,Ingestion mode
dbutils.widgets.dropdown(
    "ingestion_mode",
    "batch",
    ["batch", "streaming"]
)

ingestion_mode = dbutils.widgets.get("ingestion_mode").lower()

if ingestion_mode not in ["batch", "streaming"]:
    raise ValueError(
        f"Invalid ingestion_mode: {ingestion_mode}. "
        "Use 'batch' or 'streaming'."
    )

print(f"Ingestion mode: {ingestion_mode}")

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

# Original Bronze source volume
volume_base_path = (
    "/Volumes/Data_Lakehouse_Databricks/Bronze/Bronze_Vol/"
)

# Landing volume used only by Auto Loader
landing_base_path = (
    "/Volumes/data_lakehouse_databricks/bronze/landing_vol"
)

catalog = "Data_Lakehouse_Databricks"
schema = "Bronze"

folders = [
    "source_erp",
    "source_crm"
]

# Only these two tables become streaming when mode=streaming
streaming_sources = {
    "cust_info": {
        "source_path": (
            f"{landing_base_path}/landing_cust"
        ),
        "schema_path": (
            f"{landing_base_path}/_schemas/landing_cust"
        ),
        "checkpoint_path": (
            f"{landing_base_path}/_checkpoints/landing_cust"
        ),
    },

    "sales_details": {
        "source_path": (
            f"{landing_base_path}/landing_sales"
        ),
        "schema_path": (
            f"{landing_base_path}/_schemas/landing_sales"
        ),
        "checkpoint_path": (
            f"{landing_base_path}/_checkpoints/landing_sales"
        ),
    },
}

# COMMAND ----------

# DBTITLE 1,Batch loader
def load_batch(file_info, folder_prefix):

    filename = file_info.name.replace(".csv", "")
    table_name = (
        f"bronze_{folder_prefix}_{filename.lower()}"
    )

    full_table_name = (
        f"{catalog}.{schema}.{table_name}"
    )

    print(f"\n[BATCH] {file_info.name}")
    print(f"Target: {full_table_name}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_info.path)
    )

    print(f"Rows: {df.count()}")
    df.printSchema()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(full_table_name)
    )

    print(f"✓ Loaded {full_table_name}")

# COMMAND ----------

# DBTITLE 1,Streaming loader
def load_streaming(base_filename, folder_prefix):

    config = streaming_sources[base_filename]

    table_name = (
        f"bronze_{folder_prefix}_{base_filename}"
    )

    full_table_name = (
        f"{catalog}.{schema}.{table_name}"
    )

    print(f"\n[STREAMING] {base_filename}")
    print(f"Source: {config['source_path']}")
    print(f"Target: {full_table_name}")

    df_stream = (
        spark.readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            "csv"
        )
        .option(
            "cloudFiles.schemaLocation",
            config["schema_path"]
        )
        .option(
            "header",
            "true"
        )
        .load(
            config["source_path"]
        )
    )

    # Keep exactly the same schema as the original batch tables.
    # Auto Loader may add _rescued_data automatically.
    if "_rescued_data" in df_stream.columns:
        df_stream = df_stream.drop("_rescued_data")

    query = (
        df_stream.writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            config["checkpoint_path"]
        )
        .trigger(
            availableNow=True
        )
        .toTable(
            full_table_name
        )
    )

    query.awaitTermination()

    print(f"✓ Loaded {full_table_name}")

# COMMAND ----------

# DBTITLE 1,Load Bronze tables
print(
    f"\n=== Creating Bronze tables "
    f"(mode={ingestion_mode}) ===\n"
)

for folder in folders:

    folder_path = (
        volume_base_path
        + folder
        + "/"
    )

    folder_prefix = (
        folder.replace(
            "source_",
            ""
        )
    )

    print(
        f"\nProcessing folder: {folder}"
    )

    try:

        files = dbutils.fs.ls(
            folder_path
        )

        csv_files = [
            f
            for f in files
            if f.name.lower().endswith(
                ".csv"
            )
        ]

        for file_info in csv_files:

            base_filename = (
                file_info.name
                .replace(".csv", "")
                .lower()
            )

            # -----------------------------------------
            # STREAMING MODE
            # cust_info + sales_details -> Auto Loader
            # everything else -> batch
            # -----------------------------------------
            if (
                ingestion_mode == "streaming"
                and folder_prefix == "crm"
                and base_filename in streaming_sources
            ):

                load_streaming(
                    base_filename=base_filename,
                    folder_prefix=folder_prefix
                )

            # -----------------------------------------
            # BATCH MODE
            # all files -> regular Spark batch read
            #
            # Also used for the four non-streaming
            # tables when mode=streaming.
            # -----------------------------------------
            else:

                load_batch(
                    file_info=file_info,
                    folder_prefix=folder_prefix
                )

    except Exception as e:

        print(
            f"✗ Error processing "
            f"{folder}: {str(e)}"
        )

print(
    "\n=== Bronze tables creation completed ==="
)

# COMMAND ----------

# DBTITLE 1,Query ERP Bronze Tables
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM Data_Lakehouse_Databricks.Bronze.bronze_erp_cust_az12
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM Data_Lakehouse_Databricks.Bronze.bronze_erp_loc_a101
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM Data_Lakehouse_Databricks.Bronze.bronze_erp_px_cat_g1v2
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Query CRM Bronze Tables
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM Data_Lakehouse_Databricks.Bronze.bronze_crm_cust_info
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM Data_Lakehouse_Databricks.Bronze.bronze_crm_prd_info
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM Data_Lakehouse_Databricks.Bronze.bronze_crm_sales_details
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create diagnostic table for missing info, nulls, etc.

# COMMAND ----------

# DBTITLE 1,Create Bronze diagnostic table
tables = (
    spark.sql(
        f"SHOW TABLES IN {catalog}.{schema}"
    )
    .filter(
        "tableName LIKE 'bronze_%'"
    )
    .collect()
)

diagnostics = []

for table_row in tables:

    table_name = (
        table_row["tableName"]
    )

    full_table_name = (
        f"{catalog}.{schema}.{table_name}"
    )

    print(
        f"Analyzing {table_name}..."
    )

    try:

        df = spark.table(
            full_table_name
        )

        row_count = df.count()
        col_count = len(df.columns)

        null_counts = {}

        for col in df.columns:

            null_count = (
                df
                .filter(
                    f"`{col}` IS NULL"
                )
                .count()
            )

            null_counts[col] = (
                null_count
            )

        total_nulls = sum(
            null_counts.values()
        )

        cols_with_nulls = sum(
            1
            for count
            in null_counts.values()
            if count > 0
        )

        total_cells = (
            row_count
            * col_count
        )

        completeness_pct = (
            (
                (
                    total_cells
                    - total_nulls
                )
                / total_cells
                * 100
            )
            if total_cells > 0
            else 0
        )

        diagnostics.append({
            "table_name":
                table_name,

            "row_count":
                row_count,

            "column_count":
                col_count,

            "total_null_values":
                total_nulls,

            "columns_with_nulls":
                cols_with_nulls,

            "completeness_percentage":
                round(
                    completeness_pct,
                    2
                ),

            "null_details":
                str(null_counts)
        })

    except Exception as e:

        print(
            f"Error analyzing "
            f"{table_name}: {str(e)}"
        )

        diagnostics.append({
            "table_name":
                table_name,

            "row_count":
                None,

            "column_count":
                None,

            "total_null_values":
                None,

            "columns_with_nulls":
                None,

            "completeness_percentage":
                None,

            "null_details":
                f"Error: {str(e)}"
        })

# COMMAND ----------

from pyspark.sql import Row

diagnostic_df = (
    spark.createDataFrame(
        [
            Row(**d)
            for d in diagnostics
        ]
    )
)

print(
    "\n=== Bronze Tables "
    "Diagnostic Summary ==="
)

display(
    diagnostic_df
)

# COMMAND ----------

diagnostic_table_name = (
    f"{catalog}.{schema}."
    "diagnostics_bronze"
)

(
    diagnostic_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        diagnostic_table_name
    )
)

print(
    f"\n✓ Diagnostic table saved as: "
    f"{diagnostic_table_name}"
)