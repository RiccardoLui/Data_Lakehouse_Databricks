# Databricks notebook source
# MAGIC %md
# MAGIC # Security and governance bootstrap
# MAGIC
# MAGIC Free-Edition-safe governance for the medallion project:
# MAGIC - metadata and data-classification tags
# MAGIC - table/column documentation
# MAGIC - catalog inventory views
# MAGIC - a dynamic masked customer view for non-admin consumers
# MAGIC
# MAGIC Enterprise RBAC is intentionally kept in `rbac_enterprise_template.sql`.

# COMMAND ----------

CATALOG = "data_lakehouse_databricks"
GOVERNANCE_SCHEMA = "governance"
GOLD_SCHEMA = "gold"
CUSTOMER_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.gold_dim_customers"
SECURE_CUSTOMER_VIEW = f"{CATALOG}.{GOLD_SCHEMA}.gold_dim_customers_secure"

# In Free Edition there is no account-level group administration. Capture the
# identity that deploys/runs this governance bootstrap as the privileged owner.
# In an enterprise workspace, replace this user check with an account group or ABAC policy.
OWNER_USER = spark.sql("SELECT current_user() AS user").first()["user"]
OWNER_USER_SQL = OWNER_USER.replace("'", "''")


def run_sql(statement: str, *, ignore_error: bool = False):
    try:
        print(f"\nSQL> {statement.strip()}")
        return spark.sql(statement)
    except Exception as exc:
        if ignore_error:
            print(f"WARNING: {exc}")
            return None
        raise


# COMMAND ----------

run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOVERNANCE_SCHEMA}")

run_sql(
    f"COMMENT ON CATALOG {CATALOG} IS "
    "'Portfolio lakehouse governed with Unity Catalog using Bronze, Silver and Gold layers'"
)

for schema_name, description in {
    "bronze": "Raw and incrementally ingested source data. Restricted write surface.",
    "silver": "Validated, standardized and deduplicated business entities.",
    "gold": "Curated dimensional model and analytics-ready data products.",
    "governance": "Governance metadata, inventories and security helper objects.",
}.items():
    run_sql(
        f"COMMENT ON SCHEMA {CATALOG}.{schema_name} IS '{description}'",
        ignore_error=True,
    )

# COMMAND ----------

# Inventory views: useful for governance checks without relying on account-level APIs.

run_sql(
    f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOVERNANCE_SCHEMA}.table_inventory AS
    SELECT
        table_catalog,
        table_schema,
        table_name,
        table_type,
        comment
    FROM {CATALOG}.information_schema.tables
    WHERE lower(table_schema) IN ('bronze', 'silver', 'gold', 'governance')
    """
)

run_sql(
    f"""
    CREATE OR REPLACE VIEW {CATALOG}.{GOVERNANCE_SCHEMA}.column_inventory AS
    SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        ordinal_position,
        full_data_type,
        is_nullable,
        comment
    FROM {CATALOG}.information_schema.columns
    WHERE lower(table_schema) IN ('bronze', 'silver', 'gold', 'governance')
    """
)

# COMMAND ----------

# Classify and document the customer dimension if it exists.

if spark.catalog.tableExists(CUSTOMER_TABLE):
    run_sql(
        f"ALTER TABLE {CUSTOMER_TABLE} "
        "SET TAGS ('layer' = 'gold', 'domain' = 'customer', 'classification' = 'confidential')",
        ignore_error=True,
    )

    column_names = {field.name.lower() for field in spark.table(CUSTOMER_TABLE).schema.fields}

    column_metadata = {
        "customer_id": ("Internal customer identifier", "internal_identifier"),
        "customer_key": ("Business customer key", "internal_identifier"),
        "firstname": ("Customer first name", "pii_name"),
        "lastname": ("Customer last name", "pii_name"),
        "birth_date": ("Customer date of birth", "pii_birth_date"),
        "country": ("Customer country", "geographic"),
        "marital_status": ("Customer marital status", "sensitive_demographic"),
        "gender": ("Customer gender value from source system", "sensitive_demographic"),
        "creation_date": ("Customer record creation date", "operational_metadata"),
    }

    for column_name, (comment, classification) in column_metadata.items():
        if column_name not in column_names:
            continue
        run_sql(
            f"ALTER TABLE {CUSTOMER_TABLE} ALTER COLUMN `{column_name}` "
            f"COMMENT '{comment}'",
            ignore_error=True,
        )
        run_sql(
            f"ALTER TABLE {CUSTOMER_TABLE} ALTER COLUMN `{column_name}` "
            f"SET TAGS ('classification' = '{classification}')",
            ignore_error=True,
        )

    # Dynamic view: the deploying owner sees full values; other consumers see masked/generalized PII.
    # The source table remains unchanged, which keeps the ETL pipeline stable.
    required = {
        "customer_id",
        "customer_key",
        "firstname",
        "lastname",
        "marital_status",
        "gender",
        "country",
        "birth_date",
        "creation_date",
    }

    if required.issubset(column_names):
        run_sql(
            f"""
            CREATE OR REPLACE VIEW {SECURE_CUSTOMER_VIEW} AS
            SELECT
                customer_id,
                CAST(customer_key AS STRING) AS customer_key,
                CASE
                    WHEN session_user() = '{OWNER_USER_SQL}' THEN firstname
                    WHEN firstname IS NULL THEN NULL
                    ELSE concat(substr(firstname, 1, 1), '***')
                END AS firstname,
                CASE
                    WHEN session_user() = '{OWNER_USER_SQL}' THEN lastname
                    WHEN lastname IS NULL THEN NULL
                    ELSE concat(substr(lastname, 1, 1), '***')
                END AS lastname,
                marital_status,
                gender,
                country,
                CASE
                    WHEN session_user() = '{OWNER_USER_SQL}' THEN birth_date
                    WHEN birth_date IS NULL THEN NULL
                    ELSE make_date(year(birth_date), 1, 1)
                END AS birth_date,
                creation_date
            FROM {CUSTOMER_TABLE}
            """
        )
        run_sql(
            f"COMMENT ON VIEW {SECURE_CUSTOMER_VIEW} IS "
            "'Governed customer view: direct identifiers are masked and birth date is generalized for non-admin users'",
            ignore_error=True,
        )
else:
    print(f"Skipping customer masking because {CUSTOMER_TABLE} does not exist yet.")

# COMMAND ----------

print("\nGovernance bootstrap complete.")
display(spark.table(f"{CATALOG}.{GOVERNANCE_SCHEMA}.table_inventory"))