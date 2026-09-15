# Modular Silver layer - Bike Data Lakehouse

This version is organized so every business table has its own notebook, while
all cleaning metadata lives in one external config and generic Spark logic is
shared.

## Structure

```text
bike_lakehouse_silver_modular/
├── common/
│   ├── silver_config.py
│   └── silver_utils.py
├── notebooks/
│   ├── 01_silver_crm_customer.py
│   ├── 02_silver_crm_product.py
│   ├── 03_silver_crm_sales.py
│   ├── 04_silver_erp_customer_demographics.py
│   ├── 05_silver_erp_customer_location.py
│   ├── 06_silver_erp_product_category.py
│   └── 07_silver_diagnostics.py
└── pipeline/
    └── 00_run_silver_pipeline.py
```

## How to use in Databricks

Import/upload the complete folder while preserving its directory structure.
The `.py` files use Databricks source-notebook format.

Run an individual table notebook whenever you want to develop or debug a
single transformation.

To run everything sequentially, run:

```text
pipeline/00_run_silver_pipeline
```

The execution order is stored centrally in `common/silver_config.py` under
`PIPELINE_NOTEBOOKS`.

## Recommended Lakeflow Jobs setup

For a more realistic data-engineering project, create one Databricks notebook
task for each file in `notebooks/`.

The six table transformations are independent because they all read Bronze and
write separate Silver tables, so a scalable DAG is:

```text
01 customer ─────────────┐
02 product ──────────────┤
03 sales ────────────────┤
04 demographics ─────────┼──> 07 diagnostics
05 location ─────────────┤
06 product category ─────┘
```

If you explicitly want to practice a sequential pipeline, use:

```text
01 -> 02 -> 03 -> 04 -> 05 -> 06 -> 07
```

## Central configuration

Edit `common/silver_config.py` to change:

- catalog/schema names
- source and target tables
- deduplication keys
- date formats
- casts
- null handling
- categorical normalization
- column names
- pipeline execution order

The generic cleaning engine is in `common/silver_utils.py`.

## Cleaning fixes retained

- Silver dates remain Spark `DATE` values.
- Meaningful NULL dates are preserved.
- Customer business keys stay strings so leading zeros survive.
- CRM/ERP customer keys are normalized consistently.
- CRM product keys are split into `category_id` and `product_key`.
- Product version end dates are derived from the next start date.
- Current product versions keep `end_date = NULL`.
- Numeric casts use `try_cast`.
- The customer gender column typo is corrected.
