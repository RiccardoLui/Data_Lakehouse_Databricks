# Silver cleaning package

This package refactors the three CRM cleaning notebooks into one
configuration-driven Bronze -> Silver pipeline and adds the three ERP tables.

## Files

- `silver_cleaning_pipeline.py`
  - **Ready to run in Databricks.**
  - Self-contained: configuration + cleaning functions + execution.
  - Upload/import it as a Databricks notebook or run it from a Databricks Repo.

- `silver_cleaning_config.py`
  - Optional extracted configuration for later modularization.
  - The main pipeline does not depend on it.

## Expected Bronze tables

- `bronze_crm_cust_info`
- `bronze_crm_prd_info`
- `bronze_crm_sales_details`
- `bronze_erp_cust_az12`
- `bronze_erp_loc_a101`
- `bronze_erp_px_cat_g1v2`

The default catalog/schema are:

```python
CATALOG = "data_lakehouse_databricks"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
```

Change those three variables near the top of the pipeline if needed.

## Silver tables created

- `silver_customer_info`
- `silver_product_info`
- `silver_sales_details`
- `silver_customer_demographics`
- `silver_customer_location`
- `silver_product_category`

## Important fixes included

1. Dates remain Spark `DATE` values.
2. Null dates are preserved; no artificial `2000-01-01` sentinel is inserted.
3. `cst_gndr` is renamed correctly to `gender`.
4. Customer business keys stay strings, so leading zeros are preserved.
5. CRM/ERP customer keys are normalized to the same digits-only representation:
   - `AW00011000` -> `00011000`
   - `NASAW00011000` -> `00011000`
   - `AW-00011000` -> `00011000`
6. CRM product keys are split:
   - `AC-HE-HL-U509-R`
   - `category_id = AC_HE`
   - `product_key = HL-U509-R`
7. Product end dates are derived from the next version's start date. The current
   product version keeps `end_date = NULL`.
8. Sales are deduplicated by `(order_number, product_key)`, not just order number.
9. Numeric casts use `try_cast`, so malformed values become null rather than
   crashing the pipeline.
10. Basic referential checks are printed after the run.

## Run all configured tables

The default is already to process all configured tables that exist in Bronze.

Just run the notebook top to bottom.

## Run only selected tables

Edit:

```python
ONLY_TABLES = [
    "bronze_crm_cust_info",
    "bronze_erp_cust_az12",
]
```

Tables not present in Bronze are skipped and reported.

## Notes

The pipeline intentionally does not try to "guess" unknown business rules.
Generic cleanup is automatic, while key parsing, category extraction and other
semantic rules are explicitly stored in the table configuration.
