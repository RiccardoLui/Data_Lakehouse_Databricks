"""
Configuration extracted from silver_cleaning_pipeline.py.

This file is useful if you later move the pipeline into a Databricks Repo
and want configuration separated from execution code.

The ready-to-run file is silver_cleaning_pipeline.py, which already contains
this configuration so it has no import/path dependencies.
"""

CATALOG = "data_lakehouse_databricks"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

TABLE_CONFIGS = {
    "bronze_crm_cust_info": {
        "target": "silver_customer_info",
        "dedupe": {
            "keys": ["cst_id"],
            "order_by": [("cst_create_date", "desc")]
        },
        "regex_replace": {
            "cst_key": [(r"(?i)^AW-?", "")]
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
        "required": ["cst_id", "cst_key"],
        "rename": {
            "cst_id": "customer_id",
            "cst_key": "customer_key",
            "cst_firstname": "firstname",
            "cst_lastname": "lastname",
            "cst_marital_status": "marital_status",
            "cst_gndr": "gender",
            "cst_create_date": "creation_date"
        }
    },

    "bronze_crm_prd_info": {
        "target": "silver_product_info",
        "dedupe": {
            "keys": ["prd_id"]
        },
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
        "required": ["prd_id", "prd_key"],
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

    "bronze_erp_cust_az12": {
        "target": "silver_customer_demographics",
        "dedupe": {
            "keys": ["CID"]
        },
        "regex_replace": {
            "CID": [(r"(?i)^(NAS)?AW-?", "")]
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
        "regex_replace": {
            "CID": [(r"(?i)^(NAS)?AW-?", "")]
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
