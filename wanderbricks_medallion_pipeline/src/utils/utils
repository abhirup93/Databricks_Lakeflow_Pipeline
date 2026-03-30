# Databricks notebook source
# MAGIC %md
# MAGIC # Wanderbricks DLT — Utilities
# MAGIC
# MAGIC Placeholder module for shared utility functions.
# MAGIC Extend as the project grows (e.g. audit logging, schema validation, alert helpers).

# COMMAND ----------

# ============================================================
# Common configuration
# ============================================================

PROJECT          = "wanderbricks_dlt"
PIPELINE_CATALOG = "main"
PIPELINE_SCHEMA  = "wanderbricks_gold"

BRONZE_TABLES = ["bronze_bookings", "bronze_properties", "bronze_reviews"]
SILVER_TABLES = ["silver_bookings", "silver_properties"]
GOLD_TABLES   = ["gold_property_performance", "gold_price_prediction"]

# ============================================================
# Placeholder functions — implement as needed
# ============================================================

def get_pipeline_config() -> dict:
    """Return basic pipeline metadata."""
    return {
        "project": PROJECT,
        "catalog": PIPELINE_CATALOG,
        "schema":  PIPELINE_SCHEMA,
    }


def log_event(event: str, details: dict = None) -> None:
    """Lightweight event logger (extend with Delta Audit Log or UC lineage)."""
    import json
    import datetime
    payload = {
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "event":     event,
        "details":   details or {},
    }
    print(json.dumps(payload))


def validate_table_exists(catalog: str, schema: str, table: str) -> bool:
    """Check whether a UC table exists (placeholder)."""
    # TODO: use spark.catalog.tableExists or information_schema
    raise NotImplementedError("Implement using spark.catalog.tableExists()")
