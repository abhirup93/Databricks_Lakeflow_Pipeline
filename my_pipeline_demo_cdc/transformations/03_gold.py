import dlt
from pyspark.sql.functions import *

# ------------------------------------------------------------------------------
# 1) Define the target SCD2 dimension (streaming table)
#    If you don't pass an explicit schema, DLT will manage system columns.
#    When you DO define a schema yourself, include __START_AT and __END_AT with
#    the same type as the sequence_by column.  (per docs)
# ------------------------------------------------------------------------------

dlt.create_streaming_table(
    name="employee_location_dim",
    comment="SCD2 dimension tracking employees' base location history."
)
# ref: create_auto_cdc_flow requires a declared target streaming table. [1](https://learn.microsoft.com/en-us/azure/databricks/dlt-ref/dlt-python-ref-apply-changes)


# ------------------------------------------------------------------------------
# 2) Build a joined 'changes' stream to feed into CDC
#    - stream-static join to avoid stream-stream complications
#    - derive seq_at: use base_location_end_date when present, otherwise an event time
#    - derive __DELETE: close out SCD2 when inactive or there's an end date
# ------------------------------------------------------------------------------

@dlt.table(
    name="employee_location_changes_gold",
    comment="Joined employees & locations with sequencing and delete semantics for SCD2."
)
def employee_location_changes_gold():
    loc = dlt.read_stream("locations_silver")
    emp = dlt.read("employees_silver")  # treated as static snapshot in this flow

    # Choose a sequence column:
    # - If your source provides a reliable change timestamp (recommended), use it.
    # - You asked to use base_location_end_date; ensure it is never NULL for CDC sequencing.
    #   We coalesce to ingestion time for active records so NULLs don't break CDC ordering.
    j = (
        loc.alias("l")
          .join(emp.alias("e"), on="employee_id", how="left")
          .select(
              col("l.employee_id"),
              col("e.firstname").alias("firstname"),
              col("e.lastname").alias("lastname"),
              col("l.base_location"),
              col("l.active_in_base_location"),
              col("l.base_location_end_date"),
              # delete semantics: mark rows as deletes if inactive OR an end date exists
              when(
                  (col("l.active_in_base_location") == lit("N")) | col("l.base_location_end_date").isNotNull(),
                  lit(True)
              ).otherwise(lit(False)).alias("__DELETE"),
              # sequencing column: prefer end_date; if NULL, fall back to current ingestion time
              coalesce(
                  col("l.base_location_end_date").cast("timestamp"),
                  current_timestamp()
              ).alias("seq_at")
          )
    )
    return j


# ------------------------------------------------------------------------------
# 3) AUTO CDC (SCD2) into the dimension
#    - keys: employee_id
#    - sequence_by: seq_at
#    - stored_as_scd_type: "2" (track full history)
#    - apply_as_deletes: use __DELETE column/expr
#    - ignore_null_updates: avoid churn when only nulls are present in updates
# ------------------------------------------------------------------------------

dlt.create_auto_cdc_flow(
    target="employee_location_dim",
    source="employee_location_changes_gold",
    keys=["employee_id"],
    sequence_by=col("seq_at"),
    stored_as_scd_type="2",
    apply_as_deletes=col("__DELETE"),
    ignore_null_updates=True,
    # Don't persist the helper column into the dimension
    except_column_list=["__DELETE", "seq_at"]
)
# AUTO CDC docs & parameters (apply_as_deletes, sequence_by, SCD2, etc.). [1](https://learn.microsoft.com/en-us/azure/databricks/dlt-ref/dlt-python-ref-apply-changes)[2](https://learn.microsoft.com/en-us/azure/databricks/dlt/cdc)