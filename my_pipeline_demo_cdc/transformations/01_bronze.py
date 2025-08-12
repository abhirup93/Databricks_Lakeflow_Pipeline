import dlt
from pyspark.sql.functions import *

catalog = spark.conf.get("catalog")  # returns "workspace"
schema = spark.conf.get("schema")    # returns "my_pipeline_demo_cdc"

##Create the bronze information table containing the raw JSON data taken from the storage path generated from 00_Data_CDC_Generator notebook

@dlt.table(
    name="employees_raw",
    comment="New employees data incrementally ingested from cloud object storage landing zone",
)
def employees_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"/Volumes/{catalog}/{schema}/raw_data/employees")
    )

@dlt.table(
    name="locations_raw",
    comment="New locations data incrementally ingested from cloud object storage landing zone",
)
def locations_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(f"/Volumes/{catalog}/{schema}/raw_data/locations_info")
    )