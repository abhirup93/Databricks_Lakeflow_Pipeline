# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbName = db = dbutils.widgets.get("schema")

volume_name = "raw_data"

# COMMAND ----------

spark.sql(f'USE CATALOG `{catalog}`')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`')
spark.sql(f'USE SCHEMA `{schema}`')
spark.sql(f'CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume_name}`')
volume_folder =  f"/Volumes/{catalog}/{db}/{volume_name}"

# COMMAND ----------

try:
    dbutils.fs.ls(volume_folder + "/employees")
    dbutils.fs.ls(volume_folder + "/locations_info")
    # If both folders exist we skip generation (same behavior as before)
except:
    print(f"Folder doesn't exist, generating the data under {volume_folder}...")

    from pyspark.sql import functions as F
    from faker import Faker
    import uuid
    import random

    fake = Faker()

    # Fixed set of location codes
    location_codes = ["MUM02", "KOL05", "DEL07", "BLR03", "HYD04"]

    # --- UDFs for employees / locations ---
    fake_id = F.udf(lambda: str(uuid.uuid4()))
    fake_firstname = F.udf(fake.first_name)
    fake_lastname = F.udf(fake.last_name)
    fake_location = F.udf(lambda: random.choice(location_codes))
    fake_past_date = F.udf(
        lambda: fake.date_between(start_date="-3y", end_date="today").strftime(
            "%Y-%m-%d"
        )
    )

    # Ensures new location != old location (robustly)
    @F.udf("string")
    def different_location(old_loc: str) -> str:
        choices = [c for c in location_codes if c != old_loc]
        return random.choice(choices) if choices else old_loc

    # ---------- EMPLOYEE CORE DETAILS ----------
    df_employees = spark.range(0, 5000).repartition(50)
    df_employees = (
        df_employees.withColumn("employee_id", fake_id())
        .withColumn("firstname", fake_firstname())
        .withColumn("lastname", fake_lastname())
        .select("employee_id", "firstname", "lastname")
        .repartition(50)
    )

    # >>> IMPORTANT <<<
    # Materialize the UUIDs ONCE and reuse for all downstream writes.
    df_employees = df_employees.persist()
    _ = df_employees.count()  # forces evaluation so subsequent actions read from cache

    # Save employees folder (core details)
    df_employees.write.format("json").mode("overwrite").save(
        volume_folder + "/employees"
    )

    # ---------- LOCATION HISTORY DETAILS ----------
    # Start from the SAME cached DataFrame so employee_id stays identical
    df_with_locs = df_employees.withColumn("old_location", fake_location()).withColumn(
        "new_location", different_location(F.col("old_location"))
    )

    # Old location record (inactive, with an end date)
    df_old = df_with_locs.select(
        "employee_id",
        F.concat_ws(" ", "firstname", "lastname").alias("employee_name"),
        F.col("old_location").alias("base_location"),
        F.lit("N").alias("active_in_base_location"),
        fake_past_date().alias("base_location_end_date"),
    )

    # New location record (active now, no end date)
    df_new = df_with_locs.select(
        "employee_id",
        F.concat_ws(" ", "firstname", "lastname").alias("employee_name"),
        F.col("new_location").alias("base_location"),
        F.lit("Y").alias("active_in_base_location"),
        F.lit(None).cast("string").alias("base_location_end_date"),
    )

    # Union both into location info and write once
    df_location_info = df_old.unionByName(df_new)

    # Save location info folder
    df_location_info.repartition(50).write.format("json").mode("overwrite").save(
        volume_folder + "/locations_info"
    )

    # Optionally free cache
    df_employees.unpersist()