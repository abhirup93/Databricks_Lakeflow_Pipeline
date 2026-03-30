# Databricks notebook source
# MAGIC %md
# MAGIC # Explore Gold Layer — Wanderbricks DLT
# MAGIC
# MAGIC Exploratory analysis on Gold layer tables produced by the `wanderbricks_medallion_pipeline`.
# MAGIC
# MAGIC | Gold Table | Description |
# MAGIC |---|---|
# MAGIC | `gold_property_performance` | Per-property booking count, revenue, avg rating |
# MAGIC | `gold_price_prediction` | Predicted optimal price and pricing recommendation |
# MAGIC
# MAGIC > Run the DLT pipeline first before executing this notebook.

# COMMAND ----------

catalog = "main"
schema  = "wanderbricks_gold"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Top 20 Properties by Revenue

# COMMAND ----------

display(spark.sql(f"""
SELECT
  property_id,
  title,
  property_type,
  destination,
  country,
  booking_count,
  ROUND(total_revenue, 2)    AS total_revenue,
  avg_review_rating,
  avg_base_price
FROM {catalog}.{schema}.gold_property_performance
ORDER BY total_revenue DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Revenue & Bookings by Property Type

# COMMAND ----------

display(spark.sql(f"""
SELECT
  property_type,
  COUNT(*)                         AS num_properties,
  SUM(booking_count)               AS total_bookings,
  ROUND(SUM(total_revenue), 2)     AS total_revenue,
  ROUND(AVG(avg_review_rating), 2) AS avg_rating,
  ROUND(AVG(avg_base_price), 2)    AS avg_price
FROM {catalog}.{schema}.gold_property_performance
GROUP BY property_type
ORDER BY total_revenue DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Top 15 Destinations by Total Bookings

# COMMAND ----------

display(spark.sql(f"""
SELECT
  destination,
  country,
  COUNT(*)                         AS num_properties,
  SUM(booking_count)               AS total_bookings,
  ROUND(SUM(total_revenue), 2)     AS total_revenue,
  ROUND(AVG(avg_review_rating), 2) AS avg_rating
FROM {catalog}.{schema}.gold_property_performance
GROUP BY destination, country
ORDER BY total_bookings DESC
LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pricing Recommendation Distribution

# COMMAND ----------

display(spark.sql(f"""
SELECT
  pricing_recommendation,
  COUNT(*)                                              AS property_count,
  ROUND(AVG(actual_price), 2)                           AS avg_actual_price,
  ROUND(AVG(predicted_optimal_price), 2)                AS avg_predicted_price,
  ROUND(AVG(predicted_optimal_price - actual_price), 2) AS avg_price_gap
FROM {catalog}.{schema}.gold_price_prediction
GROUP BY pricing_recommendation
ORDER BY property_count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. High Demand Properties (demand_index > 1.5)

# COMMAND ----------

display(spark.sql(f"""
SELECT
  property_id,
  title,
  property_type,
  destination,
  actual_price,
  predicted_optimal_price,
  demand_index,
  avg_review_rating,
  pricing_recommendation
FROM {catalog}.{schema}.gold_price_prediction
WHERE demand_index > 1.5
ORDER BY demand_index DESC
LIMIT 25
"""))
