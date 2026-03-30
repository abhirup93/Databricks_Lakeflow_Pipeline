-- # Bronze — Reviews
-- Ingests raw guest reviews from `samples.wanderbricks.reviews` as a DLT Materialized View.
-- **Layer:** Bronze | **Project:** wanderbricks_dlt

CREATE OR REFRESH MATERIALIZED VIEW bronze_reviews
COMMENT "Raw guest reviews ingested from samples.wanderbricks — Bronze layer"
TBLPROPERTIES (
  "quality"  = "bronze",
  "layer"    = "bronze",
  "project"  = "wanderbricks_dlt",
  "team"     = "data_engineering",
  "source"   = "samples.wanderbricks.reviews"
)
AS
SELECT
  review_id,
  booking_id,
  property_id,
  user_id,
  rating,
  comment,
  is_deleted,
  created_at,
  updated_at,
  current_timestamp() AS _ingested_at
FROM samples.wanderbricks.reviews