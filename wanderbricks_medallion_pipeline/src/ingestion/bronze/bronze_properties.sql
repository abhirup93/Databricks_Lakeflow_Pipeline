-- # Bronze — Properties
-- Ingests raw property listings from `samples.wanderbricks.properties` as a DLT Materialized View.
-- **Layer:** Bronze | **Project:** wanderbricks_dlt

CREATE OR REFRESH MATERIALIZED VIEW bronze_properties
COMMENT "Raw property listings ingested from samples.wanderbricks — Bronze layer"
TBLPROPERTIES (
  "quality"  = "bronze",
  "layer"    = "bronze",
  "project"  = "wanderbricks_dlt",
  "team"     = "data_engineering",
  "source"   = "samples.wanderbricks.properties"
)
AS
SELECT
  property_id,
  host_id,
  destination_id,
  title,
  description,
  base_price,
  property_type,
  max_guests,
  bedrooms,
  bathrooms,
  property_latitude,
  property_longitude,
  created_at,
  current_timestamp() AS _ingested_at
FROM samples.wanderbricks.properties