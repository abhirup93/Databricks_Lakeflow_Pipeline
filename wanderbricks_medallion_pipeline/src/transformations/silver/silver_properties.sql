-- # Silver — Properties
-- Enriches bronze properties with destination details: country, state_or_province.
-- Joins `bronze_properties` with `samples.wanderbricks.destinations` (reference table, 42 rows).
-- **Layer:** Silver | **Project:** wanderbricks_dlt

CREATE OR REFRESH MATERIALIZED VIEW silver_properties
COMMENT "Properties enriched with destination country and state — Silver layer"
TBLPROPERTIES (
  "quality"  = "silver",
  "layer"    = "silver",
  "project"  = "wanderbricks_dlt",
  "team"     = "data_engineering",
  "join"     = "bronze_properties JOIN destinations ON destination_id"
)
AS
SELECT
  p.property_id,
  p.host_id,
  p.destination_id,
  p.title,
  p.description,
  p.base_price,
  p.property_type,
  p.max_guests,
  p.bedrooms,
  p.bathrooms,
  p.property_latitude,
  p.property_longitude,
  p.created_at,
  d.destination,
  d.country,
  d.state_or_province,
  d.state_or_province_code,
  p._ingested_at
FROM bronze_properties p
INNER JOIN samples.wanderbricks.destinations d ON p.destination_id = d.destination_id