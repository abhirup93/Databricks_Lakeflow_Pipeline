-- # Silver — Bookings
-- Enriches bronze bookings with property attributes (property_type, destination_id, base_price).
-- Applies data quality constraint: `check_out > check_in`.
-- **Layer:** Silver | **Project:** wanderbricks_dlt
-- **Data Quality Rule:**
-- - `valid_checkout`: check_out > check_in → DROP ROW on violation

CREATE OR REFRESH MATERIALIZED VIEW silver_bookings (
  CONSTRAINT valid_checkout EXPECT (check_out > check_in) ON VIOLATION DROP ROW
)
COMMENT "Bookings enriched with property_type, destination_id, base_price — Silver layer"
TBLPROPERTIES (
  "quality"          = "silver",
  "layer"            = "silver",
  "project"          = "wanderbricks_dlt",
  "team"             = "data_engineering",
  "dq_rule_1"        = "valid_checkout: check_out > check_in ON VIOLATION DROP ROW"
)
AS
SELECT
  b.booking_id,
  b.user_id,
  b.property_id,
  b.check_in,
  b.check_out,
  DATEDIFF(b.check_out, b.check_in)  AS nights_stayed,
  b.guests_count,
  b.total_amount,
  b.status,
  b.created_at,
  b.updated_at,
  p.property_type,
  p.destination_id,
  p.base_price,
  b._ingested_at
FROM bronze_bookings b
INNER JOIN bronze_properties p ON b.property_id = p.property_id
