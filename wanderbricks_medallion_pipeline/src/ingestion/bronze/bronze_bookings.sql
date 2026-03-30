-- # Bronze — Bookings
-- Ingests raw bookings from `samples.wanderbricks.bookings` as a DLT Materialized View.
-- **Layer:** Bronze | **Project:** wanderbricks_dlt

CREATE OR REFRESH MATERIALIZED VIEW bronze_bookings
COMMENT "Raw bookings ingested from samples.wanderbricks — Bronze layer"
TBLPROPERTIES (
  "quality"  = "bronze",
  "layer"    = "bronze",
  "project"  = "wanderbricks_dlt",
  "team"     = "data_engineering",
  "source"   = "samples.wanderbricks.bookings"
)
AS
SELECT
  booking_id,
  user_id,
  property_id,
  check_in,
  check_out,
  guests_count,
  total_amount,
  status,
  created_at,
  updated_at,
  current_timestamp() AS _ingested_at
FROM samples.wanderbricks.bookings