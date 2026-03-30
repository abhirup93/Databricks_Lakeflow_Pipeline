-- # Gold — Property Performance
-- Aggregates per-property KPIs from the Silver layer:
-- - **booking_count**: distinct confirmed + all-status bookings
-- - **total_revenue**: sum of `total_amount` from silver_bookings
-- - **avg_review_rating**: mean rating from bronze_reviews (excluding deleted)
-- - **avg_base_price**: the property's listed base price from silver_properties
-- **Layer:** Gold | **Project:** wanderbricks_dlt

CREATE OR REFRESH MATERIALIZED VIEW gold_property_performance
COMMENT "Per-property aggregated performance: booking count, revenue, avg rating, avg base price — Gold layer"
TBLPROPERTIES (
  "quality"  = "gold",
  "layer"    = "gold",
  "project"  = "wanderbricks_dlt",
  "team"     = "data_engineering",
  "grain"    = "property_id"
)
AS
SELECT
  p.property_id,
  p.title,
  p.property_type,
  p.destination,
  p.country,
  p.state_or_province,
  p.bedrooms,
  p.bathrooms,
  p.max_guests,
  p.base_price                               AS avg_base_price,
  COUNT(DISTINCT b.booking_id)               AS booking_count,
  COALESCE(SUM(b.total_amount), 0)           AS total_revenue,
  ROUND(AVG(r.rating), 2)                    AS avg_review_rating,
  COUNT(DISTINCT r.review_id)                AS review_count
FROM silver_properties p
LEFT JOIN silver_bookings  b ON p.property_id = b.property_id
LEFT JOIN bronze_reviews   r ON p.property_id = r.property_id
                                  AND r.is_deleted = false
GROUP BY
  p.property_id,
  p.title,
  p.property_type,
  p.destination,
  p.country,
  p.state_or_province,
  p.bedrooms,
  p.bathrooms,
  p.max_guests,
  p.base_price