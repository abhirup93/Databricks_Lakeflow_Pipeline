-- # Gold — Price Prediction
-- Rule-based pricing optimisation model.
-- ## Methodology
-- 1. **Segment benchmarks** — compute avg, P25, P75 price per (property_type, destination_id)
-- 2. **Demand index** — `booking_count / avg_bookings_in_segment` (capped 0.1–3.0)
-- 3. **Rating premium factor** — `(property_rating - segment_avg_rating) / segment_avg_rating` (capped ±20%)
-- 4. **Predicted optimal price** — `benchmark_avg × (1 + demand_elasticity × 0.15) × (1 + rating_premium × 0.10)`
-- 5. **Pricing recommendation** — UNDERPRICED / OPTIMALLY PRICED / OVERPRICED (±15% tolerance band)
-- **Layer:** Gold | **Project:** wanderbricks_dlt | **Model version:** 1.0

CREATE OR REFRESH MATERIALIZED VIEW gold_price_prediction
COMMENT "Predicted optimal price per property using demand and quality signals — Gold layer"
TBLPROPERTIES (
  "quality"       = "gold",
  "layer"         = "gold",
  "project"       = "wanderbricks_dlt",
  "team"          = "data_engineering",
  "model_version" = "1.0",
  "model_type"    = "rule_based_pricing",
  "grain"         = "property_id"
)
AS
WITH segment_benchmarks AS (
  -- Step 1: Price benchmarks per (property_type, destination)
  SELECT
    property_type,
    destination_id,
    AVG(base_price)                                               AS avg_price_by_segment,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY base_price)     AS p25_price,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY base_price)     AS p75_price,
    COUNT(*)                                                      AS properties_in_segment
  FROM silver_properties
  GROUP BY property_type, destination_id
),
property_metrics AS (
  -- Step 2: Join performance metrics with segment benchmarks
  SELECT
    gpp.property_id,
    gpp.title,
    gpp.property_type,
    gpp.destination,
    gpp.country,
    gpp.state_or_province,
    gpp.bedrooms,
    gpp.bathrooms,
    gpp.max_guests,
    gpp.avg_base_price                                            AS actual_price,
    gpp.booking_count,
    gpp.total_revenue,
    gpp.avg_review_rating,
    gpp.review_count,
    sp.destination_id,
    sb.avg_price_by_segment,
    sb.p25_price,
    sb.p75_price,
    sb.properties_in_segment,
    -- Segment-level averages for normalisation
    AVG(gpp.booking_count)
      OVER (PARTITION BY gpp.property_type, sp.destination_id)   AS avg_bookings_in_segment,
    AVG(gpp.avg_review_rating)
      OVER (PARTITION BY gpp.property_type, sp.destination_id)   AS avg_rating_in_segment
  FROM gold_property_performance gpp
  JOIN silver_properties         sp  ON gpp.property_id    = sp.property_id
  JOIN segment_benchmarks             sb  ON sp.property_type   = sb.property_type
                                         AND sp.destination_id  = sb.destination_id
),
scored AS (
  -- Step 3: Compute demand index and rating premium (both capped to avoid extremes)
  SELECT
    *,
    ROUND(
      GREATEST(LEAST(
        CASE WHEN avg_bookings_in_segment > 0
             THEN booking_count / avg_bookings_in_segment
             ELSE 1.0 END,
        3.0), 0.1),
    3) AS demand_index,
    ROUND(
      GREATEST(LEAST(
        CASE WHEN avg_rating_in_segment > 0 AND avg_review_rating IS NOT NULL
             THEN (avg_review_rating - avg_rating_in_segment) / avg_rating_in_segment
             ELSE 0.0 END,
        0.20), -0.10),
    3) AS rating_premium_factor
  FROM property_metrics
)
-- Step 4: Predicted price + recommendation
SELECT
  property_id,
  title,
  property_type,
  destination,
  country,
  state_or_province,
  bedrooms,
  bathrooms,
  max_guests,
  actual_price,
  booking_count,
  avg_review_rating,
  review_count,
  ROUND(avg_price_by_segment, 2)  AS benchmark_avg_price,
  ROUND(p25_price, 2)             AS benchmark_p25_price,
  ROUND(p75_price, 2)             AS benchmark_p75_price,
  properties_in_segment,
  demand_index,
  rating_premium_factor,
  -- Predicted optimal price
  ROUND(
    avg_price_by_segment
    * (1 + (demand_index - 1)      * 0.15)   -- demand elasticity: ±15% per demand unit
    * (1 + rating_premium_factor   * 0.10),  -- quality premium:   ±10% per rating unit
  2) AS predicted_optimal_price,
  -- Pricing recommendation (±15% tolerance band around predicted price)
  CASE
    WHEN actual_price < ROUND(avg_price_by_segment
                              * (1 + (demand_index - 1) * 0.15)
                              * (1 + rating_premium_factor * 0.10), 2) * 0.85
         THEN 'UNDERPRICED'
    WHEN actual_price > ROUND(avg_price_by_segment
                              * (1 + (demand_index - 1) * 0.15)
                              * (1 + rating_premium_factor * 0.10), 2) * 1.15
         THEN 'OVERPRICED'
    ELSE 'OPTIMALLY PRICED'
  END AS pricing_recommendation,
  current_timestamp() AS _computed_at
FROM scored