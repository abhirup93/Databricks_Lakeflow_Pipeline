# Wanderbricks DLT Medallion Pipeline

## Overview

End-to-end **Delta Live Tables (DLT)** project following the medallion architecture
for the `samples.wanderbricks` dataset.

| Layer  | Tables                                          | Type               |
|--------|-------------------------------------------------|--------------------|
| Bronze | bronze_bookings, bronze_properties, bronze_reviews | Materialized View |
| Silver | silver_bookings, silver_properties              | Materialized View  |
| Gold   | gold_property_performance, gold_price_prediction | Materialized View |

---

## Source Schema: `samples.wanderbricks`

| Table        | Rows   | Description                     |
|--------------|--------|---------------------------------|
| bookings     | 72,247 | Guest reservation records       |
| properties   | 18,163 | Property listings               |
| reviews      | 99,793 | Guest reviews with ratings      |
| destinations | 42     | Destination reference data      |

---

## Entity Relationships

```
destinations (42)
     │ destination_id
     ▼
properties (18,163)
     │ property_id
     ├─────────────────────► bookings (72,247)
     └─────────────────────► reviews  (99,793)
```

**Key Joins:**
- `bookings.property_id → properties.property_id`
- `properties.destination_id → destinations.destination_id`
- `reviews.property_id → properties.property_id`

---

## Data Quality Rules

| Table           | Constraint      | Expression           | Action   |
|-----------------|-----------------|----------------------|----------|
| silver_bookings | valid_checkout  | check_out > check_in | DROP ROW |

---

## Gold Layer Details

### `gold_property_performance`
Aggregated per-property KPIs:
- `booking_count` — distinct booking IDs
- `total_revenue` — sum of `total_amount`
- `avg_review_rating` — mean rating (excluding deleted reviews)
- `avg_base_price` — listed base price from silver_properties

### `gold_price_prediction`
Rule-based pricing optimisation model (v1.0):
- **demand_index** — `booking_count / segment_avg_bookings` (capped 0.1–3.0)
- **rating_premium_factor** — normalised rating delta (capped ±20%)
- **predicted_optimal_price** — benchmark × demand elasticity × quality premium
- **pricing_recommendation** — UNDERPRICED / OPTIMALLY PRICED / OVERPRICED (±15% band)

---

## Pipeline & Schedule

| Setting          | Value                             |
|------------------|-----------------------------------|
| Pipeline name    | `wanderbricks_medallion_pipeline` |
| Catalog          | `main`                            |
| Target schema    | `wanderbricks_gold`               |
| Job name         | `wanderbricks_medallion_job`      |
| Schedule         | 6:00 AM IST (Asia/Kolkata)        |

---

## Folder Structure

```
wanderbricks_dlt_project/
├── explorations/
│   └── explore_gold_layer.py
├── ingestion/
│   └── bronze/
│       ├── bronze_bookings.sql
│       ├── bronze_properties.sql
│       └── bronze_reviews.sql
├── transformations/
│   ├── silver/
│   │   ├── silver_bookings.sql
│   │   └── silver_properties.sql
│   ├── gold/
│   │   ├── gold_property_performance.sql
│   │   └── gold_price_prediction.sql
│   └── README.md
├── utils/
│   └── utils.py
├── jobs/
│   └── wanderbricks_medallion_job.json
└── Pipelines/
    └── wanderbricks_medallion_pipeline.json
```
