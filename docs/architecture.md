# Architecture

## End-to-End Flow

```text
FMCG_2022_2024.csv
        ↓
Bronze Delta Table
        ↓
Silver Cleaned Delta Table
        ↓
Gold Star Schema
        ↓
Gold Semantic View
        ↓
Gold Analytics Tables + ML Feature Table
        ↓
MLflow Training + Batch Inference
        ↓
Dashboard & Business Insights
```

## Design Rationale

The pipeline follows the Medallion Architecture pattern:

- **Bronze** stores raw ingested data.
- **Silver** standardizes and cleans the data.
- **Gold** organizes the data into fact/dimension models, analytics tables, and machine learning feature tables.

## Star Schema

### Fact Table

`fact_sales` stores measurable sales events and operational metrics:

- units sold
- revenue
- price
- stock available
- delivered quantity
- delivery days

### Dimension Tables

- `dim_date`: calendar attributes
- `dim_product`: SKU, brand, segment, category, pack type
- `dim_channel`: sales channel
- `dim_region`: sales region
- `dim_promotion`: promotion status

## Semantic View

`vw_fact_sales_enriched` joins the fact table with all dimensions. It provides a business-friendly layer for dashboard queries and feature engineering.
