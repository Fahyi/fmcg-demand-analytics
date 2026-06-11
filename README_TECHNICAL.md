# FMCG Sales Demand Forecasting & Performance Analytics using Databricks

## Project Overview

This project builds an end-to-end **Data & AI pipeline** for FMCG sales analytics using **Databricks, Apache Spark, Delta Lake, MLflow, and Databricks SQL dashboards**.

The project uses a public/synthetic FMCG transaction dataset covering sales activity from **2022 to 2024**. The dataset includes product, brand, category, channel, region, promotion, price, stock, delivery, and units sold information.

The goal is to transform raw FMCG sales records into business-ready analytics tables and train a demand prediction model that forecasts `units_sold` for SKU-level sales records.

## Business Problem

FMCG companies need to understand which products, brands, channels, regions, and promotion strategies drive sales performance. They also need a practical way to estimate product demand so that inventory, supply planning, and promotion decisions can be improved.

This project answers two business questions:

1. **Performance analytics:** Which products, brands, categories, channels, regions, and promotion types contribute most to revenue and sales volume?
2. **Demand prediction:** Can historical product, pricing, promotion, stock, date, channel, and region features be used to predict `units_sold`?

## Dataset

Expected raw CSV file:

```text
FMCG_2022_2024.csv
```

Columns:

| Column | Description |
|---|---|
| `date` | Sales date |
| `sku` | Product SKU |
| `brand` | Product brand |
| `segment` | Product segment |
| `category` | FMCG category |
| `channel` | Sales channel |
| `region` | Sales region |
| `pack_type` | Product packaging type |
| `price_unit` | Unit price |
| `promotion_flag` | 1 if promoted, 0 otherwise |
| `delivery_days` | Delivery lead time in days |
| `stock_available` | Available stock quantity |
| `delivered_qty` | Delivered quantity |
| `units_sold` | Units sold |

Derived metric:

```text
revenue = price_unit * units_sold
```

## Architecture

```text
Raw CSV
  ↓
Bronze Layer
  bronze_fmcg_sales_raw
  ↓
Silver Layer
  silver_fmcg_sales_cleaned
  ↓
Gold Star Schema
  dim_date
  dim_product
  dim_channel
  dim_region
  dim_promotion
  fact_sales
  ↓
Gold Semantic View
  vw_fact_sales_enriched
  ↓
Gold Analytics Tables
  gold_monthly_sales_trend
  gold_product_performance
  gold_brand_performance
  gold_category_performance
  gold_channel_performance
  gold_region_performance
  gold_promotion_impact
  gold_inventory_delivery_performance
  ↓
Feature Table
  gold_demand_forecasting_features
  ↓
MLflow Model Training
  Linear Regression / Random Forest / GBT Regressor
  ↓
Batch Inference
  gold_demand_forecast_predictions
  ↓
Dashboard & Insights
```

## Medallion Architecture

### Bronze Layer
Stores raw data from CSV as a Delta table with minimal transformation.

Table:

```text
bronze_fmcg_sales_raw
```

Purpose:

- Preserve raw records
- Add ingestion metadata
- Provide an auditable source of truth

### Silver Layer
Cleans and standardizes raw data.

Table:

```text
silver_fmcg_sales_cleaned
```

Transformations:

- Convert `date` into date type
- Cast numeric columns
- Normalize `promotion_flag`
- Create `revenue`
- Handle negative values by preserving original values and creating cleaned metric columns
- Add data quality flags
- Add date attributes such as year, quarter, month, and day of week

### Gold Layer
Creates a star schema and analytics-ready tables.

Tables:

```text
dim_date
dim_product
dim_channel
dim_region
dim_promotion
fact_sales
vw_fact_sales_enriched
gold_monthly_sales_trend
gold_product_performance
gold_brand_performance
gold_category_performance
gold_channel_performance
gold_region_performance
gold_promotion_impact
gold_inventory_delivery_performance
gold_demand_forecasting_features
gold_demand_forecast_predictions
```

## Machine Learning Task

### Objective
Predict `units_sold` for FMCG sales records using product, price, promotion, stock, date, channel, and region attributes.

### Target

```text
units_sold
```

### Features

Categorical features:

- `sku`
- `brand`
- `segment`
- `category`
- `channel`
- `region`
- `pack_type`
- `promotion_status`

Numeric features:

- `price_unit`
- `promotion_flag`
- `delivery_days`
- `stock_available`
- `year`
- `quarter`
- `month`
- `day_of_week`

### Models

- Linear Regression as baseline
- Random Forest Regressor as comparison
- GBT Regressor as comparison

### Evaluation Metrics

- RMSE
- MAE
- R²

### MLflow

MLflow is used to log:

- Parameters
- Metrics
- Feature list
- Spark ML pipeline models
- Model comparison results

## Dashboard Pages

### 1. Executive Sales Overview

Key metrics:

- Total revenue
- Total units sold
- Average selling price
- Unique SKUs
- Unique brands
- Average delivery days
- Monthly revenue trend

### 2. Product & Brand Performance

Visuals:

- Top SKUs by revenue
- Top brands by revenue
- Top categories by revenue
- Top pack types by revenue

### 3. Channel & Region Performance

Visuals:

- Revenue by channel
- Units sold by channel
- Revenue by region
- Channel-region performance matrix

### 4. Promotion Impact

Visuals:

- Revenue: promoted vs non-promoted
- Units sold: promoted vs non-promoted
- Average price by promotion status
- Promotion share of revenue

### 5. Inventory & Delivery Performance

Visuals:

- Average delivery days by region
- Stock availability by category
- Delivered quantity vs units sold
- Negative data quality flags

### 6. Demand Forecasting Results

Visuals:

- Actual vs predicted units sold
- Forecast error by category
- Top SKUs by predicted demand
- Prediction output table

## Repository Structure

```text
fmcg_demand_databricks_project/
├── README.md
├── config/
│   └── project_config.json
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── dashboard_design.md
│   └── business_insights.md
├── notebooks/
│   ├── 00_catalog_setup.py
│   ├── 01_data_understanding.py
│   ├── 02_bronze_ingestion.py
│   ├── 03_silver_processing.py
│   ├── 04_gold_star_schema.py
│   ├── 05_gold_analytics_tables.py
│   ├── 06_ml_feature_engineering.py
│   ├── 07_ml_demand_forecasting.py
│   ├── 08_batch_inference.py
│   └── 09_delta_lake_features.py
├── sql/
│   ├── 01_semantic_view.sql
│   ├── 02_dashboard_queries.sql
│   └── 03_quality_checks.sql
├── orchestration/
│   └── databricks_workflows.md
└── presentation/
    └── video_script_10_min.md
```

## How to Run

1. Upload `FMCG_2022_2024.csv` to a Databricks Volume or DBFS path.
2. Update notebook parameters:
   - `catalog_name`
   - `schema_name`
   - `input_path`
3. Run notebooks in this order:

```text
00_catalog_setup.py
01_data_understanding.py
02_bronze_ingestion.py
03_silver_processing.py
04_gold_star_schema.py
05_gold_analytics_tables.py
06_ml_feature_engineering.py
07_ml_demand_forecasting.py
08_batch_inference.py
09_delta_lake_features.py
```

4. Use `sql/02_dashboard_queries.sql` to create Databricks SQL dashboard tiles.
5. Use `orchestration/databricks_workflows.md` to set up Databricks Jobs.

## Business Impact

This project helps FMCG decision-makers:

- Identify top-performing products, brands, and categories
- Compare performance across sales channels and regions
- Evaluate promotion effectiveness
- Monitor stock, delivery, and fulfillment indicators
- Predict SKU-level units sold to support demand planning

## Limitations

- The dataset does not include customer-level data, so customer segmentation is not included.
- The dataset does not include cost or profit, so margin analysis is not performed.
- The demand model predicts observed `units_sold`, not unconstrained market demand.
- Promotion analysis is descriptive, not causal.
- Future predictions require future values for price, promotion, stock, channel, and product features.

## Future Improvements

- Add customer-level data for segmentation and retention analysis
- Add cost data for profit and margin analysis
- Add external calendar events or holidays for seasonality modeling
- Deploy model monitoring and scheduled retraining
- Add Databricks alerts for stock risk or demand spikes
