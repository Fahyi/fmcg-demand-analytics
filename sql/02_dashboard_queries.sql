-- Dashboard Queries: FMCG Sales Demand Forecasting & Performance Analytics
-- Replace catalog/schema if needed.

USE CATALOG main;
USE SCHEMA fmcg_demand_analytics;

-- ==========================================================
-- PAGE 1: EXECUTIVE SALES OVERVIEW
-- ==========================================================

-- KPI: Total Revenue
SELECT ROUND(SUM(revenue), 2) AS total_revenue
FROM vw_fact_sales_enriched;

-- KPI: Total Units Sold
SELECT SUM(units_sold) AS total_units_sold
FROM vw_fact_sales_enriched;

-- KPI: Average Selling Price
SELECT ROUND(SUM(revenue) / NULLIF(SUM(units_sold), 0), 2) AS average_selling_price
FROM vw_fact_sales_enriched;

-- KPI: Unique SKUs
SELECT COUNT(DISTINCT sku) AS unique_skus
FROM vw_fact_sales_enriched;

-- KPI: Unique Brands
SELECT COUNT(DISTINCT brand) AS unique_brands
FROM vw_fact_sales_enriched;

-- KPI: Average Delivery Days
SELECT ROUND(AVG(delivery_days), 2) AS avg_delivery_days
FROM vw_fact_sales_enriched;

-- Line chart: Monthly Revenue Trend
SELECT
  year_month,
  total_revenue,
  total_units_sold,
  avg_price_unit,
  avg_delivery_days
FROM gold_monthly_sales_trend
ORDER BY year_month;

-- ==========================================================
-- PAGE 2: PRODUCT & BRAND PERFORMANCE
-- ==========================================================

-- Bar chart: Top SKUs by Revenue
SELECT
  sku,
  brand,
  category,
  pack_type,
  total_revenue,
  total_units_sold,
  avg_price_unit
FROM gold_product_performance
ORDER BY total_revenue DESC
LIMIT 20;

-- Bar chart: Top Brands by Revenue
SELECT
  brand,
  total_revenue,
  total_units_sold,
  unique_skus,
  unique_categories,
  avg_price_unit
FROM gold_brand_performance
ORDER BY total_revenue DESC;

-- Bar chart: Category Revenue
SELECT
  category,
  total_revenue,
  total_units_sold,
  unique_skus,
  avg_price_unit,
  avg_delivery_days
FROM gold_category_performance
ORDER BY total_revenue DESC;

-- Bar chart: Pack Type by Revenue
SELECT
  pack_type,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  SUM(total_units_sold) AS total_units_sold,
  ROUND(AVG(avg_price_unit), 2) AS avg_price_unit
FROM gold_product_performance
GROUP BY pack_type
ORDER BY total_revenue DESC;

-- ==========================================================
-- PAGE 3: CHANNEL & REGION PERFORMANCE
-- ==========================================================

-- Bar chart: Revenue by Channel
SELECT
  channel,
  total_revenue,
  total_units_sold,
  unique_skus,
  avg_price_unit,
  avg_delivery_days
FROM gold_channel_performance
ORDER BY total_revenue DESC;

-- Bar chart: Revenue by Region
SELECT
  region,
  total_revenue,
  total_units_sold,
  unique_skus,
  avg_delivery_days,
  avg_stock_available
FROM gold_region_performance
ORDER BY total_revenue DESC;

-- Heatmap/Table: Channel x Region Performance
SELECT
  channel,
  region,
  total_revenue,
  total_units_sold,
  avg_delivery_days,
  avg_stock_available
FROM gold_channel_region_performance
ORDER BY total_revenue DESC;

-- ==========================================================
-- PAGE 4: PROMOTION IMPACT
-- ==========================================================

-- Bar chart: Promotion Impact Summary
SELECT
  promotion_status,
  total_revenue,
  total_units_sold,
  avg_price_unit,
  avg_units_sold_per_record,
  record_count
FROM gold_promotion_impact
ORDER BY promotion_flag;

-- KPI/Table: Promotion Revenue Share
SELECT
  promotion_status,
  total_revenue,
  ROUND(total_revenue / NULLIF(SUM(total_revenue) OVER(), 0) * 100, 2) AS revenue_share_percent,
  total_units_sold,
  ROUND(total_units_sold / NULLIF(SUM(total_units_sold) OVER(), 0) * 100, 2) AS units_share_percent
FROM gold_promotion_impact
ORDER BY promotion_flag;

-- Category x Promotion comparison
SELECT
  category,
  promotion_status,
  ROUND(SUM(revenue), 2) AS total_revenue,
  SUM(units_sold) AS total_units_sold,
  ROUND(AVG(price_unit), 2) AS avg_price_unit,
  ROUND(AVG(units_sold), 2) AS avg_units_sold_per_record
FROM vw_fact_sales_enriched
GROUP BY category, promotion_status
ORDER BY category, promotion_status;

-- ==========================================================
-- PAGE 5: INVENTORY & DELIVERY PERFORMANCE
-- ==========================================================

-- Bar chart: Average Delivery Days by Region
SELECT
  region,
  ROUND(AVG(avg_delivery_days), 2) AS avg_delivery_days,
  ROUND(SUM(total_revenue), 2) AS total_revenue,
  SUM(total_units_sold) AS total_units_sold
FROM gold_inventory_delivery_performance
GROUP BY region
ORDER BY avg_delivery_days DESC;

-- Bar chart: Stock Availability by Category
SELECT
  category,
  SUM(total_stock_available) AS total_stock_available,
  SUM(total_units_sold) AS total_units_sold,
  ROUND(AVG(avg_sell_through_rate), 4) AS avg_sell_through_rate,
  SUM(quality_issue_rows) AS quality_issue_rows
FROM gold_inventory_delivery_performance
GROUP BY category
ORDER BY total_stock_available DESC;

-- Scatter plot: Delivered Quantity vs Units Sold
SELECT
  sku,
  category,
  channel,
  region,
  SUM(delivered_qty) AS total_delivered_qty,
  SUM(units_sold) AS total_units_sold,
  ROUND(SUM(revenue), 2) AS total_revenue
FROM vw_fact_sales_enriched
GROUP BY sku, category, channel, region
ORDER BY total_revenue DESC
LIMIT 500;

-- Table: Data Quality Summary
SELECT *
FROM gold_data_quality_summary;

-- ==========================================================
-- PAGE 6: DEMAND FORECASTING RESULTS
-- ==========================================================

-- KPI/Table: Model Output Summary
SELECT
  COUNT(*) AS scored_records,
  ROUND(AVG(absolute_error), 2) AS avg_absolute_error,
  ROUND(AVG(predicted_units_sold), 2) AS avg_predicted_units_sold,
  ROUND(AVG(actual_units_sold), 2) AS avg_actual_units_sold,
  MAX(model_rmse) AS model_rmse,
  MAX(model_mae) AS model_mae,
  MAX(model_r2) AS model_r2
FROM gold_demand_forecast_predictions;

-- Scatter plot: Actual vs Predicted Units Sold
SELECT
  sku,
  category,
  channel,
  region,
  actual_units_sold,
  predicted_units_sold,
  absolute_error
FROM gold_demand_forecast_predictions
LIMIT 1000;

-- Bar chart: Forecast Error by Category
SELECT
  category,
  ROUND(AVG(absolute_error), 2) AS avg_absolute_error,
  ROUND(AVG(predicted_units_sold), 2) AS avg_predicted_units_sold,
  ROUND(AVG(actual_units_sold), 2) AS avg_actual_units_sold,
  COUNT(*) AS scored_records
FROM gold_demand_forecast_predictions
GROUP BY category
ORDER BY avg_absolute_error DESC;

-- Bar chart: Top SKUs by Predicted Demand
SELECT
  sku,
  brand,
  category,
  ROUND(SUM(predicted_units_sold), 2) AS total_predicted_units,
  ROUND(SUM(predicted_revenue), 2) AS total_predicted_revenue,
  ROUND(AVG(absolute_error), 2) AS avg_absolute_error
FROM gold_demand_forecast_predictions
GROUP BY sku, brand, category
ORDER BY total_predicted_units DESC
LIMIT 20;
