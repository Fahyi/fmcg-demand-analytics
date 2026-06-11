USE CATALOG main;
USE SCHEMA fmcg_demand_analytics;

-- Row counts across layers
SELECT 'bronze_fmcg_sales_raw' AS table_name, COUNT(*) AS row_count FROM bronze_fmcg_sales_raw
UNION ALL
SELECT 'silver_fmcg_sales_cleaned', COUNT(*) FROM silver_fmcg_sales_cleaned
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales;

-- Silver data quality issue summary
SELECT
  COUNT(*) AS total_rows,
  SUM(data_quality_issue_flag) AS rows_with_quality_issue,
  SUM(negative_stock_flag) AS negative_stock_rows,
  SUM(negative_delivered_flag) AS negative_delivered_rows,
  SUM(negative_units_flag) AS negative_units_rows,
  SUM(invalid_price_flag) AS invalid_price_rows
FROM silver_fmcg_sales_cleaned;

-- Revenue consistency check
SELECT
  ROUND(SUM(price_unit * units_sold), 2) AS recomputed_revenue,
  ROUND(SUM(revenue), 2) AS stored_revenue,
  ROUND(SUM(price_unit * units_sold) - SUM(revenue), 2) AS difference
FROM fact_sales;

-- Null checks in enriched view
SELECT
  SUM(CASE WHEN sku IS NULL THEN 1 ELSE 0 END) AS missing_sku,
  SUM(CASE WHEN channel IS NULL THEN 1 ELSE 0 END) AS missing_channel,
  SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS missing_region,
  SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS missing_category
FROM vw_fact_sales_enriched;
