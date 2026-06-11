-- Semantic view for FMCG Sales Analytics
-- Run after fact_sales and dimension tables are created.

USE CATALOG main;
USE SCHEMA fmcg_demand_analytics;

CREATE OR REPLACE VIEW vw_fact_sales_enriched AS
SELECT
    fs.sales_record_id,
    fs.date_id,
    dd.date,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.month_short_name,
    dd.year_month,
    dd.day_of_week,
    dd.day_name,

    dp.product_id,
    dp.sku,
    dp.brand,
    dp.segment,
    dp.category,
    dp.pack_type,

    dc.channel_id,
    dc.channel,

    dr.region_id,
    dr.region,

    dpr.promotion_id,
    dpr.promotion_flag,
    dpr.promotion_status,

    fs.price_unit,
    fs.delivery_days,
    fs.stock_available,
    fs.delivered_qty,
    fs.units_sold,
    fs.revenue,

    CASE WHEN fs.units_sold > 0 THEN ROUND(fs.revenue / fs.units_sold, 2) ELSE NULL END AS avg_selling_price,
    CASE WHEN fs.stock_available > 0 THEN ROUND(fs.units_sold / fs.stock_available, 4) ELSE NULL END AS sell_through_rate,
    fs.delivered_qty - fs.units_sold AS delivery_gap,

    fs.negative_stock_flag,
    fs.negative_delivered_flag,
    fs.negative_units_flag,
    fs.invalid_price_flag,
    fs.data_quality_issue_flag

FROM fact_sales fs
LEFT JOIN dim_date dd ON fs.date_id = dd.date_id
LEFT JOIN dim_product dp ON fs.product_id = dp.product_id
LEFT JOIN dim_channel dc ON fs.channel_id = dc.channel_id
LEFT JOIN dim_region dr ON fs.region_id = dr.region_id
LEFT JOIN dim_promotion dpr ON fs.promotion_id = dpr.promotion_id;

SELECT * FROM vw_fact_sales_enriched LIMIT 20;
