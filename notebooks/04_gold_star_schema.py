# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold Star Schema
# MAGIC
# MAGIC This notebook transforms the Silver table into a Gold star schema.
# MAGIC
# MAGIC Outputs:
# MAGIC - `dim_date`
# MAGIC - `dim_product`
# MAGIC - `dim_channel`
# MAGIC - `dim_region`
# MAGIC - `dim_promotion`
# MAGIC - `fact_sales`
# MAGIC - `vw_fact_sales_enriched`

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "fmcg_demand_analytics", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Building Gold star schema in {catalog_name}.{schema_name}")

# COMMAND ----------

df = spark.table("silver_fmcg_sales_cleaned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Tables

# COMMAND ----------

dim_date = (
    df.select(
        "date_id",
        F.col("sales_date").alias("date"),
        "year",
        "quarter",
        "month",
        "month_name",
        "month_short_name",
        "year_month",
        "day_of_week",
        "day_name"
    )
    .dropDuplicates(["date_id"])
)

dim_product = (
    df.select("sku", "brand", "segment", "category", "pack_type")
    .dropDuplicates()
    .withColumn("product_id", F.sha2(F.concat_ws("||", "sku", "brand", "segment", "category", "pack_type"), 256))
    .select("product_id", "sku", "brand", "segment", "category", "pack_type")
)

dim_channel = (
    df.select("channel")
    .dropDuplicates()
    .withColumn("channel_id", F.sha2(F.col("channel"), 256))
    .select("channel_id", "channel")
)

dim_region = (
    df.select("region")
    .dropDuplicates()
    .withColumn("region_id", F.sha2(F.col("region"), 256))
    .select("region_id", "region")
)

dim_promotion = (
    df.select("promotion_flag")
    .dropDuplicates()
    .withColumn("promotion_id", F.col("promotion_flag"))
    .withColumn("promotion_status", F.when(F.col("promotion_flag") == 1, "Promotion").otherwise("Non-Promotion"))
    .select("promotion_id", "promotion_flag", "promotion_status")
)

# COMMAND ----------

for name, table_df in {
    "dim_date": dim_date,
    "dim_product": dim_product,
    "dim_channel": dim_channel,
    "dim_region": dim_region,
    "dim_promotion": dim_promotion,
}.items():
    (
        table_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(name)
    )
    print(f"✓ {name}: {spark.table(name).count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table

# COMMAND ----------

fact_sales = (
    df
    .withColumn("product_id", F.sha2(F.concat_ws("||", "sku", "brand", "segment", "category", "pack_type"), 256))
    .withColumn("channel_id", F.sha2(F.col("channel"), 256))
    .withColumn("region_id", F.sha2(F.col("region"), 256))
    .withColumn("promotion_id", F.col("promotion_flag"))
    .withColumn("sales_record_id", F.sha2(F.concat_ws("||", "sales_date", "sku", "brand", "segment", "category", "channel", "region", "pack_type", "price_unit_clean", "promotion_flag", "delivery_days", "stock_available", "delivered_qty", "units_sold"), 256))
    .select(
        "sales_record_id",
        "date_id",
        "product_id",
        "channel_id",
        "region_id",
        "promotion_id",
        F.col("price_unit_clean").alias("price_unit"),
        "delivery_days",
        F.col("stock_available_clean").alias("stock_available"),
        F.col("delivered_qty_clean").alias("delivered_qty"),
        F.col("units_sold_clean").alias("units_sold"),
        "revenue",
        "negative_stock_flag",
        "negative_delivered_flag",
        "negative_units_flag",
        "invalid_price_flag",
        "data_quality_issue_flag"
    )
)

(
    fact_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("fact_sales")
)

print(f"✓ fact_sales: {spark.table('fact_sales').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semantic View: vw_fact_sales_enriched
# MAGIC
# MAGIC This view joins `fact_sales` with all dimension tables. It acts as a business-friendly semantic layer for dashboards and feature engineering.

# COMMAND ----------

spark.sql("""
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
LEFT JOIN dim_promotion dpr ON fs.promotion_id = dpr.promotion_id
""")

print("✓ vw_fact_sales_enriched created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Gold Star Schema Tables

# COMMAND ----------

spark.sql("OPTIMIZE fact_sales ZORDER BY (date_id, product_id, channel_id, region_id)")
spark.sql("OPTIMIZE dim_product ZORDER BY (sku, category, brand)")
spark.sql("OPTIMIZE dim_date ZORDER BY (date_id)")

print("✓ Gold star schema optimized")

# COMMAND ----------

spark.sql("SELECT * FROM vw_fact_sales_enriched LIMIT 10").display()
