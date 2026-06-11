# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold Analytics Tables
# MAGIC
# MAGIC This notebook creates analytics-ready Gold tables from the enriched semantic view.
# MAGIC These tables are optimized for Databricks SQL dashboards.

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "fmcg_demand_analytics", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Creating Gold analytics tables in {catalog_name}.{schema_name}")

# COMMAND ----------

df = spark.table("vw_fact_sales_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Monthly Sales Trend

# COMMAND ----------

gold_monthly = (
    df.groupBy("year_month", "year", "month")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("units_sold").alias("total_units_sold"),
        F.countDistinct("sku").alias("unique_skus"),
        F.round(F.avg("price_unit"), 2).alias("avg_price_unit"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days")
    )
    .orderBy("year_month")
)

gold_monthly.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_monthly_sales_trend")
print("✓ gold_monthly_sales_trend")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Product Performance

# COMMAND ----------

gold_product = (
    df.groupBy("sku", "brand", "segment", "category", "pack_type")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("units_sold").alias("total_units_sold"),
        F.round(F.avg("price_unit"), 2).alias("avg_price_unit"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.round(F.avg("sell_through_rate"), 4).alias("avg_sell_through_rate"),
        F.count("sales_record_id").alias("record_count")
    )
)

gold_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_product_performance")
print("✓ gold_product_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Brand Performance

# COMMAND ----------

gold_brand = (
    df.groupBy("brand")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("units_sold").alias("total_units_sold"),
        F.countDistinct("sku").alias("unique_skus"),
        F.countDistinct("category").alias("unique_categories"),
        F.round(F.avg("price_unit"), 2).alias("avg_price_unit")
    )
)

gold_brand.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_brand_performance")
print("✓ gold_brand_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Category Performance

# COMMAND ----------

gold_category = (
    df.groupBy("category")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("units_sold").alias("total_units_sold"),
        F.countDistinct("sku").alias("unique_skus"),
        F.round(F.avg("price_unit"), 2).alias("avg_price_unit"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.round(F.avg("sell_through_rate"), 4).alias("avg_sell_through_rate")
    )
)

gold_category.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_category_performance")
print("✓ gold_category_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Channel Performance

# COMMAND ----------

gold_channel = (
    df.groupBy("channel")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("units_sold").alias("total_units_sold"),
        F.countDistinct("sku").alias("unique_skus"),
        F.round(F.avg("price_unit"), 2).alias("avg_price_unit"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days")
    )
)

gold_channel.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_channel_performance")
print("✓ gold_channel_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Region Performance

# COMMAND ----------

gold_region = (
    df.groupBy("region")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("units_sold").alias("total_units_sold"),
        F.countDistinct("sku").alias("unique_skus"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.round(F.avg("stock_available"), 2).alias("avg_stock_available")
    )
)

gold_region.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_region_performance")
print("✓ gold_region_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Channel × Region Performance

# COMMAND ----------

gold_channel_region = (
    df.groupBy("channel", "region")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("units_sold").alias("total_units_sold"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.round(F.avg("stock_available"), 2).alias("avg_stock_available")
    )
)

gold_channel_region.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_channel_region_performance")
print("✓ gold_channel_region_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Promotion Impact

# COMMAND ----------

gold_promo = (
    df.groupBy("promotion_flag", "promotion_status")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("units_sold").alias("total_units_sold"),
        F.round(F.avg("price_unit"), 2).alias("avg_price_unit"),
        F.round(F.avg("units_sold"), 2).alias("avg_units_sold_per_record"),
        F.count("sales_record_id").alias("record_count")
    )
)

gold_promo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_promotion_impact")
print("✓ gold_promotion_impact")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Inventory & Delivery Performance

# COMMAND ----------

gold_inventory = (
    df.groupBy("category", "region")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.sum("units_sold").alias("total_units_sold"),
        F.sum("stock_available").alias("total_stock_available"),
        F.sum("delivered_qty").alias("total_delivered_qty"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.round(F.avg("sell_through_rate"), 4).alias("avg_sell_through_rate"),
        F.sum("data_quality_issue_flag").alias("quality_issue_rows")
    )
)

gold_inventory.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_inventory_delivery_performance")
print("✓ gold_inventory_delivery_performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Quality Summary

# COMMAND ----------

gold_quality = (
    df.agg(
        F.count("*").alias("total_records"),
        F.sum("data_quality_issue_flag").alias("records_with_quality_issue"),
        F.sum("negative_stock_flag").alias("negative_stock_records"),
        F.sum("negative_delivered_flag").alias("negative_delivered_records"),
        F.sum("negative_units_flag").alias("negative_units_records"),
        F.sum("invalid_price_flag").alias("invalid_price_records")
    )
)

gold_quality.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_data_quality_summary")
print("✓ gold_data_quality_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Gold Analytics Tables

# COMMAND ----------

for table, zorder_cols in {
    "gold_monthly_sales_trend": "year_month",
    "gold_product_performance": "sku, category, brand",
    "gold_brand_performance": "brand",
    "gold_category_performance": "category",
    "gold_channel_performance": "channel",
    "gold_region_performance": "region",
    "gold_promotion_impact": "promotion_flag",
    "gold_inventory_delivery_performance": "category, region"
}.items():
    try:
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({zorder_cols})")
        print(f"✓ Optimized {table}")
    except Exception as e:
        print(f"Could not optimize {table}: {e}")

print("Gold analytics tables complete.")
