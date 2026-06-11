# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — ML Feature Engineering
# MAGIC
# MAGIC This notebook creates the feature table for demand forecasting.
# MAGIC
# MAGIC Target:
# MAGIC - `units_sold`
# MAGIC
# MAGIC This model predicts observed SKU-level sales volume, not unconstrained demand.

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "fmcg_demand_analytics", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

df = spark.table("vw_fact_sales_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Selection
# MAGIC
# MAGIC We avoid using `delivered_qty` as a model feature because it may be known after operational fulfillment and can create leakage. The model uses features that are more likely to be available before or during planning: product, price, promotion, stock, date, channel, and region.

# COMMAND ----------

feature_df = (
    df.select(
        "sales_record_id",
        "date",
        "year",
        "quarter",
        "month",
        "day_of_week",
        "sku",
        "brand",
        "segment",
        "category",
        "pack_type",
        "channel",
        "region",
        "promotion_flag",
        "promotion_status",
        "price_unit",
        "delivery_days",
        "stock_available",
        "units_sold",
        "revenue"
    )
    .fillna({
        "sku": "Unknown",
        "brand": "Unknown",
        "segment": "Unknown",
        "category": "Unknown",
        "pack_type": "Unknown",
        "channel": "Unknown",
        "region": "Unknown",
        "promotion_status": "Unknown",
        "price_unit": 0.0,
        "delivery_days": 0,
        "stock_available": 0,
        "promotion_flag": 0,
        "units_sold": 0
    })
    .withColumn("log_stock_available", F.log1p(F.col("stock_available")))
    .withColumn("log_price_unit", F.log1p(F.col("price_unit")))
)

# COMMAND ----------

(
    feature_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_demand_forecasting_features")
)

spark.sql("OPTIMIZE gold_demand_forecasting_features ZORDER BY (date, sku, category, channel, region)")

print(f"✓ gold_demand_forecasting_features rows: {spark.table('gold_demand_forecasting_features').count():,}")
display(spark.table("gold_demand_forecasting_features").limit(10))
