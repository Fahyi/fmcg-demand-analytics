# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver Layer Processing
# MAGIC
# MAGIC This notebook cleans and standardizes the Bronze FMCG sales data.
# MAGIC
# MAGIC Silver layer outputs:
# MAGIC - `silver_fmcg_sales_cleaned`
# MAGIC
# MAGIC Main transformations:
# MAGIC - Date parsing
# MAGIC - Numeric validation
# MAGIC - Negative value handling with audit flags
# MAGIC - Derived revenue and date attributes
# MAGIC - Consistent naming and clean business fields

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "fmcg_demand_analytics", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Processing Silver layer in {catalog_name}.{schema_name}")

# COMMAND ----------

df = spark.table("bronze_fmcg_sales_raw")

# COMMAND ----------

df_silver = (
    df
    .withColumn("sales_date", F.to_date("date", "yyyy-MM-dd"))
    .withColumn("sku", F.trim(F.col("sku")))
    .withColumn("brand", F.trim(F.col("brand")))
    .withColumn("segment", F.trim(F.col("segment")))
    .withColumn("category", F.trim(F.col("category")))
    .withColumn("channel", F.trim(F.col("channel")))
    .withColumn("region", F.trim(F.col("region")))
    .withColumn("pack_type", F.trim(F.col("pack_type")))
    .withColumn("promotion_flag", F.when(F.col("promotion_flag") == 1, 1).otherwise(0))
    # Quality flags
    .withColumn("negative_stock_flag", F.when(F.col("stock_available") < 0, 1).otherwise(0))
    .withColumn("negative_delivered_flag", F.when(F.col("delivered_qty") < 0, 1).otherwise(0))
    .withColumn("negative_units_flag", F.when(F.col("units_sold") < 0, 1).otherwise(0))
    .withColumn("invalid_price_flag", F.when((F.col("price_unit").isNull()) | (F.col("price_unit") <= 0), 1).otherwise(0))
    .withColumn("invalid_date_flag", F.when(F.col("sales_date").isNull(), 1).otherwise(0))
    # Cleaned business metrics, preserving raw fields above
    .withColumn("stock_available_clean", F.greatest(F.col("stock_available"), F.lit(0)))
    .withColumn("delivered_qty_clean", F.greatest(F.col("delivered_qty"), F.lit(0)))
    .withColumn("units_sold_clean", F.greatest(F.col("units_sold"), F.lit(0)))
    .withColumn("price_unit_clean", F.when(F.col("price_unit") > 0, F.col("price_unit")).otherwise(F.lit(None)))
    .withColumn("revenue", F.round(F.col("price_unit_clean") * F.col("units_sold_clean"), 2))
    # Date attributes
    .withColumn("date_id", F.date_format("sales_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("sales_date"))
    .withColumn("quarter", F.quarter("sales_date"))
    .withColumn("month", F.month("sales_date"))
    .withColumn("month_name", F.date_format("sales_date", "MMMM"))
    .withColumn("month_short_name", F.date_format("sales_date", "MMM"))
    .withColumn("year_month", F.date_format("sales_date", "yyyy-MM"))
    .withColumn("day_of_week", F.dayofweek("sales_date"))
    .withColumn("day_name", F.date_format("sales_date", "EEEE"))
    .withColumn(
        "data_quality_issue_flag",
        F.when(
            (F.col("negative_stock_flag") == 1) |
            (F.col("negative_delivered_flag") == 1) |
            (F.col("negative_units_flag") == 1) |
            (F.col("invalid_price_flag") == 1) |
            (F.col("invalid_date_flag") == 1), 1
        ).otherwise(0)
    )
    .drop("ingestion_timestamp", "source_file")
    .dropDuplicates()
)

# Filter out rows without a valid date or valid price after flags are created.
# Original issue count remains auditable in profiling tables.
df_silver_valid = df_silver.filter(
    (F.col("sales_date").isNotNull()) &
    (F.col("price_unit_clean").isNotNull())
)

# COMMAND ----------

(
    df_silver_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver_fmcg_sales_cleaned")
)

print(f"✓ silver_fmcg_sales_cleaned rows: {spark.table('silver_fmcg_sales_cleaned').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Silver Table

# COMMAND ----------

spark.sql("OPTIMIZE silver_fmcg_sales_cleaned ZORDER BY (sales_date, sku, category, channel, region)")
print("✓ silver_fmcg_sales_cleaned optimized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Validation

# COMMAND ----------

spark.sql("""
SELECT
  COUNT(*) AS total_rows,
  SUM(data_quality_issue_flag) AS rows_with_quality_issue,
  SUM(negative_stock_flag) AS rows_negative_stock,
  SUM(negative_delivered_flag) AS rows_negative_delivered,
  SUM(negative_units_flag) AS rows_negative_units,
  ROUND(SUM(revenue), 2) AS total_revenue,
  SUM(units_sold_clean) AS total_units_sold
FROM silver_fmcg_sales_cleaned
""").show(truncate=False)

spark.sql("""
SELECT category, COUNT(*) AS rows, ROUND(SUM(revenue), 2) AS revenue
FROM silver_fmcg_sales_cleaned
GROUP BY category
ORDER BY revenue DESC
""").show(truncate=False)
