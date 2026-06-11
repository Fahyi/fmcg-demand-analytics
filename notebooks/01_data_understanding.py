# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Data Understanding
# MAGIC
# MAGIC This notebook profiles the raw FMCG dataset before transformation.
# MAGIC
# MAGIC Objectives:
# MAGIC - Inspect schema and sample rows
# MAGIC - Count records and distinct business entities
# MAGIC - Identify missing, negative, or inconsistent values
# MAGIC - Document transformation requirements for the Silver layer

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("input_path", "/Volumes/main/fmcg_demand_analytics/raw_data/FMCG_2022_2024.csv", "Input CSV Path")
input_path = dbutils.widgets.get("input_path")

print(f"Input path: {input_path}")

# COMMAND ----------

df = spark.read.csv(input_path, header=True, inferSchema=True)

print(f"Rows    : {df.count():,}")
print(f"Columns : {len(df.columns)}")
df.printSchema()

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entity Cardinality

# COMMAND ----------

cardinality_cols = ["sku", "brand", "segment", "category", "channel", "region", "pack_type", "promotion_flag"]

cardinality_exprs = [F.countDistinct(F.col(c)).alias(c) for c in cardinality_cols]
display(df.select(cardinality_exprs))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Range

# COMMAND ----------

summary_date = df.select(
    F.min("date").alias("min_date"),
    F.max("date").alias("max_date"),
    F.countDistinct("date").alias("distinct_dates")
)
display(summary_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null Counts

# COMMAND ----------

null_counts = df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns
])
display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Negative Value Checks
# MAGIC
# MAGIC Negative values in stock, delivered quantity, or units sold can appear in operational datasets because of returns, corrections, or data quality issues. The Silver layer keeps quality flags and creates cleaned metric columns.

# COMMAND ----------

numeric_cols = ["price_unit", "promotion_flag", "delivery_days", "stock_available", "delivered_qty", "units_sold"]

negative_checks = []
for c in numeric_cols:
    negative_checks.append(F.sum(F.when(F.col(c) < 0, 1).otherwise(0)).alias(f"negative_{c}"))

display(df.select(negative_checks))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribution Summary

# COMMAND ----------

display(df.describe(numeric_cols))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Observations
# MAGIC
# MAGIC - Revenue is not available directly and must be derived as `price_unit * units_sold`.
# MAGIC - The dataset does not contain customer identifiers, so customer segmentation is out of scope.
# MAGIC - The dataset is suitable for product, brand, category, channel, region, promotion, inventory, delivery, and demand prediction analysis.
# MAGIC - Negative quantity values should be handled transparently using clean columns and quality flags.
