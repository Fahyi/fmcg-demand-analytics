# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Layer Ingestion
# MAGIC
# MAGIC This notebook loads the raw FMCG CSV file into a Bronze Delta table.
# MAGIC
# MAGIC Bronze design principles:
# MAGIC - Preserve raw records with minimal transformation
# MAGIC - Apply explicit schema for schema enforcement
# MAGIC - Add ingestion metadata for traceability
# MAGIC - Store as Delta for ACID transactions

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "fmcg_demand_analytics", "Schema Name")
dbutils.widgets.text("input_path", "/Volumes/main/fmcg_demand_analytics/raw_data/FMCG_2022_2024.csv", "Input CSV Path")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
input_path = dbutils.widgets.get("input_path")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"Target: {catalog_name}.{schema_name}")
print(f"Input : {input_path}")

# COMMAND ----------

schema_fmcg = StructType([
    StructField("date", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("category", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("region", StringType(), True),
    StructField("pack_type", StringType(), True),
    StructField("price_unit", DoubleType(), True),
    StructField("promotion_flag", IntegerType(), True),
    StructField("delivery_days", IntegerType(), True),
    StructField("stock_available", IntegerType(), True),
    StructField("delivered_qty", IntegerType(), True),
    StructField("units_sold", IntegerType(), True),
])

# COMMAND ----------

df_raw = (
    spark.read
    .option("header", True)
    .schema(schema_fmcg)
    .csv(input_path)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.lit(input_path))
)

print(f"Rows read: {df_raw.count():,}")
display(df_raw.limit(10))

# COMMAND ----------

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("bronze_fmcg_sales_raw")
)

row_count = spark.table("bronze_fmcg_sales_raw").count()
print(f"✓ bronze_fmcg_sales_raw created with {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Validation

# COMMAND ----------

spark.sql("DESCRIBE TABLE bronze_fmcg_sales_raw").show(truncate=False)
spark.sql("DESCRIBE HISTORY bronze_fmcg_sales_raw").show(truncate=False)
