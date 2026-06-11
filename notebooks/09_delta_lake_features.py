# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Delta Lake Features Demonstration
# MAGIC
# MAGIC This notebook demonstrates how Delta Lake features are used in the project.
# MAGIC
# MAGIC Features:
# MAGIC - Delta table format
# MAGIC - ACID transactions
# MAGIC - Schema enforcement
# MAGIC - Time travel
# MAGIC - DESCRIBE HISTORY
# MAGIC - OPTIMIZE / ZORDER
# MAGIC - VACUUM dry run

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "fmcg_demand_analytics", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Confirm Delta Table Format

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Table History and Audit Trail

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY fact_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time Travel Example

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM fact_sales VERSION AS OF 0
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. OPTIMIZE and ZORDER

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE fact_sales ZORDER BY (date_id, product_id, channel_id, region_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. VACUUM Dry Run
# MAGIC
# MAGIC This previews files that would be deleted. It does not actually remove files.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM fact_sales DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Delta Lake supports the project by ensuring reliable writes, historical auditability, and optimized dashboard/ML queries.
