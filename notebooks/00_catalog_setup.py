# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Catalog & Schema Setup
# MAGIC
# MAGIC This notebook initializes the Databricks workspace objects used by the FMCG demand forecasting project.
# MAGIC
# MAGIC **Creates:**
# MAGIC - Catalog if using Unity Catalog
# MAGIC - Schema/database
# MAGIC - Optional raw data volume path for Unity Catalog environments
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `catalog_name`
# MAGIC - `schema_name`

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "fmcg_demand_analytics", "Schema Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Catalog: {catalog_name}")
print(f"Schema : {schema_name}")

# COMMAND ----------

if catalog_name != "hive_metastore":
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    spark.sql(f"USE SCHEMA {schema_name}")
    print(f"Unity Catalog ready: {catalog_name}.{schema_name}")
else:
    spark.sql("USE CATALOG hive_metastore")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")
    spark.sql(f"USE {schema_name}")
    print(f"Hive metastore database ready: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Create Volume for Raw Data
# MAGIC
# MAGIC If you are using Unity Catalog, upload `FMCG_2022_2024.csv` to:
# MAGIC
# MAGIC ```text
# MAGIC /Volumes/<catalog_name>/<schema_name>/raw_data/FMCG_2022_2024.csv
# MAGIC ```

# COMMAND ----------

if catalog_name != "hive_metastore":
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.raw_data")
        print(f"Volume ready: /Volumes/{catalog_name}/{schema_name}/raw_data")
    except Exception as e:
        print(f"Volume creation skipped or not permitted: {e}")

print("Catalog and schema setup complete.")
