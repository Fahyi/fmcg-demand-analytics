# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Batch Inference
# MAGIC
# MAGIC This notebook loads the best MLflow run by lowest RMSE and scores the feature table.
# MAGIC Predictions are stored in the Gold layer as `gold_demand_forecast_predictions`.

# COMMAND ----------

import mlflow
import mlflow.spark
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "fmcg_demand_analytics", "Schema Name")
dbutils.widgets.text("experiment_name", "/Shared/FMCG_Demand_Forecasting", "MLflow Experiment Name")
dbutils.widgets.text("scoring_year", "2024", "Scoring Year")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
experiment_name = dbutils.widgets.get("experiment_name")
scoring_year = int(dbutils.widgets.get("scoring_year"))

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

client = mlflow.tracking.MlflowClient()
experiment = client.get_experiment_by_name(experiment_name)

if experiment is None:
    raise ValueError(f"Experiment not found: {experiment_name}. Run 07_ml_demand_forecasting.py first.")

runs = client.search_runs(
    experiment_ids=[experiment.experiment_id],
    filter_string="metrics.rmse IS NOT NULL",
    order_by=["metrics.rmse ASC"],
    max_results=1
)

if len(runs) == 0:
    raise ValueError("No MLflow runs with RMSE found. Run model training first.")

best_run = runs[0]
best_run_id = best_run.info.run_id
best_rmse = best_run.data.metrics.get("rmse")
best_mae = best_run.data.metrics.get("mae")
best_r2 = best_run.data.metrics.get("r2")
model_uri = f"runs:/{best_run_id}/model"

print(f"Best run ID: {best_run_id}")
print(f"Model URI  : {model_uri}")
print(f"RMSE       : {best_rmse}")

model = mlflow.spark.load_model(model_uri)

# COMMAND ----------

features = spark.table("gold_demand_forecasting_features")
score_df = features.filter(F.col("year") == scoring_year)

if score_df.count() == 0:
    print(f"No records found for scoring_year={scoring_year}. Scoring all records instead.")
    score_df = features

predictions = model.transform(score_df)

final_predictions = (
    predictions
    .select(
        "sales_record_id",
        "date",
        "year",
        "quarter",
        "month",
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
        F.col("units_sold").alias("actual_units_sold"),
        F.col("prediction").alias("predicted_units_sold")
    )
    .withColumn("predicted_units_sold", F.when(F.col("predicted_units_sold") < 0, 0).otherwise(F.col("predicted_units_sold")))
    .withColumn("prediction_error", F.col("actual_units_sold") - F.col("predicted_units_sold"))
    .withColumn("absolute_error", F.abs(F.col("prediction_error")))
    .withColumn("predicted_revenue", F.round(F.col("predicted_units_sold") * F.col("price_unit"), 2))
    .withColumn("model_run_id", F.lit(best_run_id))
    .withColumn("model_rmse", F.lit(float(best_rmse)))
    .withColumn("model_mae", F.lit(float(best_mae) if best_mae is not None else None))
    .withColumn("model_r2", F.lit(float(best_r2) if best_r2 is not None else None))
    .withColumn("inference_timestamp", F.current_timestamp())
)

# COMMAND ----------

(
    final_predictions.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_demand_forecast_predictions")
)

spark.sql("OPTIMIZE gold_demand_forecast_predictions ZORDER BY (date, sku, category, channel, region)")

print(f"✓ gold_demand_forecast_predictions rows: {spark.table('gold_demand_forecast_predictions').count():,}")
display(spark.table("gold_demand_forecast_predictions").limit(20))
