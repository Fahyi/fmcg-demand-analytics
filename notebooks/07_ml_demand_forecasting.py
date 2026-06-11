# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — ML Demand Forecasting
# MAGIC
# MAGIC This notebook trains and evaluates regression models to predict `units_sold`.
# MAGIC
# MAGIC Models:
# MAGIC - Linear Regression baseline
# MAGIC - Random Forest Regressor
# MAGIC - GBT Regressor
# MAGIC
# MAGIC Metrics:
# MAGIC - RMSE
# MAGIC - MAE
# MAGIC - R²
# MAGIC
# MAGIC MLflow tracks all runs and model artifacts.

# COMMAND ----------

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "fmcg_demand_analytics", "Schema Name")
dbutils.widgets.text("experiment_name", "/Shared/FMCG_Demand_Forecasting", "MLflow Experiment Name")

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
experiment_name = dbutils.widgets.get("experiment_name")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

mlflow.set_experiment(experiment_name)
print(f"MLflow experiment: {experiment_name}")

# COMMAND ----------

df = spark.table("gold_demand_forecasting_features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-Based Train/Test Split
# MAGIC
# MAGIC To avoid using future information to predict the past, the split is based on year:
# MAGIC - Train: 2022–2023
# MAGIC - Test: 2024

# COMMAND ----------

train_df = df.filter(F.col("year") < 2024)
test_df = df.filter(F.col("year") == 2024)

# Fallback if the dataset does not contain the expected years
if train_df.count() == 0 or test_df.count() == 0:
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

print(f"Train rows: {train_df.count():,}")
print(f"Test rows : {test_df.count():,}")

# COMMAND ----------

categorical_cols = [
    "sku", "brand", "segment", "category", "pack_type",
    "channel", "region", "promotion_status"
]

numeric_cols = [
    "price_unit", "log_price_unit", "promotion_flag", "delivery_days",
    "stock_available", "log_stock_available", "year", "quarter", "month", "day_of_week"
]

target_col = "units_sold"

indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in categorical_cols
]

encoders = [
    OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_ohe", handleInvalid="keep")
    for c in categorical_cols
]

feature_cols = [f"{c}_ohe" for c in categorical_cols] + numeric_cols

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="keep")

# COMMAND ----------

def evaluate_regression(predictions):
    evaluator_rmse = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")
    evaluator_mae = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="mae")
    evaluator_r2 = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="r2")
    return {
        "rmse": evaluator_rmse.evaluate(predictions),
        "mae": evaluator_mae.evaluate(predictions),
        "r2": evaluator_r2.evaluate(predictions),
    }


def train_log_model(model_name, estimator, params):
    pipeline = Pipeline(stages=indexers + encoders + [assembler, estimator])

    with mlflow.start_run(run_name=model_name):
        model = pipeline.fit(train_df)
        predictions = model.transform(test_df)
        metrics = evaluate_regression(predictions)

        for k, v in params.items():
            mlflow.log_param(k, v)
        mlflow.log_param("target", target_col)
        mlflow.log_param("categorical_features", ",".join(categorical_cols))
        mlflow.log_param("numeric_features", ",".join(numeric_cols))
        mlflow.log_param("split_strategy", "time_based_train_2022_2023_test_2024")

        for k, v in metrics.items():
            mlflow.log_metric(k, v)

        mlflow.spark.log_model(model, artifact_path="model")

        print(f"{model_name}")
        print(f"  RMSE: {metrics['rmse']:.4f}")
        print(f"  MAE : {metrics['mae']:.4f}")
        print(f"  R²  : {metrics['r2']:.4f}")

        return model, metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 1: Linear Regression Baseline

# COMMAND ----------

lr = LinearRegression(labelCol=target_col, featuresCol="features", maxIter=50, regParam=0.05, elasticNetParam=0.0)
lr_model, lr_metrics = train_log_model(
    "Linear Regression Baseline",
    lr,
    {"model_type": "LinearRegression", "maxIter": 50, "regParam": 0.05}
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 2: Random Forest Regressor

# COMMAND ----------

rf = RandomForestRegressor(labelCol=target_col, featuresCol="features", numTrees=80, maxDepth=8, seed=42)
rf_model, rf_metrics = train_log_model(
    "Random Forest Regressor",
    rf,
    {"model_type": "RandomForestRegressor", "numTrees": 80, "maxDepth": 8}
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model 3: GBT Regressor

# COMMAND ----------

gbt = GBTRegressor(labelCol=target_col, featuresCol="features", maxIter=60, maxDepth=5, seed=42)
gbt_model, gbt_metrics = train_log_model(
    "GBT Regressor",
    gbt,
    {"model_type": "GBTRegressor", "maxIter": 60, "maxDepth": 5}
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Comparison

# COMMAND ----------

comparison = [
    ("Linear Regression", lr_metrics["rmse"], lr_metrics["mae"], lr_metrics["r2"]),
    ("Random Forest", rf_metrics["rmse"], rf_metrics["mae"], rf_metrics["r2"]),
    ("GBT", gbt_metrics["rmse"], gbt_metrics["mae"], gbt_metrics["r2"]),
]

comparison_df = spark.createDataFrame(comparison, ["model", "rmse", "mae", "r2"])
display(comparison_df.orderBy("rmse"))

best = min(comparison, key=lambda x: x[1])
print(f"Best model by RMSE: {best[0]} (RMSE={best[1]:.4f})")
print("Open MLflow UI to inspect runs and model artifacts.")
