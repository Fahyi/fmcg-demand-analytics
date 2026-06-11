# Databricks Workflows Orchestration

Use **Databricks Workflows (Jobs)** to automate the pipeline.

## Job Name

```text
fmcg-demand-forecasting-analytics-pipeline
```

## Recommended Task Flow

```text
Task 1: Catalog Setup
  ↓
Task 2: Data Understanding
  ↓
Task 3: Bronze Ingestion
  ↓
Task 4: Silver Processing
  ↓
Task 5: Gold Star Schema
  ↓
Task 6: Gold Analytics Tables
  ↓
Task 7: ML Feature Engineering
  ↓
Task 8: ML Demand Forecasting
  ↓
Task 9: Batch Inference
  ↓
Task 10: Delta Lake Features / Validation
```

## Task Details

| Task | Notebook | Depends On |
|---|---|---|
| 1 | `00_catalog_setup.py` | None |
| 2 | `01_data_understanding.py` | Task 1 |
| 3 | `02_bronze_ingestion.py` | Task 1 |
| 4 | `03_silver_processing.py` | Task 3 |
| 5 | `04_gold_star_schema.py` | Task 4 |
| 6 | `05_gold_analytics_tables.py` | Task 5 |
| 7 | `06_ml_feature_engineering.py` | Task 5 |
| 8 | `07_ml_demand_forecasting.py` | Task 7 |
| 9 | `08_batch_inference.py` | Task 8 |
| 10 | `09_delta_lake_features.py` | Task 6 and Task 9 |

## Parameters

Use the same parameters across notebooks:

| Parameter | Example |
|---|---|
| `catalog_name` | `main` |
| `schema_name` | `fmcg_demand_analytics` |
| `input_path` | `/Volumes/main/fmcg_demand_analytics/raw_data/FMCG_2022_2024.csv` |
| `experiment_name` | `/Shared/FMCG_Demand_Forecasting` |
| `scoring_year` | `2024` |

## Production Notes

For a production-like pipeline:

- Add a `processing_date` parameter.
- Process only incremental data for new dates.
- Add quality checks before Gold table refresh.
- Add alerts when quality issues exceed a threshold.
- Schedule the job daily, weekly, or monthly depending on business need.
- Add model retraining only when enough new data is available.
