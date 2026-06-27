# Spark & ML Evidence

**Owner:** Daniil Hontar  
**Role:** Data Processing & ML Engineer

## Screenshots

| File | Description |
|---|---|
| `spark_streams_running.png` | Spark Streaming job running — all 10 pipeline steps completed, `✓ ALL STREAMS RUNNING` confirmed |
| `mlflow_metrics.png` | XGBoost model training run in MLflow — ROC-AUC: 0.800, PR-AUC: 0.754, Precision: 0.967 |
| `postgres_fraud_metrics.png` | PostgreSQL `fraud_metrics` table — 119 real flagged transactions written by the pipeline |
| `delta_lake_layers.png` | Delta Lake Bronze / Silver / Gold layers on VM — parquet files and `_delta_log` folders present |
| `docker_containers.png` | Full Docker stack running — Kafka, Spark, PostgreSQL, Airflow, Grafana, MLflow all active |

## Pipeline Verified

The full Kafka → Spark Streaming → Delta Lake → PostgreSQL pipeline was verified end-to-end on the GCP VM (`fraud-detection-vm`). 119 real flagged transactions were written to `fraud_metrics` with an average fraud score of 0.73.
