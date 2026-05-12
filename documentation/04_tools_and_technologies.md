# Tools and Technologies

## 1. Full Stack Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Layer              │ Technology                │ Version   │ Role               │
├────────────────────┼───────────────────────────┼───────────┼────────────────────┤
│ Ingestion          │ Apache Kafka (Confluent)   │ 7.5       │ Message broker     │
│                    │ Apache ZooKeeper           │ 7.5       │ Kafka coordination │
│ Stream Processing  │ Apache Spark               │ 3.5       │ Micro-batch engine │
│                    │ Delta Lake                 │ 3.0       │ ACID table storage │
│ ML Training        │ XGBoost                    │ 2.0       │ Fraud classifier   │
│                    │ scikit-learn               │ 1.4       │ Preprocessing, DQ  │
│ ML Tracking        │ MLflow                     │ 2.10      │ Experiment tracking│
│ Orchestration      │ Apache Airflow             │ 2.8       │ DAG scheduler      │
│ Metadata Store     │ PostgreSQL                 │ 15        │ Metrics + DQ store │
│ Dashboards         │ Grafana                    │ 10.2      │ Live monitoring    │
│ Containerization   │ Docker + Docker Compose    │ v2        │ All services       │
│ Language           │ Python                     │ 3.11      │ All components     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Technology Deep Dives

### 2.1 Apache Kafka

**What it is**: A distributed, fault-tolerant, high-throughput message broker. Producers write events to _topics_; consumers read from topics with configurable offsets.

**Why it's used here**:
- Decouples the transaction generator from Spark — if Spark goes down, Kafka retains the events.
- Enables _replay_: Spark can reprocess from any offset for debugging or backfill.
- Scales horizontally with partitions.

**Configuration in this project**:
- 2 topics: `raw-transactions` (3 partitions), `flagged-transactions` (1 partition)
- Replication factor = 1 (single broker, dev setup)
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`

**Key Kafka concepts used**:
| Concept | Role |
|---|---|
| Topic | Logical event stream (one per data type) |
| Partition | Parallelism unit; Spark reads partitions concurrently |
| Consumer Group | Spark's streaming consumer group maintains offsets |
| Offset | Pointer to where each partition was last read |
| Retention | Events kept for 7 days by default |

**Alternatives considered**:
- **RabbitMQ**: Good for task queues but no event replay, no partitioned consumers.
- **Apache Pulsar**: Feature-equivalent but adds operational complexity.
- **AWS Kinesis / GCP Pub/Sub**: Managed alternatives for cloud; overkill for local dev.

---

### 2.2 Apache Spark Structured Streaming

**What it is**: A stream-processing engine built on Spark's DataFrame API. Treats a stream as an unbounded table, processing it in micro-batches (or continuously).

**Why it's used here**:
- Unified batch + streaming API — the same code style as a batch Spark job.
- `foreachBatch` enables arbitrary sink logic (Delta, Kafka, PostgreSQL) per micro-batch.
- Stateful aggregations (window functions) for velocity features.
- Mature ecosystem: Delta Lake, MLlib, PySpark all integrate natively.

**Configuration in this project**:
```python
query = (df
  .writeStream
  .foreachBatch(process_batch)
  .option("checkpointLocation", "/data/checkpoints/main")
  .trigger(processingTime="10 seconds")
  .start()
)
```

**Trigger options**:
| Trigger | Latency | Use Case |
|---|---|---|
| `processingTime="10 seconds"` | ~10s | This project (near-real-time) |
| `processingTime="0 seconds"` | Sub-second | Low-latency streaming |
| `once` | Batch | Backfill / one-shot |
| `availableNow` | Batch | Process all pending + stop |

**Alternatives considered**:
- **Apache Flink**: Lower latency, better stateful streaming but much steeper learning curve and less Python support.
- **Kafka Streams**: JVM-only, tight coupling to Kafka.
- **Spark Streaming (DStreams)**: Legacy API, replaced by Structured Streaming.

---

### 2.3 Delta Lake

**What it is**: An open-source storage layer that brings ACID transactions to Apache Spark and Parquet files.

**Why it's used here**:
- **ACID transactions**: Safe concurrent reads (Airflow DAG) + writes (Spark streaming) without corruption.
- **Time travel**: `df.read.format("delta").option("versionAsOf", 5)` — audit trail, replay.
- **Schema enforcement**: Rejects malformed records at write time.
- **Schema evolution**: `mergeSchema=true` allows adding columns without breaking readers.
- **OPTIMIZE + ZORDER**: Compact small files (a streaming problem) and co-locate similar data.

**Layer design**:
```
Bronze → Silver → Gold
  Raw     Features  Fraud only
          Scoring
```
This is the **Medallion Architecture** — a standard data lakehouse pattern.

**Alternatives considered**:
- **Apache Iceberg**: Functionally equivalent; better cross-engine support (Flink, Trino) but less Python-friendly.
- **Apache Hudi**: Strong upsert/CDC support but more complex setup.
- **Plain Parquet**: No ACID, no time travel — insufficient for concurrent streaming + batch reads.

---

### 2.4 XGBoost

**What it is**: Gradient-boosted decision tree library, widely considered the best off-the-shelf algorithm for tabular classification.

**Why it's used here**:
- Handles class imbalance with `scale_pos_weight = negative_samples / positive_samples`.
- Fast training and sub-millisecond inference per sample.
- Interpretable: native feature importance scores.
- Beats deep learning on tabular data (well-established benchmark result).

**Key hyperparameters**:
```python
XGBClassifier(
    n_estimators=300,      # Number of trees
    max_depth=6,           # Tree depth (controls overfitting)
    learning_rate=0.05,    # Step size shrinkage
    subsample=0.8,         # Row sampling (prevents overfitting)
    colsample_bytree=0.8,  # Feature sampling per tree
    scale_pos_weight=w,    # Class imbalance weight
    eval_metric="aucpr",   # Optimize for PR-AUC (imbalanced target)
    early_stopping_rounds=50
)
```

**Model features** (10 total):
`amount`, `log_amount`, `merchant_risk_score`, `flag_high_amount`, `flag_velocity`, `flag_off_hours`, `flag_geo_anomaly`, `flag_risky_merchant`, `is_online`, `event_hour`

**Alternatives considered**:
- **LightGBM**: Comparable performance, slightly faster training; less documented for beginners.
- **scikit-learn RandomForest**: Slower, weaker baseline; good for understanding.
- **Neural Network (PyTorch/TF)**: Overkill for tabular; harder to explain to compliance teams.
- **Isolation Forest**: Unsupervised anomaly detection — useful when no labels exist; we have labels.

---

### 2.5 MLflow

**What it is**: Open-source ML lifecycle management platform covering experiment tracking, model registry, and deployment.

**Why it's used here**:
- Tracks every training run: parameters, metrics, model artifact.
- Enables comparison across runs (which hyperparameters gave best AUC?).
- Model registry provides a promotion path: Staging → Production.
- Integrates with Airflow for automated retraining pipelines.

**What gets tracked per run**:
```
Parameters:  n_estimators, max_depth, learning_rate, subsample, ...
Metrics:     roc_auc, pr_auc, precision, recall, f1, feature_importances
Artifacts:   fraud_model.pkl, feature_names.txt
Tags:        data_source, training_rows, fraud_rate
```

**MLflow backend**: PostgreSQL (not the default SQLite — more reliable for concurrent Airflow access).

**Alternatives considered**:
- **Weights & Biases**: More feature-rich for deep learning but requires cloud account.
- **Neptune.ai**: Paid; overkill for educational project.
- **Manual CSV logging**: No versioning, no artifact management.

---

### 2.6 Apache Airflow

**What it is**: A workflow orchestration platform for scheduling, monitoring, and managing data pipelines as DAGs (Directed Acyclic Graphs).

**Why it's used here**:
- Schedule daily model retraining and hourly DQ checks.
- Handles task dependencies, retries, and failure alerting natively.
- Industry standard — widely used in data engineering.
- `BranchPythonOperator` enables conditional retraining logic.

**Executor**: LocalExecutor (single-machine; sufficient for this project).

**DAGs in this project**:
| DAG | Schedule | Purpose |
|---|---|---|
| `fraud_detection_daily_dag` | `0 0 * * *` (midnight UTC) | Retrain + evaluate + promote Gold |
| `data_quality_monitoring_dag` | `0 * * * *` (every hour) | 4 DQ checks → PostgreSQL |

**Alternatives considered**:
- **Prefect**: Modern, Python-native API but less industry adoption.
- **Dagster**: Better asset-centric model but heavier setup.
- **Cron + Python scripts**: No dependency management, no retry, no UI.
- **Luigi**: Older; replaced by Airflow in most shops.

---

### 2.7 PostgreSQL

**What it is**: Open-source relational database.

**Why it's used here**:
- Stores Airflow metadata, MLflow run data, `fraud_metrics`, and `dq_checks`.
- Grafana reads from it directly — no additional connector needed.
- ACID-compliant for reliable metric writes from concurrent Spark batches.

**Tables in this project**:
```sql
fraud_metrics   -- written by Spark Gold layer every 10s
dq_checks       -- written by Airflow hourly DQ DAG
(+ Airflow and MLflow internal tables)
```

**Alternatives considered**:
- **MySQL**: Equally valid; PostgreSQL has better JSON support and is more common in data engineering.
- **SQLite**: Not safe for concurrent writes from multiple containers.
- **InfluxDB**: Better for time-series but adds operational complexity.

---

### 2.8 Grafana

**What it is**: Open-source analytics and monitoring platform with a rich dashboard UI.

**Why it's used here**:
- Auto-provisions dashboards from JSON — no manual clicking after `make up`.
- Native PostgreSQL data source.
- Flexible panel types: time-series, stat, table, bar chart.

**Dashboard panels**:
1. Total flagged transactions (last 3h) — Stat panel
2. Average flagged amount — Stat panel
3. Max single fraud amount — Stat panel
4. DQ check pass rate (last 24h) — Gauge panel
5. Flagged count + avg amount per minute — Time-series
6. DQ check log (last 20 entries) — Table
7. Fraud events per micro-batch (last 50) — Bar chart

**Alternatives considered**:
- **Apache Superset**: More SQL-oriented; heavier setup.
- **Kibana**: Requires ELK stack.
- **Metabase**: Simpler but less real-time capability.

---

## 3. Python Libraries

### Producer
| Library | Version | Purpose |
|---|---|---|
| `confluent-kafka` | 2.3 | Kafka producer client |
| `pandas` | 2.1 | CSV reading and row replay |
| `faker` | 21.0 | Synthetic transaction generation (fallback) |

### Spark Jobs
| Library | Version | Purpose |
|---|---|---|
| `pyspark` | 3.5 | Spark DataFrame + Streaming API |
| `delta-spark` | 3.0 | Delta Lake integration |
| `xgboost` | 2.0 | ML model inference |
| `scikit-learn` | 1.4 | Feature preprocessing |
| `psycopg2-binary` | 2.9 | PostgreSQL writes |
| `confluent-kafka` | 2.3 | Kafka sink (flagged transactions) |

### ML Training
| Library | Version | Purpose |
|---|---|---|
| `xgboost` | 2.0 | Model training |
| `scikit-learn` | 1.4 | Train/test split, metrics |
| `mlflow` | 2.10 | Experiment tracking |
| `pandas` | 2.1 | Data loading / manipulation |
| `numpy` | 1.26 | Numerical operations |
| `matplotlib` | 3.8 | Feature importance plots |

---

## 4. Infrastructure and DevOps

### Docker Compose Services (13 total)

| Service | Image | Purpose |
|---|---|---|
| `zookeeper` | confluentinc/cp-zookeeper:7.5.0 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.5.0 | Message broker |
| `kafka-ui` | provectuslabs/kafka-ui:latest | Kafka topic browser |
| `spark-master` | bitnami/spark:3.5 | Spark cluster master |
| `spark-worker` | bitnami/spark:3.5 | Spark executor |
| `spark-streaming` | bitnami/spark:3.5 | Runs fraud_streaming_job.py |
| `transaction-producer` | Custom (producer/Dockerfile) | CSV replay producer |
| `ml-trainer` | Custom (ml/) | XGBoost training |
| `postgres` | postgres:15 | Metadata + metrics DB |
| `airflow-init` | apache/airflow:2.8.0 | DB init + admin user creation |
| `airflow-webserver` | apache/airflow:2.8.0 | Airflow UI |
| `airflow-scheduler` | apache/airflow:2.8.0 | DAG scheduling |
| `mlflow` | Custom (mlflow) | Experiment tracking server |
| `grafana` | grafana/grafana:10.2.0 | Dashboards |

### Shared Docker Volumes

| Volume | Mounted To | Purpose |
|---|---|---|
| `delta-data` | `/data/delta` | Bronze/Silver/Gold Delta tables |
| `models-data` | `/models` | Trained ML models |
| `postgres-data` | PostgreSQL data dir | Persistent DB |
| `grafana-data` | Grafana data dir | Persisted dashboards |

---

## 5. Dependency Map

```
transaction-producer
    └── confluent-kafka  ──►  Kafka  ◄──  spark-streaming
                                              ├── delta-spark  ──►  delta-data volume
                                              ├── xgboost      ◄──  models-data volume
                                              └── psycopg2     ──►  postgres

ml-trainer
    ├── xgboost
    ├── mlflow           ──►  mlflow-server  ──►  postgres
    └── pandas           ◄──  delta-data volume (Silver)
         └── saves fraud_model.pkl  ──►  models-data volume

airflow-scheduler
    ├── SparkSubmitOperator  ──►  spark-master
    ├── PythonOperator       ──►  postgres (DQ results)
    └── HttpOperator         ──►  Teams webhook (optional)

grafana  ──►  postgres  (fraud_metrics, dq_checks)
```

---

## 6. Development Environment Requirements

| Requirement | Minimum | Recommended |
|---|---|---|
| RAM (Docker) | 8 GB | 12 GB |
| CPU | 4 cores | 8 cores |
| Disk | 10 GB | 20 GB |
| Docker Desktop | 4.x | Latest |
| Python (local) | 3.9 | 3.11 |
| Ports | 3000, 5001, 5432, 7077, 8080–8082, 9092 | — |
