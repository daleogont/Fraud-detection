# System Design: Real-Time Financial Fraud Detection

## 1. High-Level Architecture

The system follows a **Lambda-like architecture** with a real-time streaming path and a batch retraining loop, unified through a shared Delta Lake storage layer.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                                   │
│  synthetic_fraud_dataset.csv  ──►  csv_to_kafka.py (chunk-based Kafka Producer) │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │  raw-transactions (Kafka topic)
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│  STREAMING LAYER  (Apache Spark Structured Streaming, 10s micro-batch)          │
│                                                                                 │
│   ┌─────────────┐    ┌─────────────────────┐    ┌──────────────────────────┐   │
│   │   BRONZE    │    │       SILVER         │    │          GOLD            │   │
│   │ Raw events  │───►│ Features + Scoring   │───►│ Flagged Transactions     │   │
│   │ schema-     │    │ rule_score           │    │ (fraud_score ≥ 0.35)     │   │
│   │ enforced    │    │ ml_score             │    │                          │   │
│   │ append-only │    │ fraud_score          │    │──► Kafka (flagged-txns)  │   │
│   └─────────────┘    │ is_flagged           │    │──► PostgreSQL            │   │
│   Delta Lake         └─────────────────────┘    └──────────────────────────┘   │
│                      Delta Lake                  Delta Lake                     │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│  ORCHESTRATION LAYER  (Apache Airflow 2.8)                                      │
│                                                                                 │
│  fraud_detection_daily_dag (00:00 UTC)                                          │
│  validate_silver ─► decide_retrain ─► retrain_model ─► evaluate_model          │
│                                    ─► skip_retrain   ─► promote_gold ─► notify │
│                                                                                 │
│  data_quality_monitoring_dag (every 1h)                                         │
│  row_count_check ┐                                                              │
│  schema_check    ├── parallel ──► PostgreSQL (dq_checks table)                  │
│  null_rate_check ┘                                                              │
│  fraud_rate_check                                                               │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│  ML LAYER  (XGBoost + MLflow)                                                   │
│  train_model.py  ──►  MLflow experiment tracking  ──►  fraud_model.pkl         │
│                                                        (shared Docker volume)  │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────────┐
│  ANALYTICS LAYER  (Grafana + PostgreSQL)                                        │
│  Live Dashboard: flagged counts · avg amount · DQ pass rate · time-series      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Design

### 2.1 Data Ingestion — Kafka Producer

**File**: `producer/csv_to_kafka.py`

The producer reads the dataset in chunks and sends each transaction row as a JSON message to Kafka topic raw-transactions. This avoids loading the full CSV file into memory at once and simulates a micro-batch ETL ingestion process.

```
CSV File
  │
  ▼
csv_to_kafka.py
  │  Serialize to JSON
  │  Add fraud signals (5 patterns)
  ▼
Kafka Topic: raw-transactions (3 partitions)
```

**Kafka Schema** (JSON per message):
```json
{
  "Transaction_ID": "TXN_001",
  "User_ID": "USER_42",
  "Transaction_Amount": 5000.00,
  "Transaction_Type": "Online",
  "Timestamp": "2024-01-01 10:30:00",
  "Merchant_Category": "Gambling",
  "Fraud_Label": 1
}
```

---

### 2.2 Stream Processing — Spark Structured Streaming

**File**: `spark_jobs/fraud_streaming_job.py`

A single unified `foreachBatch` function processes every micro-batch (10-second trigger) through three layers:

#### Bronze Layer — Raw Ingestion
- Schema enforcement on incoming JSON
- Append-only write to Delta Lake (`/data/delta/bronze`)
- No transformation — preserves raw events for replay/debugging

#### Silver Layer — Feature Engineering + Scoring
Derived features computed per transaction:

| Feature | Description |
|---|---|
| `log_amount` | `log(1 + Transaction_Amount)` — normalizes skewed amounts |
| `event_hour` | Hour of day (0–23) from Timestamp |
| `amount_bucket` | Bucketed amount: low/medium/high/very_high |
| `merchant_risk_score` | Lookup table: gambling=0.9, crypto=0.85, ... |
| `flag_high_amount` | 1 if amount > $3,000 |
| `flag_velocity` | 1 if daily_count ≥ 6 OR failed_7d ≥ 3 OR previous_fraud > 0 |
| `flag_off_hours` | 1 if hour in [2, 3] |
| `flag_geo_anomaly` | 1 if IP flagged OR distance > 75 |
| `flag_risky_merchant` | 1 if crypto/gambling/wire_transfer |
| `rule_score` | Weighted sum of flags (max 1.0) |
| `ml_score` | XGBoost model prediction (0.0 if no model loaded) |
| `fraud_score` | `max(rule_score, ml_score)` |
| `is_flagged` | 1 if fraud_score ≥ 0.35 |

#### Gold Layer — Flagged Transactions
- Filters Silver to `is_flagged = 1`
- Writes to Delta Lake (`/data/delta/gold`)
- Publishes to Kafka topic `flagged-transactions`
- Writes metrics to PostgreSQL `fraud_metrics` table

---

### 2.3 ML Training — XGBoost + MLflow

**File**: `ml/train_model.py`

```
Silver Delta Table  OR  synthetic_fraud_dataset.csv
          │
          ▼
  Feature selection (10 features)
          │
          ▼
  Train/Test split (80/20, stratified)
          │
  ┌───────┴───────┐
  │ XGBoost       │  scale_pos_weight = neg/pos  (handles imbalance)
  │ n_estimators=300
  │ max_depth=6
  │ learning_rate=0.05
  └───────┬───────┘
          │
  MLflow tracking:
    - ROC-AUC, PR-AUC, Precision, Recall, F1
    - Feature importances
    - Model artifact
          │
          ▼
  fraud_model.pkl  ──►  shared Docker volume  ──►  Spark job loads at startup
```

---

### 2.4 Orchestration — Airflow DAGs

#### DAG 1: `fraud_detection_daily_dag` (Midnight UTC)

```
start
  │
  ▼
validate_silver_data
  │  Check: Silver table readable, > 1000 rows, < 5% nulls
  │
  ▼
decide_retrain  (BranchPythonOperator)
  │
  ├── [if > 10,000 rows] ──► retrain_model
  │                               │
  │                               ▼
  │                         evaluate_model
  │                               │  ROC-AUC ≥ 0.80?
  │                               │
  └── [else] ──► skip_retrain     │
                      │           │
                      └─────┬─────┘
                            ▼
                      promote_to_gold
                            │  Compact + optimize Gold Delta table
                            ▼
                      notify_teams
                            │  POST to MS Teams webhook (optional)
                            ▼
                           end
```

#### DAG 2: `data_quality_monitoring_dag` (Every Hour)

```
start
  │
  ├──────────────────┬──────────────────┬──────────────────┐
  ▼                  ▼                  ▼                  ▼
row_count_check  schema_check     null_rate_check  fraud_rate_check
(all 3 layers)   (Silver cols)    (critical cols)  (0.1%–10% range)
  │                  │                  │                  │
  └──────────────────┴──────────────────┴──────────────────┘
                            │
                            ▼
                  Write results to PostgreSQL
                      (dq_checks table)
                            │
                            ▼
                           end
```

---

### 2.5 Storage — Delta Lake (Bronze / Silver / Gold)

```
/data/delta/
  ├── bronze/          Raw events — append only, schema-enforced
  │   └── _delta_log/  Transaction log (ACID)
  │
  ├── silver/          Feature-engineered + scored
  │   └── _delta_log/
  │
  └── gold/            Fraud-flagged only (is_flagged = 1)
      └── _delta_log/
```

Delta Lake provides ACID transactions, time-travel (audit/replay), and schema evolution — critical for a regulated financial use case.

---

### 2.6 Analytics — PostgreSQL + Grafana

Two PostgreSQL tables feed the Grafana dashboard:

**`fraud_metrics`** — written by Spark Gold layer every micro-batch:
```sql
transaction_id, user_id, amount, fraud_score, is_flagged,
merchant_category, batch_timestamp
```

**`dq_checks`** — written by Airflow hourly DQ DAG:
```sql
check_name, check_type, status, value, threshold,
layer, checked_at
```

The Grafana dashboard auto-provisions with 7 panels:
- Total flagged transactions (last 3h)
- Average flagged transaction amount
- Max single fraud amount
- DQ check pass rate (last 24h)
- Time-series: flagged count + avg amount per minute
- DQ check log table (last 20 entries)
- Fraud events per micro-batch (bar chart, last 50 batches)

---

## 3. Data Flow Diagram (End-to-End)

```
[CSV Dataset]
     │ replay rows
     ▼
[Kafka Producer] ──── raw-transactions (JSON) ────►[Kafka Broker]
                                                         │
                                    ┌────────────────────┘
                                    │ Spark reads stream
                                    ▼
                             [Bronze Layer]
                             Delta Lake append
                                    │
                                    ▼
                             [Silver Layer]
                             Features + rule_score + ml_score
                                    │
                          ┌─────────┴─────────┐
                          │ is_flagged = 1    │ is_flagged = 0
                          ▼                   │ (discard from Gold)
                     [Gold Layer]             │
                     Delta Lake               │
                          │                   │
              ┌───────────┤                   │
              │           │                   │
              ▼           ▼                   │
     [Kafka: flagged] [PostgreSQL]            │
                       fraud_metrics          │
                          │                   │
                          ▼                   │
                      [Grafana]               │
                      Dashboard               │
                                              │
[Airflow Daily DAG]                           │
     │                                        │
     ├── reads Silver ◄───────────────────────┘
     ├── trains XGBoost
     ├── saves model.pkl
     └── model loaded by Spark at next startup
```

---

## 4. Technology Decisions and Rationale

| Decision | Choice | Why Not Alternative |
|---|---|---|
| Message broker | Kafka | RabbitMQ lacks replay; Pulsar adds complexity |
| Stream processor | Spark Structured Streaming | Flink is more powerful but steeper learning curve |
| Storage format | Delta Lake | Parquet lacks ACID; Iceberg equivalent but less Python-friendly |
| ML framework | XGBoost | Neural nets overkill for tabular; LightGBM similar but XGBoost more documented |
| Orchestrator | Airflow | Prefect/Dagster newer but less industry-standard |
| Metadata store | PostgreSQL | Simple, free, Grafana native data source |
| Dashboard | Grafana | Kibana requires ELK; Superset heavier |

---

## 5. Scalability Path (Beyond This Project)

| Concern | Current | Production Path |
|---|---|---|
| Kafka throughput | 1 broker, RF=1 | 3+ brokers, RF=3, partition scaling |
| Spark | Single master + 1 worker | YARN/Kubernetes cluster mode |
| Airflow | LocalExecutor | CeleryExecutor + Redis |
| ML serving | Batch pkl load | MLflow Model Registry + online serving (FastAPI) |
| Storage | Local filesystem | S3/GCS/ADLS with Delta Lake |
| Security | PLAINTEXT Kafka, no TLS | SASL/SSL Kafka, mTLS between services |
