# Real-Time Financial Fraud Detection System

A **complete, working** ML data engineering pipeline for fraud detection. Built for learning.

**⚡ Quick Start (5 minutes)**
```bash
# 1. Clone and navigate
git clone https://github.com/khurshidnm/fraud-detection.git
cd fraud-detection

# 2. Setup environment
cp .env.example .env

# Generate required keys:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" > /tmp/fernet.txt
python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(24)).decode())" > /tmp/secret.txt

# Edit .env and fill in:
# - AIRFLOW_FERNET_KEY: (from /tmp/fernet.txt)
# - AIRFLOW_SECRET_KEY: (from /tmp/secret.txt)
# - POSTGRES_PASSWORD: any strong password
# - GRAFANA_PASSWORD: any password

# 3. Start everything
make up

# 4. Open services
# Kafka UI:        http://localhost:8080
# Spark Master:    http://localhost:8081
# Airflow:         http://localhost:8082 (admin / AIRFLOW_ADMIN_PASSWORD)
# MLflow:          http://localhost:5001
# Grafana:         http://localhost:3000 (admin / GRAFANA_PASSWORD)
# PostgreSQL:      localhost:5432

# 5. Train model (in another terminal)
make train-kaggle

# 6. Explore data
make kafka-consume
make kafka-consume-fraud
```

That's it! The system is now running and generating fraud detections.

## Why This Project?

If you're an ML student, this project teaches you:
- ✅ **Data Engineering**: How data flows through a real system
- ✅ **ML Ops**: Training, deploying, and monitoring models
- ✅ **Streaming**: Real-time processing (not just batch)
- ✅ **DevOps**: Docker, orchestration, service management
- ✅ **End-to-End**: From data ingestion to dashboards

This is a **complete, working system** — you can run it today.

## How Fraud Detection Works (Simple Explanation)

1. **Data arrives**: Customers make transactions (purchase, transfer, etc.)
2. **Signals**: System checks for "fraud signals" (large amount, unusual location, many txns fast, etc.)
3. **Scoring**: Combines signals into a fraud score (0-1, where 1 = definitely fraud)
4. **Alert**: If score > 0.35, transaction is flagged and investigated
5. **Learning**: System trains models weekly using past transactions to improve

**Key insight**: We use both *rule-based* (manual business rules) and *ML-based* (learned from data) approaches.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                                  │
│  chunk-based CSV producer  →  Kafka (raw-transactions, 3 partitions)     │
│  Micro-batch CSV ingestion · JSON transaction events                    │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────┐
│                     SPARK STRUCTURED STREAMING                          │
│                   Unified foreachBatch (10 s trigger)                   │
│                                                                         │
│   Bronze  ──►  Raw events, schema-enforced          Delta Lake          │
│   Silver  ──►  Features + rule score + ML score     Delta Lake          │
│   Gold    ──►  Flagged transactions only            Delta Lake          │
│                    │                                                    │
│                    ├──► Kafka (flagged-transactions)                    │
│                    └──► PostgreSQL (fraud_metrics)                      │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────┐
│                      ORCHESTRATION (Airflow)                            │
│                                                                         │
│  fraud_detection_daily_dag   (midnight UTC)                             │
│    validate_silver → decide_retrain → retrain_model → evaluate_model   │
│    → promote_to_gold → notify_teams                                     │
│                                                                         │
│  data_quality_monitoring_dag (every hour)                               │
│    row_counts · schema_check · null_rates · fraud_rate  →  PostgreSQL  │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────┐
│                    ML LAYER (XGBoost + MLflow)                          │
│  train_model.py  →  fraud_model.pkl  →  loaded by Spark Streaming      │
│  Tracks: ROC-AUC · PR-AUC · precision · recall · F1 · feature imp.     │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────┐
│                      ANALYTICS (Grafana)                                │
│  Live dashboard: flagged counts · avg amount · DQ check log            │
│  Data source: PostgreSQL (fraud_metrics + dq_checks tables)            │
└─────────────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 7.5 (Confluent) + ZooKeeper |
| Stream processing | Apache Spark 3.5 (Structured Streaming) |
| Storage | Delta Lake 3.0 (Bronze / Silver / Gold) |
| ML training | XGBoost 2.0 + scikit-learn 1.4 |
| ML tracking | MLflow 2.10 |
| Orchestration | Apache Airflow 2.8 (LocalExecutor) |
| Metadata DB | PostgreSQL 15 |
| Dashboards | Grafana 10.2 |

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) ≥ 4.x (with Docker Compose v2)
- 8 GB RAM allocated to Docker (12 GB recommended)
- Ports free: `3000`, `5001`, `5432`, `7077`, `8080`, `8081`, `8082`, `9092`

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/khurshidnm/fraud-detection-system.git
cd fraud-detection-system

# 2. Create your .env file and fill in passwords
cp .env.example .env
# Edit .env — replace all CHANGE_ME values

# 3. Start all services (takes 2–3 minutes on first run)
make up

# 4. (Optional) Pre-train the ML model before streaming starts
make train-kaggle
```

After starting the services, run the CSV producer to stream transaction data into Kafka. Spark Streaming can then consume the records from `raw-transactions`.

## Service UIs

| Service | URL | Default credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Spark Master UI | http://localhost:8081 | — |
| Airflow | http://localhost:8082 | admin / `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| MLflow | http://localhost:5001 | — |
| Grafana | http://localhost:3000 | admin / `GRAFANA_PASSWORD` from `.env` |
| PostgreSQL | localhost:5432 | `POSTGRES_USER` / `POSTGRES_PASSWORD` from `.env` |

## Configuration

The updated CSV producer can be configured with command-line arguments:

| Argument | Default | Description |
|---|---|---|
| `--topic` | `raw-transactions` | Kafka topic name |
| `--max-rows` | Full dataset | Maximum number of rows to send |
| `--chunk-size` | `1000` | Number of CSV rows read per chunk |
| `--delay-between-chunks` | `2` | Delay between chunks to simulate micro-batch ingestion |

All tunables live in `.env`:
| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `fraud_admin` | PostgreSQL superuser |
| `POSTGRES_PASSWORD` | — | Set a strong password |
| `AIRFLOW_FERNET_KEY` | — | Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `AIRFLOW_SECRET_KEY` | — | Generate with `python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(24)).decode())"` |
| `AIRFLOW_ADMIN_PASSWORD` | — | Airflow web UI password |
| `GRAFANA_PASSWORD` | — | Grafana admin password |
| `TRANSACTIONS_PER_SECOND` | `10` | Producer throughput |
| `FRAUD_RATE` | `0.01` | Fraction of transactions forced-fraud |
| `TEAMS_WEBHOOK_URL` | — | (Optional) MS Teams alert webhook |

### Using the Kaggle dataset

### Updated Ingestion Approach

Based on the progress presentation feedback, the ingestion layer was improved from a full CSV loading approach to a chunk-based / micro-batch ETL approach.
The updated producer reads `synthetic_fraud_dataset.csv` in smaller chunks, converts each transaction row into a JSON message, and sends the records to Kafka topic `raw-transactions`.
This avoids loading the full dataset into memory at once, reduces memory usage, and makes the ingestion pipeline closer to a real-world streaming / ETL system.
Example run:

```bash
python producer/csv_to_kafka.py \
  --topic raw-transactions \
  --max-rows 50000 \
  --chunk-size 1000 \
  --delay-between-chunks 2
```

This keeps the project's real-time architecture intact while swapping the synthetic generator for the Kaggle source data.

## Fraud Detection Pipeline

### Derived Fraud Signals (Spark)

| Pattern | Flag | Trigger |
|---|---|---|
| High-amount spike | `flag_high_amount` | amount > $3,000 |
| Velocity attack | `flag_velocity` | daily count >= 6 OR failed count (7d) >= 3 OR previous fraud > 0 |
| Off-hours activity | `flag_off_hours` | transaction hour in [02:00, 04:00] |
| Geo-anomaly | `flag_geo_anomaly` | flagged IP OR transaction distance > 75 |
| Risky merchant | `flag_risky_merchant` | crypto exchange / gambling / wire transfer |

### Scoring (Spark Streaming)

1. **Rule-based score** runs on every batch (weighted sum of fraud flags, max 1.0)
2. Final `fraud_score = max(rule_score, ml_score)` — flagged if ≥ 0.35

### Delta Lake Layers

| Layer | Path | Contents |
|---|---|---|
| Bronze | `/data/delta/bronze` | Raw events, append-only |
| Silver | `/data/delta/silver` | Feature-engineered: `log_amount`, `merchant_risk_score`, `event_hour`, `amount_bucket`, `fraud_score`, `is_flagged` |
| Gold | `/data/delta/gold` | Flagged transactions only |

## ML Training

```bash
# Train with the exact Kaggle dataset mounted at /data/synthetic_fraud_dataset.csv
make train-kaggle

# Optional local fallback (only if you intentionally want synthetic training)
make train
```

**Model features**: `amount`, `log_amount`, `merchant_risk_score`, `flag_high_amount`, `flag_velocity`, `flag_off_hours`, `flag_geo_anomaly`, `flag_risky_merchant`, `is_online`, `event_hour`

## Airflow DAGs

### `fraud_detection_daily_dag` (runs at midnight UTC)

```
start → validate_silver_data → decide_retrain ─┬─► retrain_model ─┐
                                                └─► skip_retrain   ┘
                                                         │
                                               evaluate_model (ROC-AUC ≥ 0.80)
                                                         │
                                               promote_to_gold → notify_teams → end
```

### `data_quality_monitoring_dag` (runs every hour)

Checks run in parallel:
- **row_count_check** — all Delta layers must be readable and non-empty
- **schema_check** — Silver layer must have all expected columns
- **null_rate_check** — critical columns must have < 5% nulls
- **fraud_rate_check** — fraud rate must be in range [0.1%, 10%]

Results are persisted to the `dq_checks` PostgreSQL table and visible in Grafana.

## Makefile Commands

```bash
make up              # Start all services
make down            # Stop all services
make restart         # Stop then start
make logs            # Tail all logs
make logs-producer   # Tail producer logs
make logs-spark      # Tail Spark streaming logs
make logs-airflow    # Tail Airflow logs
make train           # Run ML training (default source)
make train-kaggle    # Run ML training using Kaggle CSV
make kafka-topics    # List Kafka topics
make kafka-consume   # Peek at raw-transactions (20 messages)
make kafka-consume-fraud  # Peek at flagged-transactions (20 messages)
make status          # Show container health
make clean           # Remove all containers, volumes, and images
```

## Project Structure

```
fraud-detection-system/
├── docker-compose.yml           # Full stack (13 services)
├── Makefile                     # Operational shortcuts
├── .env.example                 # Config template (copy → .env)
│
├── producer/
│   ├── csv_to_kafka.py # Chunk-based CSV-to-Kafka producer
│   ├── requirements.txt
│   └── Dockerfile
│
├── spark_jobs/
│   ├── fraud_streaming_job.py   # Unified Bronze/Silver/Gold streaming pipeline
│   └── requirements.txt
│
├── ml/
│   ├── train_model.py           # XGBoost trainer + MLflow tracking
│   └── requirements.txt
│
├── dags/
│   ├── fraud_detection_daily_dag.py      # Daily retrain + Gold promotion
│   └── data_quality_monitoring_dag.py   # Hourly DQ monitoring
│
├── scripts/
│   └── init_postgres.sql        # DB init: airflow + mlflow DBs, metrics tables
│
└── grafana/
    ├── dashboards/
    │   ├── fraud_overview.json             # Original live overview dashboard
    │   └── fraud_alerts_dashboard.json     # Final fraud alerts monitoring dashboard
    └── provisioning/
        ├── dashboards/dashboard.yaml
        └── datasources/datasource.yaml
```

## Grafana Fraud Alerts Monitoring Dashboard

The project includes a Grafana monitoring dashboard for the fraud detection pipeline.

Dashboard export:

```text
grafana/dashboards/fraud_alerts_dashboard.json
```

The dashboard visualizes fraud alerts generated by the real-time pipeline:

```text
Kafka → Spark Structured Streaming → Delta Lake Gold Layer → PostgreSQL → Grafana
```

The PostgreSQL `fraud_metrics` table is used as the Gold-layer alert table. It stores flagged transactions only, where `fraud_score >= 0.35`. Therefore, the dashboard is designed as a fraud alerts monitoring dashboard rather than a full transaction-volume dashboard.

### Dashboard Data Sources

| PostgreSQL Table | Purpose |
|------------------|----------|
| fraud_metrics | Fraud alert metrics from the Gold layer |
| dq_checks | Data quality validation results |
| model_training_history | Model evaluation and version history |

### Dashboard Components

The dashboard includes:

- Total Fraud Alerts
- Average Fraud Score
- High Risk Alerts
- Distinct Alerted Entities
- Maximum Alert Amount
- Average Alert Amount
- Model Performance Metrics
- Alert Severity Distribution
- Fraud Score Distribution
- Highest Alert Amounts
- Top Alerted Entities
- Average Detection Contribution
- Data Quality Checks
- Fraud Alert Details

### Validation Snapshot

During final validation, the PostgreSQL monitoring layer contained:

| Metric | Value |
|----------|-------:|
| Total rows in fraud_metrics | 119 |
| Sample rows | 5 |
| Real fraud alert rows | 114 |
| Real flagged rows | 114 |git status
| Average real fraud score | 0.7404 |

The first 5 rows come from the PostgreSQL initialization script. The remaining 114 rows were generated by the operational Kafka → Spark Streaming → Delta Lake → PostgreSQL pipeline.

### Data Quality Monitoring

The `dq_checks` table stores data quality validation results generated by the Airflow data quality monitoring DAG.

Validated checks include:

- row_count_check
- schema_check
- null_rate_check
- fraud_rate_check

All validation checks passed successfully.

### Model Monitoring

The dashboard also visualizes model performance from the `model_training_history` table.

The active model version is `v1.0` with the following metrics:

| Metric | Value |
|----------|-------:|
| ROC-AUC | 0.820 |
| PR-AUC | 0.785 |
| Precision | 0.750 |
| Recall | 0.810 |
| F1 Score | 0.775 |

### Dashboard Portability

The dashboard has been exported as a JSON artifact and committed to the repository.

```text
grafana/dashboards/fraud_alerts_dashboard.json
```

The dashboard can be imported directly into Grafana if the Grafana database is reset or recreated.

## Known Limitations

- **Single Kafka broker / replication-factor=1** — suitable for local development; add brokers and increase replication for production
- **LocalExecutor in Airflow** — does not scale horizontally; switch to CeleryExecutor or KubernetesExecutor for multi-node setups
- **No TLS on Kafka** — `PLAINTEXT` listeners only; configure SSL/SASL for production
- **Pickle for model serialization** — convenient but not portable across Python versions; MLflow artifact store is the authoritative model registry

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
