# Real-Time Financial Fraud Detection System

A production-grade data engineering pipeline that detects financial fraud in real time using Apache Kafka, Spark Structured Streaming, Delta Lake, XGBoost, MLflow, Airflow, and Grafana — all running locally via Docker Compose.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                                  │
│  transaction-producer  →  Kafka (raw-transactions, 3 partitions)        │
│  PaySim-style generator   5 fraud patterns · configurable TPS           │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
┌──────────────────────────────────▼──────────────────────────────────────┐
│                     SPARK STRUCTURED STREAMING                          │
│                   Unified foreachBatch (10 s trigger)                   │
│                                                                         │
│   Bronze  ──►  Raw events, schema-enforced          Delta Lake          │
│   Silver  ──►  Features + rule score + ML score     Delta Lake          │
│   Gold    ──►  Flagged transactions only            Delta Lake           │
│                    │                                                     │
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
make train
```

That's it. The transaction producer starts generating data automatically, and Spark Streaming picks it up within seconds.

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

## Fraud Detection Pipeline

### Fraud Patterns Simulated (Producer)

| Pattern | Flag | Trigger |
|---|---|---|
| High-amount spike | `flag_high_amount` | amount > $3,000 |
| Velocity attack | `flag_velocity` | > 5 transactions from same card in 2 min |
| Off-hours activity | `flag_off_hours` | Transactions 02:00–04:00 UTC |
| Geo-anomaly | `flag_geo_anomaly` | Location far from customer home region |
| Risky merchant | `flag_risky_merchant` | crypto exchange / gambling / wire transfer |

### Scoring (Spark Streaming)

1. **Rule-based score** runs on every batch (weighted sum of fraud flags, max 1.0)
2. **ML score** (XGBoost `predict_proba`) is computed when a trained model exists
3. Final `fraud_score = max(rule_score, ml_score)` — flagged if ≥ 0.35

### Delta Lake Layers

| Layer | Path | Contents |
|---|---|---|
| Bronze | `/data/delta/bronze` | Raw events, append-only |
| Silver | `/data/delta/silver` | Feature-engineered: `log_amount`, `merchant_risk_score`, `event_hour`, `amount_bucket`, `fraud_score`, `is_flagged` |
| Gold | `/data/delta/gold` | Flagged transactions only |

## ML Training

```bash
# Train on synthetic data (no Spark / Delta Lake required — good for cold start)
make train

# Train on real Silver Delta Lake data (run after streaming has collected data)
docker compose run --rm \
  -v $(PWD)/ml:/opt/ml \
  -v fraud-detection-system_models-data:/data/models \
  -e MLFLOW_TRACKING_URI=http://mlflow:5001 \
  -e MODEL_PATH=/data/models/fraud_model.pkl \
  airflow-webserver \
  python /opt/ml/train_model.py --source delta
```

The trained model is saved to the shared `models-data` Docker volume and loaded automatically by the Spark Streaming job on its next batch. No restart required.

**Model features**: `amount`, `log_amount`, `merchant_risk_score`, `flag_high_amount`, `flag_velocity`, `flag_off_hours`, `flag_geo_anomaly`, `flag_risky_merchant`, `is_online`, `event_hour`, `merchant_category_enc`, `card_type_enc`

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
make train           # Run ML training (synthetic data)
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
│   ├── transaction_generator.py # Kafka producer — PaySim-style synthetic data
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
    │   └── fraud_overview.json  # Pre-built live overview dashboard
    └── provisioning/
        ├── dashboards/dashboard.yaml
        └── datasources/datasource.yaml
```

## Grafana Dashboard

The **Fraud Detection — Live Overview** dashboard auto-provisions on first start with four panels:

- Total flagged transactions (last 3 h)
- Average flagged transaction amount
- Max single fraud amount
- Data quality check pass rate (last 24 h)
- Time-series: flagged count + avg amount per minute
- DQ check log table (last 20 entries)
- Fraud events per micro-batch (bar chart, last 50 batches)

## Known Limitations

- **Single Kafka broker / replication-factor=1** — suitable for local development; add brokers and increase replication for production
- **LocalExecutor in Airflow** — does not scale horizontally; switch to CeleryExecutor or KubernetesExecutor for multi-node setups
- **No TLS on Kafka** — `PLAINTEXT` listeners only; configure SSL/SASL for production
- **Pickle for model serialization** — convenient but not portable across Python versions; MLflow artifact store is the authoritative model registry

## License

MIT
