# Setup Help - Step by Step

This guide helps you get the system running.

## Prerequisites

1. **Docker Desktop** installed ([download here](https://www.docker.com/products/docker-desktop))
2. **At least 8 GB RAM** allocated to Docker (12 GB recommended)
3. **These ports free**: 3000, 5001, 5432, 7077, 8080, 8081, 8082, 9092

### Check Prerequisites

```bash
# Check Docker is installed
docker --version
# Output: Docker version 20.10.x or higher

# Check ports are free (macOS/Linux)
lsof -i :3000,5001,5432,8080,8081,8082,9092

# On Windows:
netstat -ano | findstr /B :3000
netstat -ano | findstr /B :5001
# etc.
```

## Step 1: Clone and Navigate

```bash
git clone https://github.com/khurshidnm/fraud-detection.git
cd fraud-detection
```

## Step 2: Create Environment Configuration

```bash
# Copy template
cp .env.example .env

# Generate encryption keys for Airflow
FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
SECRET_KEY=$(python -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(24)).decode())")

echo "FERNET_KEY: $FERNET_KEY"
echo "SECRET_KEY: $SECRET_KEY"
```

## Step 3: Edit .env File

Open `.env` in your editor and fill in:

```env
# PostgreSQL (set your own secure password)
POSTGRES_PASSWORD=YourSecurePassword123!

# Airflow (copy from Step 2 above)
AIRFLOW_FERNET_KEY=<paste fernet key here>
AIRFLOW_SECRET_KEY=<paste secret key here>
AIRFLOW_ADMIN_PASSWORD=<set your password>

# Grafana
GRAFANA_PASSWORD=grafana123

# Everything else can stay as default
```

## Step 4: Build and Start Services

```bash
# Start all services (this takes 2-3 minutes on first run)
make up

# Watch the logs
make logs

# Wait for all services to be healthy
# Look for lines like "✓ Spark Master is ready"
# You can Ctrl+C when all services are up
```

**Troubleshooting first startup**:
- If a service fails, it will automatically retry
- Airflow takes longest to initialize (3-5 minutes)
- If Kafka fails, check that port 9092 is free

## Step 5: Check Service Status

```bash
# Show which services are running
make status

# Output should show all as "healthy" or "running"
```

## Step 6: Open Service UIs

```bash
# Kafka UI - View topics and messages
open http://localhost:8080

# Spark Master UI - View streaming job
open http://localhost:8081

# Airflow - View and manage DAGs
open http://localhost:8082
# Login: admin / AIRFLOW_ADMIN_PASSWORD

# MLflow - View model training runs
open http://localhost:5001

# Grafana - View dashboards
open http://localhost:3000
# Login: admin / grafana123

# PostgreSQL - Direct database access
# Host: localhost:5432
# User: fraud_admin
# Password: <from .env POSTGRES_PASSWORD>
```

## Step 7: Train the ML Model

In a **new terminal**:

```bash
cd fraud-detection  # Make sure you're in project directory
make train-kaggle

# Watch for:
# 📂 Loading dataset from /data/synthetic_fraud_dataset.csv
# ✓ Data split: Train: 4000, Test: 1000
# ✓ Training XGBoost...
# 📈 Evaluating on test set...
# 💾 Saving model...
# ✓ TRAINING COMPLETE
```

Once training completes, check MLflow:
```bash
open http://localhost:5001
# You should see a new run with metrics
```

## Step 8: View Fraud Detections

```bash
# See transactions being processed
make kafka-consume

# Example output:
# {
#   "Transaction_ID": "TXN_...",
#   "Transaction_Amount": 5000.00,
#   "Merchant_Category": "Gambling",
#   "Fraud_Label": 1
# }

# See only flagged (fraud) transactions
make kafka-consume-fraud

# Example output:
# {
#   "Transaction_ID": "TXN_...",
#   "fraud_score": 0.85,
#   "rule_based_score": 0.80,
#   "ml_score": 0.90
# }
```

## Step 9: View Real-Time Dashboard

```bash
# Open Grafana
open http://localhost:3000

# Login: admin / grafana123

# You should see:
# - Flagged Transactions (count chart)
# - Average Amount of Flagged Txns
# - Max Single Fraud Amount
# - Data Quality Check Pass Rate
# - Fraud Events per Minute (bar chart)
# - DQ Check Log (table)
```

## Step 10: Monitor Pipelines

```bash
# View Airflow DAGs
open http://localhost:8082

# You should see:
# - fraud_detection_daily_dag (runs daily at midnight)
# - data_quality_monitoring_dag (runs every hour)

# Manual trigger (for testing):
# 1. Click on DAG name
# 2. Click "Trigger DAG"
# 3. Click "Graph" to see it running
```

## Verification Checklist

✅ All services started:
```bash
make status
# All should show "running" or "healthy"
```

✅ Data is flowing:
```bash
make kafka-consume
# Should show new transactions every second
```

✅ Spark is processing:
```bash
make logs-spark
# Should show "💾 Writing to silver layer"
```

✅ Model is trained:
```bash
open http://localhost:5001
# Should see training run with metrics
```

✅ Dashboard shows data:
```bash
open http://localhost:3000
# Should see charts with data (not empty)
```

## Useful Commands

```bash
# View all logs
make logs

# View specific service logs
make logs-producer
make logs-spark
make logs-airflow

# List Kafka topics
make kafka-topics

# Check service health
make status

# Stop everything
make down

# Clean everything (removes all data)
make clean

# Restart everything
make restart
```

## If Something Goes Wrong

### Services won't start

```bash
# Check Docker is running
docker ps

# See what's failing
docker-compose logs

# Restart everything
make restart
```

### Airflow fails to start

```bash
# Airflow needs PostgreSQL ready first
docker-compose logs postgres  # Check if healthy

# Give it more time (can take 5 minutes)
sleep 60
make up

# Or just restart
make restart
```

### No data in Grafana

```bash
# Make sure producer is running
make logs-producer
# Should see "📊 Produced 100 transactions"

# Make sure Spark is processing
make logs-spark
# Should see "💾 Writing to silver layer"

# Check PostgreSQL has data
make shell-postgres
fraud_db=# SELECT COUNT(*) FROM fraud_metrics;
```

### Can't connect to PostgreSQL

```bash
# Check if port 5432 is free
lsof -i :5432

# Check if database is healthy
docker-compose exec postgres pg_isready

# Restart PostgreSQL
docker-compose restart postgres
```