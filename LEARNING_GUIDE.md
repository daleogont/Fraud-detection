# Learning Guide - Understanding the Fraud Detection System

This guide explains each component of the system from an ML student's perspective.

## Table of Contents
1. [Data Flow](#data-flow)
2. [Component Walkthrough](#component-walkthrough)
3. [ML Model Explained](#ml-model-explained)
4. [Common Tasks](#common-tasks)
5. [Troubleshooting](#troubleshooting)

---

## Data Flow

Think of the system as an **assembly line**:

```
Transactions → Kafka → Spark → Delta Lake → ML Model → Scoring → Alerts
   (INPUT)    (BUFFER)  (CLEAN)  (STORAGE)  (PREDICT)  (OUTPUT)   (ACTION)
```

### Step-by-step:

**1. Transactions Enter (Producer)**
```python
# producer/transaction_generator.py
# Generates synthetic transaction data with fraud signals
transaction = {
    "transaction_id": "TXN_001",
    "amount": 5000.00,
    "card_id": "CARD_1234",
    "flag_high_amount": True,  # Signal
    "flag_velocity": False,     # Signal
    "timestamp": "2024-01-01T10:30:00"
}
```

**2. Stored in Kafka (Message Queue)**
- Kafka is a **message queue** — it stores transactions temporarily
- Spark reads from it continuously
- If Spark goes down, Kafka still has the data

**3. Spark Processes (Streaming Pipeline)**
```python
# spark_jobs/fraud_streaming_job.py
# For each batch of transactions:
# 1. Read from Kafka
# 2. Extract features (engineer features)
# 3. Apply business rules (rule-based scoring)
# 4. Load ML model and score
# 5. Store in Delta Lake
# 6. Publish flagged txns to alert queue
```

**4. Delta Lake Storage (Data Warehouse)**
- **Bronze layer**: Raw data (immutable history)
- **Silver layer**: Cleaned, featured data (for training)
- **Gold layer**: Flagged transactions only (for alerts)

Think of layers like this:
- **Bronze** = photograph of reality (never change it)
- **Silver** = organized photo album (ready to use)
- **Gold** = highlights (what matters)

**5. ML Model Scores**
- XGBoost model predicts: "Is this fraud?"
- Output: Probability (0-1)
- Examples:
  - 0.05 = probably NOT fraud
  - 0.50 = could be either
  - 0.85 = probably fraud

**6. Combine Scores & Alert**
```python
# Combine rule-based + ML scores
final_score = max(rule_score, ml_score)

# Flag if above threshold
if final_score >= 0.35:
    publish_to_kafka("flagged-transactions", transaction)
```

---

## Component Walkthrough

### 1. Transaction Generator (`producer/transaction_generator.py`)

**What it does**: Simulates real transactions with fraud signals

**For students**: This is where synthetic data comes from. In real systems, this would be a card network or payment processor.

**Fraud patterns it creates**:
- **High amount** (>$3000): Fraudsters try to steal large amounts
- **Velocity** (many txns fast): Fraudsters test stolen cards
- **Off-hours** (2-4 AM): Fraudsters avoid detection by acting when you sleep
- **Geo-anomaly** (far from home): Transaction from impossible location
- **Risky merchant** (crypto, gambling): High-risk categories

**Example run**:
```bash
# Start producer
docker-compose up producer

# In another terminal, peek at data
make kafka-consume

# Output:
# {"transaction_id": "TXN_001", "amount": 5000, "flag_high_amount": true, ...}
# {"transaction_id": "TXN_002", "amount": 50, "flag_high_amount": false, ...}
```

### 2. Spark Streaming (`spark_jobs/fraud_streaming_job.py`)

**What it does**: Processes transactions in real-time, engineers features, scores fraud

**For students**: This is the ML pipeline's main component

**Key steps**:
```
1. Read Kafka
2. Feature Engineering
   - log_amount = log(amount)  # Scale large amounts
   - event_hour = extract hour from timestamp
   - merchant_risk_score = check merchant category
   - etc.
3. Rule-Based Scoring
   - fraud_score = 0.3*flag_high_amount + 0.25*flag_velocity + ...
4. ML Scoring
   - Load pretrained XGBoost model
   - ml_score = model.predict_proba(features)
5. Final Scoring
   - final_score = max(rule_score, ml_score)
   - is_flagged = final_score > 0.35
6. Store Results
   - Bronze: raw transactions
   - Silver: with all features
   - Gold: only flagged ones
```

**View logs**:
```bash
make logs-spark

# Look for lines like:
# ✓ Kafka stream reader initialized
# ✓ Features engineered
# 💾 Writing to silver layer at /data/delta/silver
```

### 3. ML Training (`ml/train_model.py`)

**What it does**: Trains XGBoost model on labeled historical data

**For students**: This is where the ML happens!

**Model details**:
```python
# Input: Historical transactions with labels
X = [amount, log_amount, merchant_risk_score, flags, ...]
y = [0, 1, 0, 1, ...]  # 0=normal, 1=fraud

# Train XGBoost
model = XGBClassifier(n_estimators=100, max_depth=5, ...)
model.fit(X_train, y_train)

# Evaluate
roc_auc = 0.82  # How good at ranking fraud vs normal?
pr_auc = 0.75   # How good at finding fraud?
precision = 0.70 # Of flagged txns, how many real fraud?
recall = 0.80    # Of all fraud, how many did we catch?
```

**Key metrics for fraud**:
- **ROC-AUC**: General goodness (0.5=random, 1.0=perfect)
- **PR-AUC**: Better for imbalanced data (fraud is rare!)
- **Precision**: "When you flag a txn, are you right?"
- **Recall**: "Do you catch all the fraud?"

**Run training**:
```bash
make train

# Outputs:
# 🤖 Generating 5000 synthetic transactions...
# 📊 Data split: Train: 4000 samples, Test: 1000 samples
# 🤖 Training XGBoost...
# ✓ Model trained
# 📈 Feature importance:
#    flag_high_amount: 0.2854
#    merchant_risk_score: 0.2146
#    log_amount: 0.1892
#    flag_velocity: 0.1654
```

### 4. Airflow DAGs (`dags/`)

**What it does**: Orchestrates workflows — daily training, hourly data quality checks

**For students**: Airflow manages the "who runs when" questions

**Daily DAG** (`fraud_detection_daily_dag.py`):
```
Start
  ↓
Validate Silver data quality
  ↓
Decide: Should we retrain? (>1000 new samples?)
  ├─ YES → Retrain model
  │         ↓
  │      Evaluate (ROC-AUC >= 0.80?)
  │         ↓
  │      Promote flagged txns to Gold
  │
  └─ NO → Skip retrain
           ↓
        Promote flagged txns to Gold
           ↓
        Notify team
  ↓
End
```

**Hourly DAG** (`data_quality_monitoring_dag.py`):
```
Check: Row counts exist?
Check: Schema correct?
Check: Null rates < 5%?
Check: Fraud rate in range [0.1%, 10%]?
  → All pass? Log as PASS
  → Any fail? Log as FAIL and alert
```

**View in Airflow**:
```
http://localhost:8083
username: admin
password: admin123
```

### 5. PostgreSQL Database (`scripts/init_postgres.sql`)

**What it stores**:
- **fraud_metrics**: Every flagged transaction (for alerting)
- **dq_checks**: Data quality check results (for monitoring)
- **model_training_history**: Model performance over time (for auditing)

**Explore**:
```bash
make shell-postgres

# Inside postgres shell:
fraud_db=# SELECT * FROM fraud_metrics LIMIT 5;
fraud_db=# SELECT COUNT(*) as flagged_count FROM fraud_metrics;
fraud_db=# SELECT * FROM dq_checks ORDER BY check_timestamp DESC LIMIT 10;
```

### 6. Grafana Dashboards (`grafana/dashboards/fraud_overview.json`)

**What it shows**:
- Flagged transactions (count, amount, rate)
- Data quality check results
- Fraud events over time (bar chart)
- Real-time metrics

**Access**:
```
http://localhost:3000
username: admin
password: grafana123
```

---

## ML Model Explained

### Why XGBoost for Fraud?

```
XGBoost vs Other Models for Fraud Detection:

| Model        | Speed | Interpretable | Prod-Ready | Imbalanced | Good for Fraud? |
|--------------|-------|---------------|------------|-----------|-----------------|
| Logistic Reg | Fast  | Very Good     | Yes        | Poor      | ⭐              |
| Random Forest| Good  | Good          | Yes        | Good      | ⭐⭐⭐⭐        |
| XGBoost      | Good  | Good          | Yes        | Good      | ⭐⭐⭐⭐⭐      |
| Deep Learning| Slow  | Poor          | Complex    | Medium    | ⭐⭐            |

→ XGBoost is the sweet spot!
```

### Feature Importance

After training, see which features matter most:
```
Feature                    Importance  Why it matters
─────────────────────────────────────────────────────
flag_high_amount           28.5%       Large amounts are risky
merchant_risk_score        21.5%       Risky merchants (crypto, gambling)
log_amount                 18.9%       Amount patterns
flag_velocity              16.5%       Multiple txns = card test
flag_off_hours            14.6%       Night transactions suspicious
```

### Fraud Score Interpretation

```
Score  | Meaning                              | Action
─────────────────────────────────────────────────────
0.05   | Very likely legitimate              | Approve
0.25   | Probably legitimate                 | Approve
0.35   | ← THRESHOLD                         | FLAG & REVIEW
0.50   | Could be either                     | Definitely flag
0.85   | Very likely fraud                   | Block
0.99   | Almost certainly fraud              | Block
```

---

## Common Tasks

### Task 1: View Generated Transactions

```bash
# See what producer is generating
make kafka-consume

# Shows ~10 transactions as JSON
```

### Task 2: Check Spark Processing

```bash
make logs-spark

# Look for:
# ✓ Kafka stream reader initialized
# ✓ Features engineered
# 💾 Writing to bronze layer
```

### Task 3: Monitor Data Quality

```bash
# Login to Grafana
open http://localhost:3000

# Or query directly
make shell-postgres
fraud_db=# SELECT * FROM dq_checks ORDER BY check_timestamp DESC LIMIT 5;
```

### Task 4: View Model Metrics

```bash
# Open MLflow
open http://localhost:5001

# Shows:
# - All training runs
# - Metrics (ROC-AUC, precision, recall, F1)
# - Feature importance
# - Training parameters
```

### Task 5: Check Data in Delta Lake

```bash
# View data in each layer
docker-compose exec spark-master spark-sql

> SELECT COUNT(*) FROM delta.`/data/delta/bronze`;
> SELECT COUNT(*) FROM delta.`/data/delta/silver` WHERE is_flagged = true;
> SELECT COUNT(*) FROM delta.`/data/delta/gold`;
```

---

## Troubleshooting

### Issue: Producer not generating data

```bash
# Check producer logs
make logs-producer

# Restart producer
docker-compose restart producer

# Verify Kafka is running
make kafka-topics  # Should show topics
```

### Issue: Spark streaming not processing

```bash
# Check Spark logs
make logs-spark

# Make sure producer is sending data
make kafka-consume

# Verify Delta Lake paths exist
docker-compose exec spark-master ls -la /data/delta/
```

### Issue: Airflow DAG not running

```bash
# Check Airflow logs
make logs-airflow

# Verify PostgreSQL is healthy
docker-compose exec postgres psql -U fraud_admin -c "SELECT 1"

# Trigger DAG manually in Airflow UI
# http://localhost:8083 → fraud_detection_daily_dag → Trigger
```

### Issue: Grafana not showing data

```bash
# Check PostgreSQL connection
docker-compose exec postgres psql -U fraud_admin -d fraud_db -c "SELECT * FROM fraud_metrics LIMIT 1"

# Verify datasource is configured
# Grafana → Configuration → Data Sources → PostgreSQL
```

### Issue: Model not loading

```bash
# Check if model file exists
docker-compose exec spark-master ls -la /data/models/

# Check if training completed successfully
make logs-airflow  # Look for training task

# Retrain manually
make train
```

---

## Next Steps

1. **Run the system**: `make up && make train`
2. **Explore Kafka**: `make kafka-consume` and `make kafka-consume-fraud`
3. **Train your own model**: Edit `ml/train_model.py` and `make train`
4. **Modify fraud rules**: Edit `producer/transaction_generator.py`
5. **Add new features**: Edit `spark_jobs/fraud_streaming_job.py`
6. **Create new dashboards**: Edit `grafana/dashboards/fraud_overview.json`

---

## Key Takeaways

For ML students, remember:

✅ **Data Quality is Everything**
- Bad data → Bad model
- Always validate: nulls, schemas, value ranges
- This DAG does it hourly!

✅ **Monitoring is Critical**
- Models degrade over time (data drift)
- Track metrics in production
- MLflow and Grafana show you what's happening

✅ **Real-time is Hard**
- Spark handles streaming complexity
- Delta Lake ensures correctness
- Kafka buffers between stages

✅ **Balance Rules and ML**
- Rules are fast and interpretable (good for fraud)
- ML learns from data (good for new patterns)
- Combine both for best results!

✅ **Reproducibility Matters**
- Airflow orchestrates everything
- Docker makes it portable
- Version your models in MLflow

Happy learning! 🚀
