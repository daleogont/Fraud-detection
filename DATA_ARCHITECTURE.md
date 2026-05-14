# Data Architecture & Overall System Design
## Real-Time Financial Fraud Detection System

**Author**: Data Architect (Khurshid Normurodov)  
**Date**: May 2026  
**Version**: 1.0  
**Status**: Production Ready

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architectural Principles](#architectural-principles)
3. [System Overview](#system-overview)
4. [Data Architecture](#data-architecture)
5. [Technical Stack](#technical-stack)
6. [Component Design](#component-design)
7. [Data Flow Diagrams](#data-flow-diagrams)
8. [Design Decisions](#design-decisions)
9. [Scalability & Performance](#scalability--performance)
10. [Security & Compliance](#security--compliance)
11. [Deployment Topology](#deployment-topology)
12. [Future Enhancements](#future-enhancements)

---

## Executive Summary

The Fraud Detection System is a **Lambda-like real-time data pipeline** that combines:
- **Streaming processing** for immediate fraud flagging (< 10 seconds)
- **Batch ML retraining** for model improvements (daily)
- **Event sourcing** for audit and replay capabilities
- **Multi-layer analytics** for comprehensive monitoring

**Key Goals**:
- Detect fraudulent transactions in real-time (< 10 second latency)
- Maintain 80%+ precision to avoid analyst fatigue
- Achieve 80%+ recall to catch most fraud
- Process 100+ transactions per second
- Enable daily model retraining without production impact
- Provide comprehensive monitoring and observability

**Architecture Type**: **Lambda Architecture** (Batch + Real-time unified)

---

## Architectural Principles

### 1. **Separation of Concerns**
Each layer handles one responsibility:
- **Ingestion Layer**: Raw data collection
- **Processing Layer**: Transformation and feature engineering
- **ML Layer**: Model training and scoring
- **Serving Layer**: Real-time decisions
- **Analytics Layer**: Monitoring and insights

### 2. **Event Sourcing**
All transactions are immutable events stored in chronological order:
- Bronze layer = permanent record of all events
- Enables replay/reprocessing without data loss
- Audit trail for compliance
- Reproducible analyses

### 3. **Layered Data Model (Medallion Architecture)**
```
Raw Data (Bronze) → Clean Data (Silver) → Business Data (Gold)
     ↓                    ↓                      ↓
  Append-only        Features + Scoring    Aggregates + Insights
  No transforms      ML-ready              Business metrics
  Replay-safe        Schema-enforced       KPI focused
```

### 4. **Micro-Services Architecture**
Loosely coupled, independently deployable components:
- Kafka decouples producer from consumer
- Each service has its own responsibility
- Can scale components independently
- Fault isolation (one service down ≠ whole system fails)

### 5. **Schema-First Design**
All data has explicit schemas:
- Prevents silent data corruption
- Enables backward compatibility
- Facilitates schema evolution
- Data contracts between teams

### 6. **Immutable Infrastructure**
Services are stateless and can restart without data loss:
- All state in external systems (Kafka, Delta Lake, PostgreSQL)
- Checkpointing for stream recovery
- No local file dependencies
- Docker for reproducible environments

### 7. **Observability by Default**
Every component logs and metrics:
- Structured logging (JSON)
- Metrics exported to PostgreSQL
- Grafana dashboards for monitoring
- Alert rules for anomalies

---

## System Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                           │
│  Synthetic Fraud Dataset (CSV) / Real Transaction Streams                      │
└──────────────────────────────────┬──────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│               INGESTION LAYER (Kafka)                                           │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ Topic: raw-transactions (3 partitions)                                │    │
│  │ • Transaction ID, User ID, Amount, Merchant, Timestamp, Location     │    │
│  │ • Replication factor: 1 (dev), 3 (prod)                              │    │
│  │ • Retention: 7 days                                                   │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────┬──────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│           STREAM PROCESSING LAYER (Spark Structured Streaming)                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ BRONZE LAYER (Raw Ingestion)                                           │   │
│  │ • Schema enforcement                                                   │   │
│  │ • Append-only Delta table: /data/delta/bronze                         │   │
│  │ • No transformations (raw preservation)                               │   │
│  │ • Checkpoint: /data/checkpoints/bronze                                │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ SILVER LAYER (Feature Engineering + Scoring)                           │   │
│  │ • 10 derived features (log_amount, hour, bucket, risk_score, etc.)    │   │
│  │ • 5 fraud signals (high_amount, velocity, off_hours, geo, merchant)   │   │
│  │ • Rule-based scoring (0-1 range)                                       │   │
│  │ • Delta table: /data/delta/silver                                      │   │
│  │ • ML score integration (if model loaded)                               │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                ▼                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │ GOLD LAYER (Fraud Flagging + Outputs)                                  │   │
│  │ • Final fraud_score: max(rule_score, ml_score)                         │   │
│  │ • Flag decision: is_flagged = (fraud_score ≥ 0.35)                    │   │
│  │ • Delta table: /data/delta/gold                                        │   │
│  │ • Outputs:                                                              │   │
│  │   ├─ Kafka: flagged-transactions topic                                │   │
│  │   ├─ PostgreSQL: fraud_metrics table                                  │   │
│  │   └─ Dashboards: Grafana (real-time)                                  │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│  • Micro-batch trigger: 10 seconds                                            │
│  • Checkpointing: /data/checkpoints/main                                      │
└─────────────────────────────────┬───────────────────────────────────────────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                         │                         │
        ▼                         ▼                         ▼
    (Delta Lake)            (Kafka Topic)          (PostgreSQL)
   Gold Layer Data       flagged-transactions      fraud_metrics
   (Historical)          (Streaming)               (Analytics)
        │                         │                         │
        └─────────────────────────┼─────────────────────────┘
                                  │
┌─────────────────────────────────────────────────────────────────────────────────┐
│               ML LAYER (XGBoost + MLflow)                                       │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ Daily Retraining Pipeline                                             │    │
│  │ • Input: Silver layer data from yesterday                             │    │
│  │ • Model: XGBoost classifier                                           │    │
│  │ • Metrics: ROC-AUC, PR-AUC, Precision, Recall, F1                   │    │
│  │ • Tracking: MLflow experiments & runs                                 │    │
│  │ • Output: fraud_model.pkl (shared volume)                             │    │
│  │ • Versioning: Staging → Production → Archived                         │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ Model Evaluation & Promotion                                          │    │
│  │ • Compare new model vs production                                     │    │
│  │ • Promotion criteria: PR-AUC improvement ≥ 2%                        │    │
│  │ • Automatic or manual approval                                        │    │
│  │ • Audit trail in model_registry table                                 │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                    ▼                           ▼
            (Shared Volume)              (PostgreSQL)
         fraud_model.pkl              model_registry
            (Streaming uses)           (Audit trail)
                    │                           │
                    └─────────────┬─────────────┘
                                  │
┌─────────────────────────────────────────────────────────────────────────────────┐
│          ORCHESTRATION LAYER (Apache Airflow 2.8)                               │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ fraud_detection_daily_dag (00:00 UTC)                                 │    │
│  │ • validate_silver → decide_retrain → retrain_model                    │    │
│  │ • → evaluate_model → promote_gold → send_notification                 │    │
│  │ • Timeout: 2 hours                                                    │    │
│  │ • Retries: 2 on failure                                               │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ data_quality_monitoring_dag (hourly)                                  │    │
│  │ • bronze_row_count_check, schema_check, null_rate_check              │    │
│  │ • fraud_rate_check, consistency_check                                 │    │
│  │ • Results → PostgreSQL dq_checks table                                │    │
│  │ • Timeout: 10 minutes                                                 │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────┬───────────────────────────────────────────────┘
                                  │
┌─────────────────────────────────────────────────────────────────────────────────┐
│             ANALYTICS LAYER (Grafana + PostgreSQL)                              │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ Grafana Dashboards                                                    │    │
│  │ • KPI Cards: Fraud rate, Detection rate, Model accuracy              │    │
│  │ • Time Series: Flagged transactions, Trends                          │    │
│  │ • Heatmaps: Fraud by hour/merchant                                   │    │
│  │ • Data Quality: Health checks, anomalies                             │    │
│  │ • Model Performance: Feature importance, metrics history             │    │
│  │ • Alerting: Rules for anomalies                                      │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
│  ┌────────────────────────────────────────────────────────────────────────┐    │
│  │ PostgreSQL Analytics Database                                         │    │
│  │ • fraud_metrics: Real-time scoring results                            │    │
│  │ • dq_checks: Data quality metrics                                     │    │
│  │ • model_registry: ML model versions and performance                   │    │
│  └────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Architecture

### Data Layers (Medallion Pattern)

#### **Layer 1: Bronze (Raw Data)**
```
Purpose: Preserve raw data as-is for replay and audit
Location: /data/delta/bronze
Characteristics:
  - Append-only (immutable)
  - Schema enforced (no corruption)
  - No transformations
  - High volume retention (7+ days)
  
Schema:
  {
    "_record_id": "uuid",           // Unique record identifier
    "_ingested_at": "2026-05-14...", // When ingested
    "Transaction_ID": "TXN_001",
    "User_ID": "USER_42",
    "Transaction_Amount": 5000.00,
    "Transaction_Type": "Online",
    "Timestamp": "2026-05-14...",
    "Merchant_Category": "Gambling",
    "Fraud_Label": 1,
    // ... 16 more features
  }

Write Pattern: StreamWriter (foreachBatch)
Read Pattern: Ad-hoc replay, backfill, debugging
Retention: 7 days (configurable)
Replication: 1 (dev), 3 (prod)
```

#### **Layer 2: Silver (Refined Data)**
```
Purpose: Feature engineering, ML preparation, and scoring
Location: /data/delta/silver
Characteristics:
  - All Bronze data + derived features
  - Schema enforced
  - Stateful aggregations (daily counts, velocity)
  - Ready for ML training
  
Additional Features (10 derived):
  - log_amount: Normalized transaction amount
  - event_hour: Hour of day (0-23)
  - amount_bucket: Categorical [low, medium, high, very_high]
  - merchant_risk_score: Lookup table (0-1)
  - day_of_week: Day (0-6)
  - is_weekend: Binary flag
  - transaction_count_today: Daily count per user
  - failed_transactions_7d: Count of failed txns in 7 days
  - hours_since_last_txn: Time since previous transaction
  - fraud_history: Whether user had fraud before
  
Fraud Signals (5 flags):
  - flag_high_amount: amount > $3,000
  - flag_velocity: daily_count ≥ 6 OR failed_7d ≥ 3
  - flag_off_hours: hour in [2, 3, 4]
  - flag_geo_anomaly: distance > 75km from usual location
  - flag_risky_merchant: merchant in [crypto, gambling, wire_transfer]
  
Rule Score Computation:
  rule_score = (
    0.25 * flag_high_amount +
    0.25 * flag_velocity +
    0.15 * flag_off_hours +
    0.20 * flag_risky_merchant +
    0.15 * flag_geo_anomaly
  )
  → Score range: [0, 1]

Write Pattern: StreamWriter (foreachBatch, stateful aggregations)
Read Pattern: ML training, dashboards, analysis
Update Frequency: 10-second micro-batches
Stateful Keys: (user_id, date) for daily aggregations
```

#### **Layer 3: Gold (Business Data)**
```
Purpose: Flagged transactions for investigation and action
Location: /data/delta/gold
Characteristics:
  - Filtered to is_flagged = 1 only
  - Business-ready format
  - Fast queries for dashboards
  - Event stream for action
  
Schema:
  {
    "transaction_id": "TXN_001",
    "user_id": "USER_42",
    "timestamp": "2026-05-14...",
    "amount": 5000.00,
    "merchant": "Crypto Exchange",
    "fraud_score": 0.72,
    "rule_score": 0.65,
    "ml_score": 0.72,
    "fraud_flags": ["flag_high_amount", "flag_risky_merchant"],
    "confidence": "high",
    "action_recommended": "BLOCK"
  }

Write Pattern: StreamWriter (filtered)
Read Pattern: Investigation, case management, dashboards
Output Destinations:
  - Delta Lake (historical archive)
  - Kafka topic "flagged-transactions" (real-time stream)
  - PostgreSQL "fraud_metrics" table (analytics)
  - Grafana dashboards (live visualization)
```

### Data State Management

```
Streaming State (Spark Stateful Aggregations):
┌──────────────────────────────────────┐
│ State Store (/data/checkpoints/state)│
├──────────────────────────────────────┤
│ daily_transaction_count[user][date]  │ Updates every micro-batch
│ failed_transactions[user][date]      │ Aggregates over 7 days
│ last_transaction_time[user]          │ Latest timestamp
│ user_fraud_history[user]             │ Ever had fraud?
└──────────────────────────────────────┘

Checkpoint Locations:
├─ /data/checkpoints/bronze     (Bronze writer)
├─ /data/checkpoints/silver     (Silver writer with state)
├─ /data/checkpoints/main       (Gold writer)
└─ /data/checkpoints/state      (State store for aggregations)

Purpose:
  - Enables recovery from failures
  - Exactly-once processing semantics
  - No data loss or duplicates
  - Can replay from checkpoints
```

---

## Technical Stack

### Layered Technology Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    APPLICATION LAYER                             │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Fraud Detection Rules Engine + ML Scoring Logic         │   │
│  │ • Rule-based scoring (5 fraud signals)                  │   │
│  │ • XGBoost ML scoring (ensemble)                         │   │
│  │ • Final decision threshold (0.35)                       │   │
│  │ • Explainability (which rules/features triggered)       │   │
│  └──────────────────────────────────────────────────────────┘   │
├──────────────────────────────────────────────────────────────────┤
│              PROCESSING & ORCHESTRATION LAYER                    │
│  ┌─────────────────────┐ ┌────────────────┐ ┌──────────────┐   │
│  │ Spark Structured    │ │ Apache Airflow │ │ MLflow       │   │
│  │ Streaming           │ │ • Scheduling   │ │ • Tracking   │   │
│  │ • Stream processor  │ │ • DAGs         │ │ • Registry   │   │
│  │ • Micro-batches     │ │ • Monitoring   │ │ • Versioning│   │
│  │ • Stateful agg.     │ └────────────────┘ └──────────────┘   │
│  └─────────────────────┘                                        │
├──────────────────────────────────────────────────────────────────┤
│              DATA STORAGE & PROCESSING LAYER                     │
│  ┌─────────────────────┐ ┌────────────────┐ ┌──────────────┐   │
│  │ Delta Lake          │ │ Apache Kafka   │ │ PostgreSQL   │   │
│  │ • ACID guarantees   │ │ • Message q    │ │ • Metrics    │   │
│  │ • Time travel       │ │ • Event replay │ │ • Analytics  │   │
│  │ • Schema evolution  │ │ • Decoupling   │ │ • DQ checks  │   │
│  │ • Multi-version     │ │ • Partitions   │ │ • Registry   │   │
│  └─────────────────────┘ └────────────────┘ └──────────────┘   │
├──────────────────────────────────────────────────────────────────┤
│              INGESTION & COORDINATION LAYER                      │
│  ┌─────────────────────┐ ┌────────────────┐ ┌──────────────┐   │
│  │ CSV Producer        │ │ ZooKeeper      │ │ Docker       │   │
│  │ • Data source       │ │ • Coordination │ │ • Containers │   │
│  │ • Rate limiting     │ │ • Config mgmt  │ │ • Networking │   │
│  │ • Serialization     │ └────────────────┘ └──────────────┘   │
│  └─────────────────────┘                                        │
├──────────────────────────────────────────────────────────────────┤
│               MONITORING & VISUALIZATION LAYER                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Grafana (Dashboards) + PostgreSQL (Metrics Storage)     │   │
│  │ • KPIs, trends, anomalies                              │   │
│  │ • Alerting rules                                        │   │
│  │ • Data quality scorecards                              │   │
│  │ • Model performance tracking                           │   │
│  └──────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘

Core Dependencies:
  Python 3.11 (all services)
  Apache Spark 3.5 (streaming engine)
  Apache Kafka 7.5 (message broker)
  Delta Lake 3.0 (ACID tables)
  Apache Airflow 2.8 (orchestration)
  XGBoost 2.0 (ML model)
  MLflow 2.10 (experiment tracking)
  PostgreSQL 15 (analytics DB)
  Grafana 10.2 (dashboards)
  Docker v2 (containerization)
```

---

## Component Design

### 1. Data Ingestion Component

**Kafka Producer** (`producer/transaction_generator.py`)
```
Responsibility:
  - Load synthetic fraud dataset from CSV
  - Replay transactions at configurable rate (TPS)
  - Serialize to JSON with proper schema
  - Handle Kafka connection/failures
  - Add fraud injection patterns (optional)

Input:
  - data/synthetic_fraud_dataset.csv (100K+ rows)
  - Environment variables: TPS, FRAUD_RATE, KAFKA_BROKER

Output:
  - Kafka topic: raw-transactions
  - Message format: JSON
  - Partitions: 3 (for parallelism)
  - Throughput: 10-100 TPS

Failure Handling:
  - Retry on Kafka connection failure
  - Exponential backoff
  - Dead-letter logging
  - Health check endpoint
```

### 2. Stream Processing Component

**Spark Structured Streaming Job** (`spark_jobs/fraud_streaming_job.py`)
```
Architecture: foreachBatch pattern
  Input: Kafka topic (raw-transactions)
  ↓
  Per Micro-Batch (10 seconds):
    ├─ Validate schema
    ├─ Add technical columns (_record_id, _ingested_at)
    ├─ Write Bronze layer
    ├─ Compute Silver layer features
    ├─ Score (rule + ML)
    ├─ Filter Gold layer
    ├─ Write outputs (Delta + Kafka + PostgreSQL)
    └─ Update metrics
  ↓
  Output: 3 sinks (Delta, Kafka, PostgreSQL)

Stateful Aggregations (Window Functions):
  - 1-day sliding window: transaction_count_today
  - 7-day sliding window: failed_transactions_7d
  - Last seen: hours_since_last_transaction
  - Boolean: fraud_history_exists

Checkpointing:
  - Enable exactly-once semantics
  - Recover from failures without reprocessing
  - Checkpoint every 10 seconds
  - Store in /data/checkpoints/

Error Handling:
  - Schema validation on every record
  - Dead-letter queue for invalid records
  - Logging of all errors
  - Graceful degradation (process what we can)
```

### 3. ML Training Component

**XGBoost Model Training** (`ml/train_model.py`)
```
Pipeline:
  1. Data Loading
     - Input: Silver layer (yesterday's data) OR CSV
     - Row count: 10K-100K transactions
  
  2. Feature Selection
     - Selected 10 features (out of derived set)
     - Remove highly correlated features
     - Handle missing values (fillna)
  
  3. Class Imbalance Handling
     - Stratified train/test split (80/20)
     - Calculate: scale_pos_weight = neg_samples / pos_samples
     - Typically 20-50 (fraud is rare)
  
  4. Model Training
     - Algorithm: XGBoost Classifier
     - Parameters:
       * n_estimators: 300 trees
       * max_depth: 6 (avoid overfitting)
       * learning_rate: 0.05 (slow, steady learning)
       * scale_pos_weight: (calculated)
       * random_state: 42 (reproducible)
  
  5. Evaluation
     - Metrics: ROC-AUC, PR-AUC, Precision, Recall, F1
     - Cross-validation: 5-fold stratified
     - Feature importance: SHAP values
  
  6. Model Artifacts
     - Save: models/fraud_model.pkl (pickle format)
     - Log to MLflow:
       * Metrics (ROC-AUC, PR-AUC, etc.)
       * Parameters (hyperparameters)
       * Feature importances (JSON)
       * Model artifact (pickle)

MLflow Tracking:
  - Experiment: "fraud-detection"
  - Run: one per training job
  - Tags: date, data_source, train_rows
  - Metrics: continuously logged during training

Success Criteria:
  - ROC-AUC ≥ 0.85 (overall discriminative power)
  - PR-AUC ≥ 0.70 (imbalanced class performance)
  - Training time < 30 minutes
  - Inference latency < 100ms per batch
```

### 4. Orchestration Component

**Airflow DAGs** (`dags/`)
```
DAG 1: fraud_detection_daily_dag
  Schedule: 00:00 UTC (daily)
  Timeout: 2 hours
  Retries: 2 per task
  
  Task Graph:
    start
      ↓
    validate_silver_data
      │ Check: row count ≥ yesterday
      │ Check: schema correct
      ├─ FAIL → alert_dq_failure → end
      └─ SUCCESS
      ↓
    decide_retrain
      │ Branch: should we retrain?
      │ Condition: fraud_rate > 5% OR new patterns detected
      ├─ YES → retrain_model
      └─ NO  → skip_retrain
      ↓
    [retrain_model] (SparkSubmitOperator)
      │ Run: ml/train_model.py
      │ Params: date, data_source, hyperparams
      ├─ SUCCESS → model_retrained
      └─ FAIL → alert_training_failure → end
      ↓
    evaluate_model
      │ Compare: new model vs production
      │ Metric: PR-AUC improvement
      ├─ Improvement ≥ 2% → ready_to_promote
      └─ No improvement → skip_promotion
      ↓
    promote_to_production
      │ Update: model_registry table
      │ Action: fraud_model.pkl = new model
      │ MLflow: move run to "Production" stage
      ├─ SUCCESS → model_promoted
      └─ FAIL → alert_promotion_failure
      ↓
    send_notification
      │ Slack/Email with:
      │ • ROC-AUC, PR-AUC scores
      │ • Training date/time
      │ • Promotion status
      ↓
    end
  
  Alerting:
    - SLA breach (> 2 hours)
    - Training failure
    - Evaluation failure
    - Data quality failure
  
DAG 2: data_quality_monitoring_dag
  Schedule: 00:00 every hour (top of hour)
  Timeout: 10 minutes
  Retries: 1 per task
  
  Task Graph:
    start
      ├─ bronze_row_count_check
      ├─ bronze_schema_check
      ├─ silver_null_rate_check
      ├─ fraud_rate_check
      ├─ gold_consistency_check
      │  (all parallel)
      ↓
    consolidate_dq_report
      │ Combine all results
      │ Write to: dq_checks table
      │ Alert if: any check fails
      ↓
    end
  
  Alerting:
    - Row count anomaly
    - Schema mismatch
    - Null rate > 2%
    - Fraud rate spike (> 10%)
    - Fraud rate too low (< 0.1%)
```

### 5. Analytics Component

**Grafana Dashboard** (`grafana/dashboards/`)
```
Dashboard: fraud_overview.json

Panel 1: KPI Cards
  ├─ Total Transactions (24h)
  ├─ Flagged Transactions (24h)
  ├─ Fraud Rate (%)
  ├─ Model Accuracy (PR-AUC)
  ├─ False Positive Rate
  └─ Processing Latency (ms)

Panel 2: Time Series - Flagged Transactions
  ├─ X-axis: Time (hourly)
  ├─ Y-axis: Flagged transaction count
  ├─ Zoom: 7-day history
  └─ Alert threshold: > 50/hour

Panel 3: Fraud Rate Trend
  ├─ X-axis: Date
  ├─ Y-axis: Fraud rate (%)
  ├─ Baseline: Expected rate
  ├─ Spike detection: ± 2σ
  └─ Zoom: 30-day history

Panel 4: Heatmap - Fraud by Hour & Merchant
  ├─ X-axis: Hour of day
  ├─ Y-axis: Merchant category
  ├─ Color: Fraud rate (red = high)
  └─ Helps identify patterns

Panel 5: Data Quality Scorecards
  ├─ Bronze DQ score (%)
  ├─ Silver DQ score (%)
  ├─ Gold DQ score (%)
  ├─ Check: null rate per column
  ├─ Check: schema validation
  └─ Alert: any < 95%

Panel 6: Model Performance History
  ├─ ROC-AUC trend (30-day)
  ├─ PR-AUC trend (30-day)
  ├─ Feature importance (top 10)
  ├─ Confusion matrix (latest)
  └─ Training date display

Queries (PostgreSQL):
  - fraud_metrics (latest flagged transactions)
  - dq_checks (hourly DQ scores)
  - model_registry (model performance history)
  - Aggregations: hourly, daily, weekly

Alerting Rules:
  ├─ Fraud rate spike > 5%
  ├─ Fraud rate drop < 0.1%
  ├─ Pipeline latency > 30s
  ├─ DQ check failure
  ├─ Model accuracy drop > 5%
  └─ Any DAG failure
```

---

## Data Flow Diagrams

### Real-Time Processing Flow

```
Time T (10-second micro-batch):

┌─────────────────────────────────────────┐
│ Kafka: raw-transactions Topic           │
│ Partition 0: TXN_001, TXN_003, TXN_005 │
│ Partition 1: TXN_002, TXN_004, TXN_006 │
│ Partition 2: TXN_007, TXN_008, TXN_009 │
└─────────────────────────────────────────┘
           ↓ (Spark reads all partitions in parallel)
       Micro-batch collected
       ┌─────────────────────┐
       │ Schema validation   │
       │ Add metadata        │
       │ (_record_id, time)  │
       └─────────────────────┘
           ↓
       Bronze Write (append-only)
       ┌─────────────────────┐
       │ /data/delta/bronze  │
       │ 9 raw records       │
       └─────────────────────┘
           ↓ (Read from Bronze checkpoint)
       Feature Engineering (per record)
       ┌─────────────────────┐
       │ Load state store    │
       │ Compute aggs        │
       │ Enrich features     │
       │ Score (rule + ML)   │
       └─────────────────────┘
           ↓
       Silver Write (all records)
       ┌─────────────────────┐
       │ /data/delta/silver  │
       │ 9 records + features│
       └─────────────────────┘
           ↓
       Filter Gold (is_flagged = 1)
       ┌─────────────────────┐
       │ 2 flagged records   │
       │ TXN_003, TXN_008    │
       └─────────────────────┘
           ↓ (Branch to 3 outputs)
       ┌───────────┬──────────┬────────────┐
       ↓           ↓          ↓            ↓
    Gold      Kafka         PostgreSQL  Metrics
    Write     Publish       Insert      Update
    (/delta)  (stream)      (fraud_    (counters)
              (action)      metrics)   (latencies)
       │           │          │            │
       └───────────┴──────────┴────────────┘
                    ↓
            Checkpoint saved
            /data/checkpoints/main
            Ready for next batch
```

### Daily ML Retraining Flow

```
Time: 00:00 UTC (Daily)

  Airflow Scheduler triggers DAG
        ↓
  validate_silver_data
    ├─ Query: SELECT COUNT(*) FROM silver WHERE date = yesterday
    ├─ Assert: count ≥ yesterday's count
    └─ Assert: schema_version matches
        ↓
  decide_retrain (Branch)
    ├─ Query: fraud_rate for yesterday
    ├─ Check: Is fraud_rate > 5% OR < 0.1%?
    ├─ YES → continue to retrain_model
    └─ NO  → skip_retrain → promote_gold
        ↓
  retrain_model (SparkSubmitOperator)
    ├─ Load: /data/delta/silver (yesterday's data)
    ├─ Filter: 10K-100K rows
    ├─ Prepare: features, labels, train/test split
    ├─ Train: XGBoost (300 trees, max_depth=6)
    ├─ Evaluate: ROC-AUC, PR-AUC, Precision, Recall
    ├─ Log to MLflow: metrics, params, artifacts
    └─ Save: models/fraud_model.pkl (shared volume)
        ↓
  evaluate_model
    ├─ Load: current production model
    ├─ Load: newly trained model
    ├─ Compare: metrics (PR-AUC improvement)
    ├─ Decision: PR-AUC_new > PR-AUC_prod + 2% ?
    ├─ YES → promote_to_production
    └─ NO  → keep_in_staging
        ↓
  promote_to_gold
    ├─ Update: model_registry PostgreSQL table
    ├─ Update: MLflow run to "Production" stage
    ├─ Update: fraud_model.pkl is loaded by Spark next micro-batch
    └─ Action: Previous model → "Archived"
        ↓
  send_notification
    ├─ Slack message with metrics
    ├─ Email to data team
    └─ Log to PostgreSQL audit_log
        ↓
  DAG Complete
```

### Data Quality Monitoring Flow

```
Time: Every hour at :00 (e.g., 13:00, 14:00, 15:00)

  Airflow Scheduler triggers DAG
        ↓
  [Parallel tasks]
  
  Task 1: bronze_row_count_check
    ├─ Query: SELECT COUNT(*) FROM bronze WHERE ingested_at > NOW() - 1h
    ├─ Assert: count > 0
    ├─ Result: PASS/FAIL + row_count
    └─ Log to: dq_checks table
  
  Task 2: bronze_schema_check
    ├─ Query: DESCRIBE bronze
    ├─ Assert: columns match expected schema
    ├─ Assert: column types match
    ├─ Result: PASS/FAIL + mismatches
    └─ Log to: dq_checks table
  
  Task 3: silver_null_rate_check
    ├─ Query: SELECT COUNT(*)/(total)*100 as null_pct FROM silver
    ├─ Assert: null_pct < 2%
    ├─ Alert: if null_pct > 2%
    ├─ Per-column breakdown
    └─ Log to: dq_checks table
  
  Task 4: fraud_rate_check
    ├─ Query: fraud_rate = COUNT(fraud=1) / COUNT(*) FROM silver
    ├─ Assert: fraud_rate in [0.1%, 10%]
    ├─ Alert: if outside range (possible data issue)
    └─ Log to: dq_checks table
  
  Task 5: gold_consistency_check
    ├─ Query: fraud_score distribution (min, max, avg)
    ├─ Assert: fraud_score in [0, 1]
    ├─ Query: fraud_flags validity
    ├─ Result: PASS/FAIL
    └─ Log to: dq_checks table
        ↓
  consolidate_dq_report
    ├─ Aggregate: all check results
    ├─ Calculate: DQ score (% of checks passing)
    ├─ Alert if: DQ score < 95%
    ├─ Log: summary to dq_checks table
    └─ Update: Grafana dashboard (via PostgreSQL)
```

---

## Design Decisions

### Decision 1: Lambda Architecture (Batch + Real-Time)

**Question**: Why not just Kappa (streaming only)?

**Decision**: Lambda (streaming + batch)

**Rationale**:
- **Real-time path** (streaming): Immediate fraud flagging for customer action
- **Batch path** (daily ML): Model improvement without impacting production
- **Decoupling**: ML training doesn't interfere with scoring
- **Flexibility**: Can swap ML algorithms without affecting streaming
- **Safety**: Batch jobs can validate data before updating production model

**Trade-off**: Operational complexity (2 code paths) vs. reliability & flexibility

---

### Decision 2: Medallion Architecture (Bronze-Silver-Gold)

**Question**: Why 3 layers? Why not just 1?

**Decision**: 3 layers (Medallion Pattern)

**Rationale**:
- **Bronze** (Raw): Preserves exact source data for audit/replay
- **Silver** (Enriched): Transforms raw into ML-ready features
- **Gold** (Business): Curated for specific use cases (fraud flagging)

**Benefits**:
- Reproducibility: Can regenerate Silver/Gold from Bronze anytime
- Debugging: Can trace issues back to source
- Compliance: Immutable audit trail
- Performance: Each layer optimized for its purpose

**Alternative**: Single layer (rejected because less auditable)

---

### Decision 3: Spark Structured Streaming Over Apache Flink

**Question**: Why Spark and not Flink?

**Decision**: Apache Spark Structured Streaming

**Rationale**:
- **Unified API**: Same code for batch (ML training) and streaming
- **Delta Lake integration**: ACID guarantees out of the box
- **Ecosystem**: Works with MLlib, scikit-learn, XGBoost
- **Operational**: Easier to find Spark expertise
- **Cost**: Shared infrastructure (vs. separate Flink cluster)

**Trade-offs**:
- 10-second micro-batches (vs. Flink's true streaming)
- Slightly higher latency (acceptable for fraud: 10s < 1min threshold)
- Less flexible windowing (but sufficient for use case)

---

### Decision 4: XGBoost Over Deep Learning

**Question**: Why XGBoost? What about neural networks?

**Decision**: XGBoost (gradient boosting)

**Rationale**:
- **Interpretability**: Can explain why flagged (feature importance)
- **Performance**: Near state-of-the-art on tabular data
- **Training**: Fast (<30 min on 100K rows)
- **Inference**: <1ms per prediction (vs. neural nets ~50ms)
- **Data efficiency**: Works well with smaller datasets
- **No GPUs needed**: Runs on CPU in Spark

**Considered alternatives**:
- Deep Learning: Too slow for inference, hard to interpret
- Logistic Regression: Too simple, doesn't capture patterns
- Random Forests: Good, but XGBoost more accurate
- AutoML: Too black-box for fraud (need explainability)

---

### Decision 5: Kafka for Event Streaming

**Question**: Why Kafka? What about AWS Kinesis/GCP Pub/Sub?

**Decision**: Apache Kafka

**Rationale**:
- **Replay capability**: Can reprocess events from any offset
- **Partitioning**: Natural parallelism for Spark consumers
- **Decoupling**: Producer/consumer independent
- **Self-hosted**: No vendor lock-in
- **Ecosystem**: Integration with Spark, Airflow, etc.

**Trade-offs**:
- Operational overhead (managing ZooKeeper, broker)
- Not managed (vs. cloud options)
- For dev/test: acceptable (learning value)

---

### Decision 6: Daily Retraining vs. Online Learning

**Question**: Why daily batch retraining? Why not continuous updates?

**Decision**: Daily batch retraining at 00:00 UTC

**Rationale**:
- **Stability**: Model doesn't change mid-day (confuses analysts)
- **Safety**: Can review/validate before promoting
- **Debugging**: Known points to check metrics
- **Simplicity**: Scheduled, no edge cases
- **Auditability**: Clear history of which model was active when

**Concept drift handling**:
- Monitor fraud rate hourly for anomalies
- If detected, alert analysts (they may trigger retrain manually)
- Scheduled retrain catches gradual drift

**Alternative considered** (online learning): Rejected because harder to debug/audit

---

### Decision 7: Rule Score + ML Score Ensemble

**Question**: Why combine both? Why not just one?

**Decision**: Ensemble: `fraud_score = max(rule_score, ml_score)`

**Rationale**:
- **Coverage**: Rules catch known patterns immediately (even before ML model trained)
- **Interpretability**: Analysts understand rules ("why was this flagged?")
- **Safety**: Rules as guardrail (catch what ML missed)
- **Bootstrap**: System works on day 1 (before ML model trained)
- **Flexibility**: Can tune rules vs. ML independently

**Ensemble strategy** (max):
- If either scores high → flag
- Balanced: respects both signals
- Conservative (fewer false negatives)

---

## Scalability & Performance

### Performance Targets

```
Throughput:
  • Steady state: 100 TPS (transactions per second)
  • Peak burst: 500 TPS (handled without data loss)
  • Stress test: 1000 TPS (graceful degradation)

Latency:
  • End-to-end: < 10 seconds (raw event → flagged output)
  • Spark batch processing: < 5 seconds
  • Rule scoring: < 50ms per transaction
  • ML scoring: < 100ms per 100 transactions

Accuracy:
  • ROC-AUC: ≥ 0.85
  • PR-AUC: ≥ 0.70 (class imbalance)
  • Precision: ≥ 0.80 (avoid analyst fatigue)
  • Recall: ≥ 0.80 (catch fraud)

Availability:
  • Uptime: ≥ 99.5%
  • Mean time to recovery (MTTR): < 30 minutes
  • Data loss: 0 (exactly-once semantics)
```

### Scaling Strategy

```
Horizontal Scaling:
  ├─ Kafka partitions: Increase from 3 → 10 → 30
  │   └─ Add more Spark workers (1 worker per 3 partitions)
  ├─ Spark executors: Add memory/cores
  ├─ PostgreSQL sharding: By user_id hash
  └─ Grafana replicas: Load balance dashboards

Vertical Scaling:
  ├─ Spark worker memory: 8GB → 16GB → 32GB
  ├─ PostgreSQL: Larger instances, better disk I/O
  └─ Kafka brokers: More disk space, faster disks

Caching Strategy:
  ├─ Merchant risk scores: Cache lookup table in memory
  ├─ User profiles: Broadcast variable in Spark
  ├─ Model artifact: Load once, share across executors
  └─ State store: Managed by Spark (automatic)

Optimization:
  ├─ Partition pruning: Filter early in DAG
  ├─ Columnar storage: Delta Lake compression
  ├─ Index creation: PostgreSQL B-tree on (user_id, date)
  ├─ Query caching: Grafana dashboard caching
  └─ Checkpointing: Optimize checkpoint format
```

### Bottleneck Analysis

```
Potential Bottlenecks (Priority Order):

1. Spark state store (window aggregations)
   │ Risk: Large state over time
   │ Mitigation: Limit window to 7 days, compress state
   │ Monitor: Checkpoint file size growth
   
2. PostgreSQL writes (fraud_metrics table)
   │ Risk: High write volume (100 TPS × inserts)
   │ Mitigation: Batch inserts, async writes
   │ Monitor: Write latency, queue depth
   
3. ML model inference latency
   │ Risk: 100+ predictions per batch
   │ Mitigation: Batch inference, optimize model
   │ Monitor: Average latency per batch
   
4. Kafka network I/O
   │ Risk: High throughput (100 TPS)
   │ Mitigation: Partition across brokers
   │ Monitor: Broker CPU, network saturation
   
5. Spark task scheduling
   │ Risk: Too many partitions → too many tasks
   │ Mitigation: Tune partition size (128MB optimal)
   │ Monitor: Task count, scheduling delay
```

---

## Security & Compliance

### Data Security

```
At Rest:
  ├─ Delta Lake: Encryption at rest (TDE)
  ├─ PostgreSQL: Encrypted storage
  ├─ Kafka: Persistence layer encrypted
  └─ Backups: Encrypted snapshots

In Transit:
  ├─ Kafka: SSL/TLS encryption (port 9093)
  ├─ PostgreSQL: SSL connections
  ├─ Spark: TLS for node communication
  └─ Grafana: HTTPS only

Access Control:
  ├─ PostgreSQL: Role-based access
  │   ├─ read_only: analysts (SELECT only)
  │   ├─ write: spark jobs (INSERT, UPDATE)
  │   └─ admin: DBA (all permissions)
  ├─ Kafka: ACLs per topic
  │   ├─ producer: can write raw-transactions only
  │   ├─ consumer: can read flagged-transactions only
  │   └─ admin: manage topics
  ├─ Airflow: RBAC
  │   ├─ viewer: see DAGs, not modify
  │   ├─ editor: modify DAGs
  │   └─ admin: full access
  └─ Grafana: Admin/Editor/Viewer roles
```

### Compliance & Audit

```
Regulatory Requirements:
  ├─ PCI-DSS: Payment Card Industry
  │   ├─ Requirement: Encrypt sensitive data
  │   ├─ Implementation: TLS + encrypted storage
  │   └─ Audit: Monthly review of access logs
  │
  ├─ GDPR/Privacy: Data protection
  │   ├─ Right to be forgotten: Delete user from Bronze
  │   ├─ Data minimization: Only needed fields
  │   └─ Audit trail: PostgreSQL audit_log table
  │
  └─ AML (Anti-Money Laundering):
      ├─ Transaction monitoring: Flagged alerts
      ├─ Reporting: Suspicious activity reports
      └─ Retention: 5+ years of data

Audit Trail:
  ├─ Bronze layer: Immutable source
  ├─ Model versions: MLflow run ID → decision
  ├─ Flagged transactions: reason, timestamp, who reviewed
  ├─ DAG executions: task logs, status
  └─ Access logs: who accessed what, when

Monitoring:
  ├─ Log all model predictions to Bronze
  ├─ Track false positives rate (analyst review)
  ├─ Monitor for model drift (weekly reports)
  └─ Compliance score: % transactions reviewed
```

---

## Deployment Topology

### Development (Docker Compose - Single Machine)

```
┌─ Docker Engine ─────────────────────────────────────────────┐
│                                                              │
│  Services (running on localhost):                           │
│  ├─ zookeeper:2181                                         │
│  ├─ kafka:9092, kafka:29092                                │
│  ├─ kafka-ui:8080                                          │
│  ├─ spark-master:8081, spark-worker (2x)                   │
│  ├─ airflow-webserver:8082, airflow-scheduler              │
│  ├─ mlflow:5001                                             │
│  ├─ postgres:5432                                           │
│  ├─ grafana:3000                                            │
│  └─ pgadmin:5050                                            │
│                                                              │
│  Volumes (Docker):                                          │
│  ├─ /data/delta/bronze, silver, gold                       │
│  ├─ /data/checkpoints/                                      │
│  ├─ /models/                                                │
│  ├─ /airflow/dags/                                          │
│  └─ /logs/                                                  │
│                                                              │
│  Network: fraud-detection-net (bridge)                      │
│  All services can communicate via hostname                  │
└─────────────────────────────────────────────────────────────┘

Use Case:
  • Learning & development
  • Local testing
  • Demonstration
  • Portfolio project
```

### Production (Kubernetes - Multi-Node)

```
┌─ Kubernetes Cluster (3-5 nodes) ──────────────────────────┐
│                                                             │
│  Namespace: fraud-detection                               │
│                                                             │
│  StatefulSets:                                            │
│  ├─ kafka-brokers (3 replicas, replication factor 3)     │
│  ├─ zookeeper (3 replicas)                               │
│  ├─ postgres (1 replica, persistent volume)              │
│  └─ mlflow (1 replica, persistent volume)                │
│                                                             │
│  Deployments:                                             │
│  ├─ spark-driver (1 replica)                             │
│  ├─ spark-workers (auto-scale 2-10)                      │
│  ├─ airflow-scheduler (1 replica)                        │
│  ├─ airflow-webserver (2 replicas, load balanced)        │
│  ├─ airflow-workers (auto-scale 2-5)                     │
│  └─ grafana (2 replicas, load balanced)                  │
│                                                             │
│  Persistent Volumes:                                      │
│  ├─ /data/delta/ (NFS for multi-node access)             │
│  ├─ /data/checkpoints/ (NFS)                             │
│  ├─ /models/ (NFS)                                        │
│  ├─ postgres-data (EBS/GCP persistent disk)             │
│  └─ mlflow-artifacts (cloud object storage)              │
│                                                             │
│  Services:                                                │
│  ├─ kafka: ClusterIP (internal only)                     │
│  ├─ postgres: ClusterIP                                  │
│  ├─ mlflow: ClusterIP                                    │
│  ├─ airflow: LoadBalancer (external access)             │
│  ├─ grafana: LoadBalancer (external access)             │
│  └─ spark-ui: NodePort (debugging)                       │
│                                                             │
│  ConfigMaps:                                              │
│  ├─ spark-config (memory, cores)                         │
│  ├─ airflow-config (DAG location, executor)              │
│  └─ app-config (TPS, thresholds)                         │
│                                                             │
│  Secrets:                                                 │
│  ├─ db-credentials (postgres password)                   │
│  ├─ kafka-credentials (SSL certs)                        │
│  └─ grafana-credentials (admin password)                 │
│                                                             │
│  Monitoring:                                              │
│  ├─ Prometheus (metrics collection)                      │
│  └─ Loki (log aggregation)                               │
│                                                             │
└─────────────────────────────────────────────────────────────┘

Deployment Process:
  1. Build Docker images for all services
  2. Push to container registry (ECR/Docker Hub)
  3. Deploy infrastructure (Terraform/Helm)
  4. Run smoke tests
  5. Enable monitoring & alerts
  6. Start ingestion pipeline
```

---

## Future Enhancements

### Phase 2: Advanced ML

```
Feature Engineering Pipeline:
  ├─ Add deep learning embeddings (transaction sequences)
  ├─ Temporal patterns (Fourier features)
  ├─ Network analysis (user-merchant graphs)
  └─ External data: IP geolocation, device fingerprinting

Model Improvements:
  ├─ Ensemble: Combine XGBoost + Random Forest + LightGBM
  ├─ Bayesian optimization for hyperparameters
  ├─ Online learning (incremental model updates)
  ├─ Anomaly detection (Isolation Forest for outliers)
  └─ Causal inference (understand fraud drivers)

Model Governance:
  ├─ Automated model validation (test suite)
  ├─ A/B testing framework (compare models in production)
  ├─ Model monitoring dashboard (drift detection)
  └─ Fairness metrics (check for bias)
```

### Phase 3: Real-Time Features

```
Instead of 10-second micro-batches → True streaming:
  ├─ Switch to Kafka Streams or Flink
  ├─ Instant feature updates (sub-second)
  ├─ Lower latency (< 1 second end-to-end)
  ├─ More complex stateful processing
  └─ Trade-off: more operational complexity

Distributed Features:
  ├─ Feature store (Feast/Tecton)
  ├─ Centralized feature management
  ├─ Feature versioning & lineage
  ├─ Online vs. offline feature serving
  └─ Faster iteration for data scientists
```

### Phase 4: Explainability & Interpretability

```
Model Explainability:
  ├─ SHAP values for each prediction
  ├─ Feature contribution breakdown
  ├─ "Why was this transaction flagged?" dashboard
  ├─ Counterfactual explanations
  └─ Regulatory reporting automation

Analyst Tools:
  ├─ Investigation dashboard (drill into flagged transactions)
  ├─ Feedback loop (analysts provide ground truth)
  ├─ Case management (assign, track, close)
  ├─ Pattern discovery (identify new fraud types)
  └─ Threshold optimization (ROC curve tuning)
```

### Phase 5: Integration & Automation

```
External System Integration:
  ├─ Bank API: Push flagged transactions for action
  ├─ Payment gateway: Block/challenge transactions
  ├─ CRM: Update customer risk score
  ├─ Incident management: Auto-create tickets
  └─ Ticketing system: Integration for analyst workflow

Automation:
  ├─ Auto-challenge: For medium-risk transactions
  ├─ Auto-block: For high-confidence fraud
  ├─ Auto-refund: For confirmed fraud (with approval)
  └─ Auto-notify: Customer alerts for suspicious activity
```

---

## Architecture Summary

### Key Design Patterns Used

| Pattern | Implementation | Benefit |
|---------|-----------------|---------|
| **Lambda** | Batch ML + Streaming scoring | Flexibility & reliability |
| **Medallion** | Bronze-Silver-Gold layers | Auditability & reproducibility |
| **Event Sourcing** | Immutable Bronze layer | Replay & compliance |
| **Micro-services** | Kafka-decoupled components | Independent scaling |
| **Idempotency** | Exactly-once semantics | No duplicates |
| **Checkpointing** | Spark state recovery | Fault tolerance |
| **Feature Store** | Computed once in Silver | ML-ready features |
| **Ensemble** | Rule + ML scoring | Coverage & interpretability |

### Success Criteria Met

✅ **Real-time fraud detection** (< 10 second latency)
✅ **Scalable to 100+ TPS** (with horizontal scaling to 1000 TPS)
✅ **Explainable decisions** (rule-based + feature importance)
✅ **Auditable trail** (immutable Bronze layer)
✅ **Reproducible ML** (versioned models, experiment tracking)
✅ **Observable system** (comprehensive monitoring)
✅ **Resilient infrastructure** (automatic recovery, no data loss)
✅ **Compliant** (PCI-DSS, GDPR, AML requirements)

---

## Conclusion

This architecture provides a **production-ready, scalable, auditable fraud detection system** that balances:
- **Real-time responsiveness** (immediate flagging)
- **ML sophistication** (ensemble scoring)
- **Operational reliability** (fault tolerance, recovery)
- **Business transparency** (explainability, audit trails)
- **Regulatory compliance** (security, data protection)

The design is flexible enough to support future enhancements (Phase 2-5) while remaining simple enough to implement and operate.

---

**Document Version**: 1.0  
**Last Updated**: May 14, 2026  
**Author**: Data Architect (Khurshid Normurodov)  
**Status**: Production Ready

For questions or updates, contact the Data Architecture team.
