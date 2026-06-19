# Team Member Task Summary Cards

Quick reference for each team member's responsibilities and deliverables.

---

## 🎯 Khurshid Normurodov - Project Lead / Data Architect

**Title**: Project Lead / Data Architect / Product Owner  
**Responsibility**: Overall design, GitHub admin, MS Teams owner, milestone tracking

### Core Tasks

| Week | Task Group | Deliverable | Status |
|------|-----------|-------------|--------|
| 1 | 1.1 | GitHub repo setup (branches, templates, CI) | ✅ Complete  |
| 1 | 1.2 | docker-compose.yml orchestration | ✅ Complete  |
| 1 | 1.4 | Dataset acquisition & EDA | ✅ Complete  |
| 2-6 | All | Project coordination & oversight | ✅ Complete  |

### Key Responsibilities
- ✅ Create GitHub repository with:
  - Branch protection (main requires 1 approval + CI)
  - PR templates, issue templates
  - GitHub Projects board (Kanban)
  - Team member permissions
  
- ✅ Architect overall system design
  
- ✅ Create docker-compose.yml with 13 services
  
- ✅ Create Makefile with shortcuts
  
- ✅ Acquire/validate synthetic fraud dataset (100K+ records)
  
- ✅ Coordinate team progress & track milestones
  
- ✅ Lead code reviews & architectural decisions

### Collaboration Points
- **Elif**: Docker setup & environment variables
- **Farzaneh**: Dataset validation & testing
- **Hontar**: Architecture review & integration
- **All**: Weekly sync meetings (Mon & Thu)

### Definition of Done
- GitHub repo accessible by all team members
- docker-compose.yml brings up all 13 services cleanly
- Dataset loaded with EDA complete
- Team can run `make up` successfully

### Timeline
```
Week 1: GitHub setup + Docker + Dataset (40 hours)
Week 2-6: Coordination + oversee integration (20 hours/week)
Total: ~140 hours
```

### Success Metrics
- All team members productive on first day
- No blocking architectural issues
- Daily standup cadence maintained
- All PRs merged within 48 hours

---

## 📨 Farzaneh Barzegar - Data Ingestion Engineer

**Title**: Data Ingestion Engineer / Development Team  
**Responsibility**: Kafka cluster setup, PaySim simulator, raw event pipeline

### Core Tasks

| Week | Task Group | Deliverable | Status |
|------|-----------|-------------|--------|
| 1 | 1.4 | Dataset validation | ✅ Complete  |
| 2 | 2.1 | Kafka Producer | ✅ Complete  |
| 3-6 | Support | Producer enhancements & support | ✅ Complete  |

### Key Responsibilities
- ✅ Create transaction_generator.py (Kafka producer)
  - Load synthetic_fraud_dataset.csv
  - Serialize to JSON
  - Configure TPS (transactions per second)
  - Send to Kafka topic `raw-transactions`
  
- ✅ Create Dockerfile for producer
  
- ✅ Implement fraud injection logic (optional):
  - 5 synthetic fraud patterns
  - Configurable fraud rate
  
- ✅ Create producer/requirements.txt
  
- ✅ Add comprehensive logging & error handling
  
- ✅ Create unit tests (tests/test_producer.py)
  
- ✅ Validate data flow to Kafka
  
- ✅ Document producer README.md

### Kafka Topics You Manage
```
raw-transactions (3 partitions) ← Your producer writes here
  └─→ Spark streaming reads ← Hontar consumes
```

### Collaboration Points
- **Khurshid**: Dataset validation
- **Hontar**: Kafka schema agreement, testing pipeline
- **Elif**: Docker image verification

### Definition of Done
- Producer sends 10-100 TPS to Kafka
- Messages are valid JSON with correct schema
- Graceful error handling & logging
- Handles Kafka connection failures
- Unit tests passing
- README documentation complete

### Timeline
```
Week 1: Dataset validation (8 hours)
Week 2: Producer implementation (24 hours)
Week 3-6: Support & enhancement (16 hours)
Total: ~48 hours
```

### Success Metrics
- Producer sustains 100 TPS without data loss
- Kafka messages are properly formatted
- Logging shows transaction counts every minute
- Zero crashed producer instances (in production)
- Code coverage ≥ 80%

---

## 🔄 Hontar Daniil - Data Processing & ML Engineer

**Title**: Data Processing & ML Engineer / Development Team  
**Responsibility**: Spark Streaming, feature engineering, XGBoost fraud scorer, MLflow

### Core Tasks

| Week | Task Group | Deliverable | Status |
|------|-----------|-------------|--------|
| 2 | 2.2 | Bronze Layer (schema enforcement) | ✅ Complete  |
| 2 | 2.3 | Silver Layer (features + rule score) | ✅ Complete  |
| 2 | 2.4 | ML Score integration | ✅ Complete  |
| 2-3 | 2.5 | Gold Layer (flagging + output) | ✅ Complete  |
| 3 | 3.1 | XGBoost ML training | ✅ Complete  |
| 3 | 3.2 | MLflow tracking & registry | ✅ Complete  |
| 3 | 3.3 | Model evaluation & promotion | ✅ Complete  |
| 4 | 6.1 | Unit tests for streaming | ✅ Complete  |
| 5 | 6.4 | Performance & load testing | ✅ Complete  |

### Key Responsibilities

#### Spark Streaming Job (fraud_streaming_job.py)
**Bronze Layer**:
- Schema enforcement on raw transactions
- Append-only writes to Delta Lake
- No transformations (raw preservation)

**Silver Layer** (10 features + 5 flags):
```
Features:
  - log_amount, event_hour, amount_bucket
  - merchant_risk_score, day_of_week, is_weekend
  - transaction_count_today, amount_z_score
  - hour_of_transaction, merchant_category

Flags:
  - flag_high_amount (amount > $3,000)
  - flag_velocity (daily count ≥ 6)
  - flag_off_hours (2-4 AM transactions)
  - flag_geo_anomaly (distance > 75km)
  - flag_risky_merchant (crypto, gambling, etc.)

Rule Score = weighted sum of flags
```

**ML Scoring**:
- Load fraud_model.pkl (if exists)
- Compute XGBoost prediction
- Fallback to 0.0 if model missing

**Gold Layer**:
- Filter is_flagged = 1
- Write to 3 outputs:
  - Delta Lake (/data/delta/gold)
  - Kafka (flagged-transactions topic)
  - PostgreSQL (fraud_metrics table)

#### ML Training (train_model.py)
- Load Silver data (or CSV for bootstrap)
- Feature selection (10 features)
- Handle class imbalance (scale_pos_weight)
- Train XGBoost with hyperparameters:
  - n_estimators=300, max_depth=6, learning_rate=0.05
- Compute metrics: ROC-AUC, PR-AUC, Precision, Recall, F1
- Save model to models/fraud_model.pkl

#### MLflow Tracking
- Log metrics, parameters, feature importances
- Create experiment: "fraud-detection"
- Create runs for each training
- Model versioning: Staging → Production → Archived

#### Model Evaluation (evaluate_model.py)
- Compare trained model vs production
- Promotion decision: PR-AUC improvement ≥ 2%?
- Update PostgreSQL model_registry table

### Data Layers You Own
```
Raw Transactions (from Kafka)
  ↓ Bronze Layer ↓
  Raw events (append-only)
  ↓ Silver Layer ↓
  Features + Scoring (10 features + 5 flags)
  ↓ Gold Layer ↓
  Flagged transactions (is_flagged=1)
  ↓ Outputs ↓
  Delta Lake + Kafka + PostgreSQL
```

### Collaboration Points
- **Farzaneh**: Kafka schema validation, message format
- **Elif**: PostgreSQL write operations, Airflow integration
- **Khurshid**: Architecture review, model strategy

### Definition of Done
- Spark job processes all 3 layers correctly
- All features computed with no nulls
- Rule scoring working (0-1 range)
- ML model training: ROC-AUC ≥ 0.85, PR-AUC ≥ 0.70
- MLflow tracking 100% of experiments
- Gold layer outputs to 3 sinks
- Unit tests: 80%+ coverage
- Load test: 100 TPS sustained
- Performance: <100ms per batch

### Timeline
```
Week 2: Bronze + Silver + ML Score (32 hours)
Week 2-3: Gold Layer + ML Training (24 hours)
Week 3: MLflow + Evaluation (16 hours)
Week 4-6: Testing + Support (24 hours)
Total: ~96 hours
```

### Success Metrics
- Streaming pipeline processes 100+ TPS
- Feature engineering latency < 50ms/batch
- ML model PR-AUC ≥ 0.75
- No data loss or duplicates
- All outputs (Delta, Kafka, PostgreSQL) in sync
- Load test: handle 500 TPS spike
- Model retraining completes in < 30 minutes

---

## 📊 Elif Sila Okutucu - Analytics & DevOps Engineer / Scrum Master

**Title**: Analytics & DevOps Engineer / Scrum Master  
**Responsibility**: Airflow DAGs, Delta Lake layers, Grafana dashboards, Docker

### Core Tasks

| Week | Task Group | Deliverable | Status |
|------|-----------|-------------|--------|
| 1 | 1.2 | Docker image & environment setup | ✅ Complete |
| 1 | 1.3 | PostgreSQL initialization and validation | ✅ Complete |
| 4 | 4.1 | Airflow setup & connections | ✅ Complete |
| 4 | 4.2 | Daily Retraining DAG validation | ✅ Complete |
| 4 | 4.3 | Hourly DQ Monitoring DAG validation | ✅ Complete |
| 4 | 4.4 | PostgreSQL Integration | ✅ Complete |
| 5 | 5.1 | Grafana Fraud Alerts Monitoring Dashboard | ✅ Complete |
| 6 | 6.3 | Monitoring documentation and evidence collection | ✅ Complete |

### Key Responsibilities

#### Docker & Environment (Week 1)
- Create Dockerfile for custom Spark image
- Create docker-compose with 13 services:
  - Kafka + ZooKeeper
  - Spark Master + Workers
  - Airflow (scheduler, webserver, worker)
  - MLflow, PostgreSQL, Grafana
- Create .env.example with all variables
- Document resource requirements
- Test on clean machine

#### PostgreSQL Setup (Week 1)
- Create init_postgres.sql with:
  - 3 databases: airflow, mlflow, fraud
  - fraud_metrics table
  - dq_checks table
  - model_registry table
- Document table schemas
- Create sample queries
- Test Spark + Airflow connections

#### Airflow Setup (Week 4)
- Configure Airflow home directory
- Create connections: PostgreSQL, Spark, MLflow
- Create dags/config.yaml with parameters
- Document setup & access

#### Airflow DAGs (Week 4)

**Daily Retraining DAG** (fraud_detection_daily_dag.py):
```
fraud_detection_daily (00:00 UTC, daily)
├── validate_silver_data (row count ≥ yesterday)
├── decide_retrain (branch: retrain needed?)
├── retrain_model (SparkSubmitOperator)
├── evaluate_model (compare metrics)
├── promote_to_gold (if improved)
└── send_notification (Slack/Email)
```

**Hourly DQ Monitoring DAG** (data_quality_monitoring_dag.py):
```
data_quality_monitoring (:00 hourly)
├── bronze_row_count_check
├── bronze_schema_check
├── silver_null_rate_check
├── fraud_rate_check
├── gold_consistency_check
└── consolidate_dq_report → PostgreSQL
```

#### Grafana Dashboards (Week 5)

**Main Dashboard** (fraud_overview.json):
- KPI Cards: Total txns, flagged, fraud rate, accuracy
- Time Series: Flagged/hour, fraud rate trend, model performance
- Heatmaps: Fraud by hour-of-day, by merchant
- Data Quality: DQ pass rate, null rate, record count
- Model: Feature importance, ROC-AUC history, PR-AUC history

**Optional Dashboards**:
- fraud_details.json (transaction explorer)
- model_performance.json (experiment comparison)
- dq_monitoring.json (pipeline health)

#### Monitoring & Alerting
- Create Grafana alert rules:
  - Fraud rate spike (> 5%)
  - Pipeline latency (> 30s)
  - DQ check failure
  - Model performance degradation

### Services You Manage
```
Docker Compose Services:
├── Kafka + ZooKeeper
├── Spark Master + Workers
├── Airflow (scheduler, webserver, worker)
├── MLflow
├── PostgreSQL ← Primary responsibility
└── Grafana ← Primary responsibility
```

### Collaboration Points
- **Khurshid**: Docker-compose.yml review, environment variables
- **Hontar**: DAG task specifications, PostgreSQL write schemas
- **Farzaneh**: Testing producer in Docker
- **All**: Airflow connection verification

### Definition of Done
- docker-compose up starts all services in 2 minutes
- PostgreSQL has correct schema & permissions
- Airflow daily DAG runs successfully at 00:00 UTC
- DQ DAG runs hourly without failure
- Grafana shows live data (auto-refresh 30s)
- All panels query PostgreSQL correctly
- Alerts trigger on threshold breach
- Documentation complete & tested

### Timeline
```
Week 1: Docker + PostgreSQL (24 hours)
Week 4: Airflow setup + DAGs (40 hours)
Week 5: Grafana dashboards (32 hours)
Week 6: Monitoring & support (16 hours)
Total: ~112 hours
```

### Success Metrics
- 100% system uptime (99.5% target)
- All DAG SLAs met (30 min per task)
- Daily retraining completes in < 2 hours
- DQ checks complete in < 10 minutes
- Grafana dashboards load in < 2 seconds
- All service health checks pass
- Zero alert noise (< 5 false positives/week)

---

## 📋 Task Distribution Summary

### By Week

```
Week 1 (Infrastructure - 80 hours)
├─ Khurshid: GitHub + Docker + Dataset (40h)
├─ Elif: Docker images + PostgreSQL (24h)
└─ Farzaneh: Data validation (8h)

Week 2-3 (Streaming & ML - 120 hours)
├─ Farzaneh: Producer enhancements (8h)
├─ Hontar: All streaming layers + ML (96h)
└─ Khurshid: Architecture support (16h)

Week 4 (Orchestration - 80 hours)
├─ Elif: Airflow + DAGs (56h)
├─ Hontar: ML integration support (16h)
└─ Khurshid: Oversight (8h)

Week 5 (Analytics - 80 hours)
├─ Elif: Grafana dashboards (48h)
├─ Hontar: Tests + performance (24h)
└─ Khurshid: Coordination (8h)

Week 6 (Documentation & Testing - 64 hours)
├─ All: Component documentation (32h)
├─ Hontar: Load testing (16h)
└─ Khurshid: Final coordination (16h)

Total: 424 hours (~18 hours/week per person avg)
```

### By Person

| Person | Week 1 | Week 2-3 | Week 4 | Week 5 | Week 6 | Total |
|--------|--------|----------|--------|--------|--------|-------|
| Khurshid | 40h | 16h | 8h | 8h | 16h | 88h |
| Farzaneh | 8h | 32h | 0h | 8h | 8h | 56h |
| Hontar | 0h | 96h | 16h | 24h | 16h | 152h |
| Elif | 24h | 0h | 56h | 48h | 16h | 144h |
| **Total** | **72h** | **144h** | **80h** | **88h** | **56h** | **440h** |

---

## 🔄 Critical Path Dependencies

```
1. GitHub Repo Created (Khurshid)
   ↓
2. Docker Setup + PostgreSQL (Khurshid, Elif)
   ├─→ Dataset Ready (Khurshid, Farzaneh)
   └─→ Kafka Producer (Farzaneh)
       ↓
3. Spark Streaming (Hontar)
   ├─→ Bronze/Silver/Gold Layers
   └─→ ML Training + MLflow (Hontar)
       ↓
4. Airflow DAGs (Elif) + Integration (Hontar)
   ├─→ Daily Retraining
   └─→ Hourly DQ Checks
       ↓
5. Grafana Dashboards (Elif)
   ├─→ Live Monitoring
   └─→ Alerting Rules
       ↓
6. Testing (All) + Documentation (All)
   ├─→ Unit Tests
   ├─→ Integration Tests
   ├─→ Load Tests
   └─→ Component README + Team Documentation
```

---

## ✅ Weekly Checklist for Each Person

### Khurshid's Checklist
- Week 1: GitHub repo created + Docker runs + Dataset loaded
- Week 2-3: Code reviews + merge PRs + unblock team
- Week 4: DAG review + model strategy + ML integration
- Week 5: Dashboard review + alert config + monitoring
- Week 6: Documentation review + final testing + release prep

### Farzaneh's Checklist
- Week 1: Dataset validation + data quality checks
- Week 2: Producer implementation + logging + error handling
- Week 3: Producer enhancements + optional fraud injection
- Week 4-6: Support + testing + documentation

### Hontar's Checklist
- Week 2: Bronze + Silver layers implemented
- Week 3: Gold layer + ML training + MLflow
- Week 4: ML integration + Airflow support
- Week 5: Unit tests + load tests + performance tuning
- Week 6: Load testing completion + documentation

### Elif's Checklist
- Week 1: Dockerfile + docker-compose + PostgreSQL
- Week 4: Airflow setup + daily DAG + DQ DAG
- Week 5: Grafana + dashboards + alerts
- Week 6: Monitoring + documentation + support

---

## 📞 Communication Cadence

**Daily** (async):
- Slack updates (end of day) in #fraud-detection-project
- GitHub PR reviews (target 24h)

**Twice Weekly** (sync):
- Monday 10:00 UTC: Sprint planning & blockers
- Thursday 15:00 UTC: Progress check & course correction

**As Needed**:
- Architecture discussions (Khurshid leads)
- Technical deep dives (owner leads)
- Emergency incidents (entire team)

---

**Created**: May 13, 2026  
**For Team**: Khurshid, Farzaneh, Hontar, Elif
