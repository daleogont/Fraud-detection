# Weekly Reference Cards - Final Project Reference

---

## 📌 KHURSHID NORMURODOV - Weekly Checklist

**Role**: Initial Project Architect (Phase 1)
**Total Project Hours**: 88 hours (~16-18 hours/week)

### Week 1 - Infrastructure Setup (40 hours)
```
MON: GitHub repo creation + team setup
TUE-WED: Docker compose refinement
THU: Dataset acquisition & EDA
FRI: Week 1 review + Week 2 prep

✅ DONE BY FRIDAY:
□ GitHub repo created with all settings
□ Team members have access
□ docker-compose.yml working
□ Dataset loaded & validated
□ Team can run 'make up' successfully
```

### Week 2-3 - Oversight & Support (16 hours total)
```
MON: Review Farzaneh's producer PR
MON: Review Hontar's Bronze layer
WED: Architecture checkpoint
FRI: Integration review
```

### Week 4 - Airflow Integration (8 hours)
```
MON: Review DAG architecture
WED: ML integration review
FRI: Pipeline validation
```

### Week 5 - Analytics Review (8 hours)
```
MON: Dashboard review
WED: Grafana alert setup
FRI: Monitoring validation
```

### Week 6 - Documentation & Release (16 hours)
```
MON: Documentation review
WED: Final testing
FRI: Release coordination
```

**Critical Responsibilities**:
- ✓ Unblock team immediately
- ✓ Lead architecture decisions
- ✓ Review all major PRs
- ✓ Run twice-weekly syncs
- ✓ Escalate issues

**Contacts**: Entire team

**First Task**: Create GitHub repo with branch protection

---

## 📨 FARZANEH BARZEGAR - Weekly Checklist

**Role**: Data Ingestion Engineer  
**Total Project Hours**: 56 hours (~8-12 hours/week)

### Week 1 - Data Preparation (8 hours)
```
TUE-WED: Dataset validation
  □ Check fraud rate (1-5%)
  □ Validate 21 features
  □ No corrupted records
  □ Document findings

FRI: Prep for producer work
```

### Week 2 - Kafka Producer Implementation (24 hours)
```
MON: Producer basic structure
  □ Load CSV
  □ Connect to Kafka
  □ Serialize to JSON
  
TUE-WED: Feature implementation
  □ CSV replay logic
  □ Configurable TPS
  □ Error handling
  
THU: Testing & refinement
  □ Unit tests written
  □ Manual testing
  
FRI: PR submission & review

✅ PRODUCER DONE WHEN:
□ Sends 10-100 TPS to Kafka
□ Messages are valid JSON
□ Error handling works
□ Logging every minute
□ Unit tests pass
□ README complete
```

### Week 3-6 - Support & Enhancement (24 hours)
```
MON: Enhancements based on feedback
WED: Optional fraud injection feature
FRI: Documentation updates

🎯 OPTIONAL ENHANCEMENTS:
- Fraud pattern injection
- Performance optimization
- Advanced error handling
```

**Key Deliverables**:
1. transaction_generator.py
2. producer/Dockerfile
3. producer/requirements.txt
4. producer/README.md
5. tests/test_producer.py

**Kafka Topic You Own**: `raw-transactions`

**Test Command**: `make kafka-consume`

**Contacts**: Hontar (schema), Khurshid (support)

**First Task**: Validate dataset (Week 1)

---

## 🔄 HONTAR DANIIL - Weekly Checklist

**Role**: Data Processing & ML Engineer  
**Total Project Hours**: 152 hours (~25-30 hours/week)

### Week 2 - Spark Streaming Part 1 (32 hours)
```
MON: Bronze layer
  □ Schema enforcement
  □ Append-only writes
  □ Checkpoint handling
  □ Error handling
  
TUE-WED: Silver layer - Features
  □ 10 features computed
  □ Rule scoring logic
  □ No null values
  
THU: Testing
  □ Unit tests
  □ Data validation
  
FRI: PR submission & review

✅ LAYERS COMPLETE WHEN:
□ Bronze ingests 100% of Kafka
□ Silver computes all features correctly
□ Rule score 0-1 range
□ Delta Lake tables readable
□ Tests passing (80%+)
```

### Week 2-3 - Spark Streaming Part 2 (24 hours)
```
MON: ML score integration
  □ Load fraud_model.pkl
  □ Graceful fallback
  □ Latency < 100ms
  
TUE-WED: Gold layer
  □ Fraud flagging logic
  □ 3 outputs: Delta, Kafka, PostgreSQL
  □ Metrics aggregation
  
THU: End-to-end testing
FRI: PR submission
```

### Week 3 - ML Training (16 hours)
```
MON-TUE: Train model
  □ XGBoost implementation
  □ Class imbalance handling
  □ Save to models/fraud_model.pkl
  
WED: MLflow integration
  □ Log metrics
  □ Log artifacts
  □ Experiment tracking
  
THU: Evaluation
  □ Model comparison
  □ Promotion logic
  
FRI: PR submission

✅ ML DONE WHEN:
□ ROC-AUC ≥ 0.85
□ PR-AUC ≥ 0.70
□ Training time < 30 min
□ MLflow tracking all runs
□ Model versioning working
```

### Week 4 - Airflow Support (16 hours)
```
MON-WED: Assist Elif with DAGs
  □ Daily retraining integration
  □ Model loading in Airflow
  □ Error handling
  
THU-FRI: Testing & refinement
```

### Week 5 - Testing (24 hours)
```
MON-TUE: Unit tests
  □ Feature engineering tests
  □ ML scoring tests
  □ 80%+ coverage
  
WED-THU: Integration tests
  □ End-to-end pipeline
  □ All outputs sync
  
FRI: Load testing
  □ 100 TPS sustained
  □ 500 TPS spike handling
```

### Week 6 - Performance & Polish (16 hours)
```
MON: Performance tuning
WED: Load test at 1000 TPS
FRI: Documentation complete
```

**Key Deliverables**:
1. spark_jobs/fraud_streaming_job.py (Bronze, Silver, Gold)
2. ml/train_model.py
3. ml/evaluate_model.py
4. tests/test_spark_job.py (unit tests)
5. spark_jobs/README.md

**Data Layers You Own**:
- Bronze: `/data/delta/bronze` (raw)
- Silver: `/data/delta/silver` (features)
- Gold: `/data/delta/gold` (flagged)

**Model You Own**: `models/fraud_model.pkl`

**Test Commands**:
```
make test                    # Unit tests
make test-integration        # End-to-end
make test-load              # Performance
```

**Kafka Topics**:
- Consume: `raw-transactions`
- Produce: `flagged-transactions`

**Contacts**: Farzaneh (schema), Elif (PostgreSQL), Khurshid (ML strategy)

**First Task**: Wait for Docker → Build Bronze layer (Week 2)

---

## 📊 ELIF SILA OKUTUCU - Weekly Checklist

**Role**: Analytics & DevOps Engineer
**Total Project Hours**: 144 hours (~24-28 hours/week)

### Week 1 - Docker & Database (24 hours)
```
MON-TUE: Docker setup
  □ Dockerfile for Spark
  □ docker-compose refinement
  □ Service health checks
  
WED-THU: PostgreSQL
  □ init_postgres.sql
  □ fraud_metrics table
  □ dq_checks table
  □ model_registry table
  
FRI: Testing & validation

✅ INFRASTRUCTURE DONE WHEN:
□ All 13 services start cleanly
□ PostgreSQL connections work
□ Spark can write to PG
□ All health checks pass
```

### Week 4 - Airflow Setup (56 hours)
```
MON: Airflow configuration
  □ Environment setup
  □ Connections configured
  □ DAG discovery working
  
TUE-WED: Daily Retraining DAG
  □ fraud_detection_daily.py
  □ 5 tasks (validate, retrain, evaluate, promote, notify)
  □ SLAs configured
  □ Error handling
  
THU: DQ Monitoring DAG
  □ data_quality_monitoring_dag.py
  □ 6 checks implemented
  □ PostgreSQL writes
  □ Alert thresholds
  
FRI: Testing & deployment

✅ AIRFLOW DONE WHEN:
□ Daily DAG runs at 00:00 UTC
□ DQ DAG runs hourly
□ All tasks complete within SLA
□ Model pickle updates on schedule
□ Notifications working
□ DAG UI shows full history
```

### Week 5 - Grafana Dashboards (48 hours)
```
MON-TUE: Dashboard setup
  □ PostgreSQL datasource
  □ Dashboard provisioning
  
WED-THU: Main dashboard
  □ KPI cards
  □ Time series charts
  □ DQ panels
  □ Model panels
  
FRI: Alerts & refinement

✅ GRAFANA DONE WHEN:
□ Dashboard live at localhost:3000
□ All panels show real data
□ Alerts trigger correctly
□ Auto-refresh every 30s
□ Performance queries optimized
```

### Week 6 - Monitoring & Polish (16 hours)
```
MON: Alert rule testing
WED: Performance optimization
FRI: Documentation complete
```

**Key Deliverables**:
1. Dockerfile (custom Spark image)
2. docker-compose.yml (refined)
3. scripts/init_postgres.sql
4. dags/fraud_detection_daily_dag.py
5. dags/data_quality_monitoring_dag.py
6. grafana/dashboards/fraud_alerts_dashboard.json
7. grafana/provisioning/ (datasource config)
8. grafana/README.md
9. documentation/evidence/dashboard/fraud_alerts_dashboard.png
10. documentation/evidence/postgres/postgres_validation.png
11. documentation/evidence/airflow/airflow_dag_execution_success.png
12. documentation/evidence/airflow/airflow_dag_registration.png

**Services You Manage**:
- Docker Compose (all 13 services)
- PostgreSQL
- Airflow (2 DAGs)
- Grafana

**PostgreSQL Tables You Create**:
- fraud_metrics (Kafka outputs → here)
- dq_checks (DQ results)
- model_registry (ML tracking)

**Airflow DAGs You Own**:
- `fraud_detection_daily` (daily @ 00:00 UTC)
- `data_quality_monitoring` (hourly)

**Grafana Dashboards You Own**:
- fraud_alerts_dashboard.json

**Test Commands**:
```
# Test DAGs
airflow dags test fraud_detection_daily 2026-05-15
airflow tasks test fraud_detection_daily validate_silver_data 2026-05-15

# Test PostgreSQL
psql -h localhost -U postgres -d fraud -c "SELECT COUNT(*) FROM fraud_metrics"

# Test Grafana
curl http://localhost:3000
```

**Contacts**: Hontar (DAG logic), Farzaneh (project coordination), All (Grafana feedback)

### Final Validation Activities

Completed during project closure:

- Airflow DAG registration and execution validation
- PostgreSQL fraud_metrics validation
- Grafana dashboard implementation and export
- Monitoring documentation updates
- Repository evidence collection and verification
- Final README and documentation review

---

## 📞 Weekly Sync Meetings

**EVERY MONDAY 10:00 UTC** (30 min)
- Sprint planning
- Blockers & dependencies
- Week goals

**EVERY THURSDAY 15:00 UTC** (30 min)
- Progress update
- Course correction
- Next steps

**Agenda Format**:
1. What you completed (2 min each)
2. Blockers (5 min)
3. Next week goals (5 min)

---

## 🚀 Success Signals

**End of Week 1**: Infrastructure ready ✅
- [x] GitHub repo + all settings
- [x] Docker services running
- [x] PostgreSQL initialized
- [x] Dataset loaded

**End of Week 2-3**: Data flowing ✅
- [x] Kafka producer → Bronze
- [x] Bronze → Silver → Gold
- [x] All 3 outputs working

**End of Week 4**: Orchestrated ✅
- [x] Daily DAG running
- [x] Model retraining scheduled
- [x] DQ checks hourly

**End of Week 5**: Monitored ✅
- [x] Grafana dashboards live
- [x] Unit tests passing
- [x] System healthy

**End of Week 6**: Production Ready ✅
- [x] All documentation
- [x] Performance baseline
- [x] Ready to present

---

## ⚠️ Blockers? Here's How to Get Help

**Quick Help**: Post in Slack #fraud-detection-project
**Documentation**: Check TEAM_TASK_BREAKDOWN.md
**Code Help**: Create GitHub Issue
**Urgent**: Contact Farzaneh (Project Lead)
**Meeting**: Request emergency sync

---

## 📋 Print This Page & Keep It Visible!

This is your quick reference for:
- What to do each week
- When things are done
- Who to contact
- What to test

**Pro Tip**: Bookmark TEAM_TASK_BREAKDOWN.md for detailed specs

---

**Project Start**: Week of May 20, 2026  
**Project End**: Week of June 30, 2026  

Project successfully completed, validated, and documented. 🚀
