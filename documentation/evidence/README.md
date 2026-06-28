# Evidence & Validation

This directory contains validation evidence collected during the final phase of the Real-Time Financial Fraud Detection System project.

## Purpose

The evidence folder provides proof that the main system components were implemented, executed, and validated through screenshots, terminal outputs, dashboard captures, and short demo recordings.

The evidence is intended to show the system from an implementation and validation perspective, including service health, orchestration, database outputs, monitoring views, and pipeline-level checks.

## Validation Areas

### Airflow Orchestration

Evidence in the airflow/ folder demonstrates:

- Airflow services were running together with PostgreSQL.
- Project DAGs were registered in Airflow.
- No DAG import errors were present during validation.
- The `data_quality_monitoring_dag` was executed successfully.
- Data quality validation results were written into the PostgreSQL dq_checks table.
- The `fraud_detection_daily_dag` was registered as part of the orchestration layer, but the final validated evidence focuses on the completed Data Quality workflow.

### PostgreSQL Validation

Evidence showing:

- Fraud metrics written successfully
- Fraud alerts stored in PostgreSQL
- Data quality monitoring results
- Aggregated fraud statistics

### Grafana Monitoring Dashboard

Evidence in the postgres/ folder demonstrates:

- Fraud alert monitoring dashboard
- Fraud score distribution
- Alert severity distribution
- Data quality monitoring table
- Model performance metrics
- Alert entity analysis

### Grafana Monitoring Dashboard

Evidence in the dashboard/ folder demonstrates:

- Grafana as the monitoring layer for fraud alert visualization with Fraud-related dashboard panels.
- PostgreSQL as the dashboard data source.
- Dashboard screenshot as visual evidence.


### End-to-End Pipeline Validation

Evidence showing:

- Transaction data ingestion and processing
- Fraud scoring and alert generation
- PostgreSQL storage of fraud metrics and data quality results
- Airflow-based orchestration and validation
- Grafana-based monitoring and dashboard visualization

## Evidence Structure

```text
evidence/
├── airflow/
│    │   ├── 01_airflow_services_and_dags.png
│    │   └── 02_data_quality_dag_success.png
│    │   └── 03_postgres_dq_checks_written.png
│    │   └── airflow_dag_execution_success.png
├── dashboard/
│   └── fraud_alerts_dashboard.png
├── postgres/
│   └── postgres_validation.png
└── README.md
```

## Final Validation Status

| Component | Status |
|------------|---------|
| Kafka Streaming | ✅ Validated |
| Spark Processing | ✅ Validated |
| PostgreSQL Storage | ✅ Validated |
| ML Model Scoring | ✅ Validated |
| MLflow Tracking | ✅ Validated |
| Airflow Orchestration | ✅ Validated |
| Data Quality Monitoring | ✅ Validated |
| Grafana Dashboard | ✅ Validated |

**Overall Project Status:** ✅ Completed with documented validation evidence for the main system components.