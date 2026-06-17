# Grafana Monitoring

This directory contains Grafana assets for the Real-Time Financial Fraud Detection System.

## Dashboards

### Fraud Alerts Monitoring Dashboard

Dashboard export file:

```text
grafana/dashboards/fraud_alerts_dashboard.json
```

This is the final Grafana dashboard created for monitoring fraud alerts produced by the real-time fraud detection pipeline.

Pipeline monitored by this dashboard:

```text
Kafka → Spark Structured Streaming → Delta Lake Gold Layer → PostgreSQL → Grafana
```

## Purpose

The dashboard is designed to monitor flagged fraud alerts, not the full transaction population.

The PostgreSQL `fraud_metrics` table receives records from the Gold layer, which contains flagged transactions only. Therefore, dashboard metrics are focused on fraud alert monitoring and investigation.

## Data Sources

The dashboard uses the PostgreSQL Grafana data source and reads from the following tables:

| Table | Purpose |
|---------|---------|
| fraud_metrics | Fraud alert metrics and scored transactions |
| dq_checks | Data quality monitoring results |
| model_training_history | ML model version and performance metrics |

## Main Dashboard Sections

The dashboard includes:

- Fraud alert KPIs
- Fraud score and severity distributions
- Highest alert amounts
- Top alerted entities
- Average ML vs rule-based detection contribution
- Model performance metrics
- Data quality checks
- Fraud alert detail table

## Validation Summary

Final validation confirmed that PostgreSQL contained:

| Metric | Value |
|---------|---------:|
| Total rows in fraud_metrics | 119 |
| Sample rows | 5 |
| Real fraud alert rows | 114 |
| Real flagged rows | 114 |
| Average real fraud score | 0.7404 |

The first 5 rows are sample initialization records. The remaining 114 rows were produced by the working Kafka → Spark Streaming → Delta Lake → PostgreSQL flow.

## Importing the Dashboard

If Grafana is reset or the local Grafana database is recreated, the dashboard can be restored manually:

1. Open Grafana.
2. Navigate to **Dashboards**.
3. Select **Import**.
4. Upload:

```text
grafana/dashboards/fraud_alerts_dashboard.json
```

5. Select the PostgreSQL data source.
6. Import the dashboard.

## Evidence

Dashboard and validation screenshots are stored under:

```text
documentation/evidence/
```

Relevant evidence includes:

```text
documentation/evidence/dashboard/
documentation/evidence/postgres/
documentation/evidence/airflow/
```

## Contribution

This dashboard was created and validated as part of the monitoring and observability workstream of the project.

Completed activities include:

- Grafana dashboard design and implementation
- PostgreSQL fraud alert validation
- Dashboard export and versioning
- Airflow DAG execution verification
- Monitoring documentation and evidence collection
- Repository documentation updates