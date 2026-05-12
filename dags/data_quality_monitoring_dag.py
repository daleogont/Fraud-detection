"""
AIRFLOW DAG: HOURLY DATA QUALITY MONITORING
=============================================
Runs every hour to validate data quality.

For ML students: Data quality is critical for ML:
- Bad data → Bad model
- This DAG checks:
  - Row counts are present
  - Schema is correct
  - Null rates are acceptable
  - Fraud rate is in expected range
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'fraud-detection-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
}

dag = DAG(
    'data_quality_monitoring_dag',
    default_args=default_args,
    description='Hourly data quality checks',
    schedule_interval='0 * * * *',  # Every hour at minute 0
    catchup=False
)

# Task functions

def row_count_check():
    """Check that all Delta layers have data."""
    logger.info("📊 Checking row counts in Delta layers...")
    
    # In production:
    # - Query Bronze, Silver, Gold layers
    # - Ensure each has > 0 rows
    
    checks = {
        'bronze': 1000,  # Placeholder
        'silver': 800,
        'gold': 50
    }
    
    for layer, count in checks.items():
        if count == 0:
            raise AirflowException(f"❌ {layer.upper()} layer is empty!")
        logger.info(f"✓ {layer}: {count} rows")


def schema_check():
    """Verify Silver layer has all required columns."""
    logger.info("🔍 Checking schema...")
    
    required_columns = [
        'Transaction_ID', 'Timestamp', 'Transaction_Amount', 'Transaction_Type',
        'Merchant_Category', 'Risk_Score', 'Fraud_Label',
        'amount', 'log_amount', 'merchant_risk_score',
        'flag_high_amount', 'flag_velocity', 'flag_off_hours',
        'flag_geo_anomaly', 'flag_risky_merchant', 'is_online',
        'event_hour', 'fraud_score', 'is_flagged', 'label'
    ]
    
    # In production: query Delta Lake table schema
    present_columns = required_columns  # Placeholder
    
    missing = set(required_columns) - set(present_columns)
    if missing:
        raise AirflowException(f"❌ Missing columns: {missing}")
    
    logger.info(f"✓ All {len(required_columns)} required columns present")


def null_rate_check():
    """Check null rates in critical columns."""
    logger.info("❓ Checking null rates...")
    
    critical_columns = ['amount', 'merchant_risk_score', 'fraud_score']
    max_null_rate = 0.05  # 5%
    
    # In production: compute actual null rates
    null_rates = {
        'amount': 0.001,
        'merchant_risk_score': 0.002,
        'fraud_score': 0.0
    }
    
    for col, rate in null_rates.items():
        if rate > max_null_rate:
            raise AirflowException(f"❌ {col} has {rate*100:.2f}% nulls (> {max_null_rate*100}%)")
        logger.info(f"✓ {col}: {rate*100:.2f}% nulls")


def fraud_rate_check():
    """Check fraud rate is within expected range."""
    logger.info("🎯 Checking fraud rate...")
    
    min_fraud_rate = 0.001  # 0.1%
    max_fraud_rate = 0.10   # 10%
    
    # In production: compute actual fraud rate
    fraud_rate = 0.015  # 1.5% - Placeholder
    
    if fraud_rate < min_fraud_rate or fraud_rate > max_fraud_rate:
        logger.warning(f"⚠️  Fraud rate {fraud_rate*100:.2f}% outside [{min_fraud_rate*100}%, {max_fraud_rate*100}%]")
    else:
        logger.info(f"✓ Fraud rate: {fraud_rate*100:.2f}% (within acceptable range)")


def log_results(**context):
    """Log all check results."""
    logger.info("=" * 60)
    logger.info("DATA QUALITY CHECK PASSED ✓")
    logger.info("=" * 60)


# Define tasks
start = DummyOperator(task_id='start', dag=dag)

check_rows = PythonOperator(
    task_id='row_count_check',
    python_callable=row_count_check,
    dag=dag
)

check_schema = PythonOperator(
    task_id='schema_check',
    python_callable=schema_check,
    dag=dag
)

check_nulls = PythonOperator(
    task_id='null_rate_check',
    python_callable=null_rate_check,
    dag=dag
)

check_fraud_rate = PythonOperator(
    task_id='fraud_rate_check',
    python_callable=fraud_rate_check,
    dag=dag
)

log_results_task = PythonOperator(
    task_id='log_results',
    python_callable=log_results,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# Build DAG (parallel checks)
start >> [check_rows, check_schema, check_nulls, check_fraud_rate] >> log_results_task >> end
