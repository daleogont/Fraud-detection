"""
AIRFLOW DAG: DAILY FRAUD DETECTION WORKFLOW
==============================================
Runs daily at midnight UTC to:
1. Validate Silver layer data quality
2. Decide if retraining is needed (>1000 new samples?)
3. Retrain the model if needed
4. Evaluate model (need ROC-AUC >= 0.80)
5. Promote flagged transactions to Gold layer
6. Send alerts

For ML students: This DAG shows the full ML workflow:
- Data validation → Model training → Model evaluation → Deployment
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

# Default arguments for all tasks
default_args = {
    'owner': 'fraud-detection-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
}

# Define DAG
dag = DAG(
    'fraud_detection_daily_dag',
    default_args=default_args,
    description='Daily fraud detection model training and evaluation',
    schedule_interval='0 0 * * *',  # Every day at midnight UTC
    catchup=False
)

# Task functions (these would normally use DeltaLake, MLflow, etc.)

def validate_silver_data():
    """Validate data quality in Silver layer."""
    logger.info("✓ Validating Silver layer...")
    # In production, check:
    # - Row counts > 0
    # - No unexpected schema changes
    # - Null rates < 5%
    # - Fraud rate in expected range
    logger.info("✓ Silver layer validation passed")


def decide_retrain(**context):
    """Decide if model retraining is needed."""
    logger.info("🤔 Checking if retraining is needed...")
    # Count new samples since last training
    new_samples = 5000  # In production, query from Delta Lake
    min_samples = int(context['dag'].params.get('min_samples_for_training', 1000))
    
    if new_samples >= min_samples:
        logger.info(f"✓ Found {new_samples} new samples (>= {min_samples}). Retraining needed.")
        return 'retrain_model'
    else:
        logger.info(f"⏭️  Only {new_samples} new samples (< {min_samples}). Skipping retrain.")
        return 'skip_retrain'


def retrain_model():
    """Retrain the XGBoost fraud detection model."""
    logger.info("🤖 Starting model retraining...")
    # In production: 
    # 1. Load labeled data from Silver Delta Lake
    # 2. Train XGBoost
    # 3. Log to MLflow
    logger.info("✓ Model retraining complete")


def skip_retrain():
    """Skip retraining this cycle."""
    logger.info("⏭️  Skipping retrain - insufficient new data")


def evaluate_model():
    """Evaluate newly trained model."""
    logger.info("📊 Evaluating model...")
    
    # In production: load test set, compute metrics
    roc_auc = 0.82  # Placeholder - would come from evaluation
    min_roc_auc = 0.80
    
    if roc_auc >= min_roc_auc:
        logger.info(f"✓ Model meets quality threshold (ROC-AUC: {roc_auc:.4f} >= {min_roc_auc})")
    else:
        raise AirflowException(
            f"❌ Model failed quality check (ROC-AUC: {roc_auc:.4f} < {min_roc_auc})"
        )


def promote_to_gold():
    """Promote flagged transactions to Gold layer."""
    logger.info("✓ Promoting flagged transactions to Gold layer...")
    # In production: copy from Silver where is_flagged=True to Gold
    logger.info("✓ Promotion complete")


def notify_teams():
    """Send notification to team."""
    logger.info("📧 Sending notification...")
    # In production: send Teams/Slack message with metrics
    logger.info("✓ Notification sent")


# Define task dependencies
start = DummyOperator(task_id='start', dag=dag)

validate = PythonOperator(
    task_id='validate_silver_data',
    python_callable=validate_silver_data,
    dag=dag
)

decide = PythonOperator(
    task_id='decide_retrain',
    python_callable=decide_retrain,
    dag=dag
)

retrain = PythonOperator(
    task_id='retrain_model',
    python_callable=retrain_model,
    dag=dag
)

skip = PythonOperator(
    task_id='skip_retrain',
    python_callable=skip_retrain,
    dag=dag
)

evaluate = PythonOperator(
    task_id='evaluate_model',
    python_callable=evaluate_model,
    dag=dag
)

promote = PythonOperator(
    task_id='promote_to_gold',
    python_callable=promote_to_gold,
    dag=dag
)

notify = PythonOperator(
    task_id='notify_teams',
    python_callable=notify_teams,
    dag=dag
)

end = DummyOperator(task_id='end', dag=dag)

# Build DAG
start >> validate >> decide
decide >> [retrain, skip]
retrain >> evaluate >> promote >> notify >> end
skip >> promote
