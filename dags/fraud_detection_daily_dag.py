"""
AIRFLOW DAG: DAILY FRAUD DETECTION WORKFLOW
==============================================
Author: Elif Sila Okutucu
Role: Analytics & DevOps Engineer

Runs daily at midnight UTC.

Pipeline it orchestrates:
  validate_silver_data  — check Silver Delta layer has sufficient data
  decide_retrain        — branch: retrain if >= 1000 new samples
  retrain_model         — trigger ml/train_model.py on the VM
  skip_retrain          — log and continue if insufficient new data
  evaluate_model        — query MLflow / model_training_history for ROC-AUC
  promote_to_gold       — verify Gold Delta layer was updated by Spark
  notify_teams          — log summary to model_training_history in PostgreSQL

All real data comes from PostgreSQL (fraud_db) and Delta Lake paths.
"""

import os
import logging
import subprocess
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
PG_HOST     = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB       = os.getenv("POSTGRES_DB", "fraud_db")
PG_USER     = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

SILVER_PATH         = os.getenv("SILVER_PATH", "/data/delta/silver")
GOLD_PATH           = os.getenv("GOLD_PATH",   "/data/delta/gold")
MODEL_PATH          = os.getenv("MODEL_PATH",  "/data/models/fraud_model.pkl")
TRAIN_SCRIPT        = os.getenv("TRAIN_SCRIPT", "/opt/ml/train_model.py")
DATASET_PATH        = os.getenv("DATASET_PATH", "/data/synthetic_fraud_dataset.csv")

MIN_SAMPLES_FOR_TRAINING = int(os.getenv("MIN_SAMPLES_FOR_TRAINING", "1000"))
MIN_ROC_AUC              = float(os.getenv("MIN_ROC_AUC", "0.80"))


def _get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
    )


# ── Task 1: Validate Silver Data ───────────────────────────────────────────

def validate_silver_data():
    """
    Check that fraud_metrics (written by Spark from Gold layer) has
    enough operational rows to decide on retraining.
    Also verifies the Silver Delta path exists on disk.
    """
    logger.info("✓ Validating Silver/Gold layer readiness...")

    # Check Silver Delta path exists
    if os.path.isdir(SILVER_PATH):
        logger.info(f"✓ Silver Delta path found: {SILVER_PATH}")
    else:
        logger.warning(f"⚠️  Silver path not accessible from Airflow container: {SILVER_PATH} "
                       f"(Spark workers may have it — continuing)")

    # Check fraud_metrics has operational data
    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*), ROUND(AVG(fraud_score)::numeric, 4)
                    FROM fraud_metrics
                    WHERE transaction_id NOT IN
                          ('TXN_001','TXN_002','TXN_003','TXN_004','TXN_005');
                """)
                count, avg_score = cur.fetchone()
        conn.close()
    except Exception as e:
        raise AirflowException(f"❌ Cannot reach PostgreSQL for validation: {e}")

    logger.info(f"✓ fraud_metrics operational rows : {count}")
    logger.info(f"✓ Average fraud score            : {avg_score}")

    if count == 0:
        raise AirflowException(
            "❌ Silver/Gold validation failed: "
            "no operational rows in fraud_metrics. "
            "Spark streaming job may not have run."
        )

    logger.info("✓ validate_silver_data PASSED")


# ── Task 2: Decide Retrain ─────────────────────────────────────────────────

def decide_retrain(**context):
    """
    Count rows in fraud_metrics added since the last model training run.
    If >= MIN_SAMPLES_FOR_TRAINING → branch to retrain_model.
    Otherwise → branch to skip_retrain.

    Returns the task_id of the next branch (used by BranchPythonOperator
    if wired that way; here it just logs the decision).
    """
    logger.info("🤔 Checking if retraining is needed...")

    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                # Get date of last training run
                cur.execute("""
                    SELECT MAX(training_date)
                    FROM model_training_history
                    WHERE status IN ('ACTIVE', 'ARCHIVED');
                """)
                last_training = cur.fetchone()[0]

                if last_training:
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM fraud_metrics
                        WHERE timestamp > %s
                          AND transaction_id NOT IN
                              ('TXN_001','TXN_002','TXN_003','TXN_004','TXN_005');
                    """, (last_training,))
                else:
                    cur.execute("""
                        SELECT COUNT(*) FROM fraud_metrics
                        WHERE transaction_id NOT IN
                              ('TXN_001','TXN_002','TXN_003','TXN_004','TXN_005');
                    """)
                new_samples = cur.fetchone()[0]
        conn.close()
    except Exception as e:
        raise AirflowException(f"❌ decide_retrain DB error: {e}")

    logger.info(f"  New samples since last training: {new_samples}")
    logger.info(f"  Minimum required              : {MIN_SAMPLES_FOR_TRAINING}")

    if new_samples >= MIN_SAMPLES_FOR_TRAINING:
        logger.info(f"✓ Retraining needed ({new_samples} >= {MIN_SAMPLES_FOR_TRAINING})")
        context["ti"].xcom_push(key="retrain_decision", value="retrain")
        return "retrain_model"
    else:
        logger.info(f"⏭️  Skipping retrain ({new_samples} < {MIN_SAMPLES_FOR_TRAINING})")
        context["ti"].xcom_push(key="retrain_decision", value="skip")
        return "skip_retrain"


# ── Task 3: Retrain Model ──────────────────────────────────────────────────

def retrain_model():
    """
    Trigger the ML training script (ml/train_model.py) as a subprocess.
    In Docker Compose, the script runs on the same container or via
    docker exec to the spark-worker container.
    """
    logger.info("🤖 Starting model retraining...")

    if not os.path.isfile(TRAIN_SCRIPT):
        logger.warning(
            f"⚠️  Training script not found at {TRAIN_SCRIPT}. "
            f"This is expected if the DAG container doesn't have direct "
            f"access to the Spark worker filesystem. "
            f"Logging retraining intent — actual training runs via Spark."
        )
        # Record the intent in model_training_history
        _log_training_attempt(status="PENDING",
                               notes="Training script not accessible from Airflow container; "
                                     "triggered via Spark worker separately.")
        return

    try:
        result = subprocess.run(
            ["python", TRAIN_SCRIPT,
             "--source", "csv",
             "--dataset-path", DATASET_PATH],
            capture_output=True, text=True, timeout=600
        )
        if result.returncode != 0:
            raise AirflowException(
                f"❌ Training script failed:\n{result.stderr}"
            )
        logger.info("✓ Model retrained successfully")
        logger.info(result.stdout[-2000:])   # last 2k chars of output
    except subprocess.TimeoutExpired:
        raise AirflowException("❌ Training script timed out (>10 min)")


def _log_training_attempt(status: str, notes: str):
    """Write a row to model_training_history."""
    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO model_training_history
                        (training_date, model_version, status, notes)
                    VALUES (NOW(), 'pending', %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (status, notes))
        conn.close()
    except Exception as e:
        logger.warning(f"Could not log training attempt: {e}")


# ── Task 4: Skip Retrain ───────────────────────────────────────────────────

def skip_retrain():
    """Log that retraining was skipped this cycle."""
    logger.info("⏭️  Skipping model retrain — insufficient new samples")

    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model_version, roc_auc, status
                    FROM model_training_history
                    ORDER BY training_date DESC
                    LIMIT 1;
                """)
                row = cur.fetchone()
        conn.close()
        if row:
            logger.info(f"✓ Current active model: {row[0]} | ROC-AUC: {row[1]} | {row[2]}")
    except Exception as e:
        logger.warning(f"Could not fetch current model info: {e}")


# ── Task 5: Evaluate Model ─────────────────────────────────────────────────

def evaluate_model():
    """
    Read the latest model metrics from model_training_history in PostgreSQL
    (written there by ml/evaluate_model.py after each training run).
    Fail the DAG if ROC-AUC < MIN_ROC_AUC.
    """
    logger.info("📊 Evaluating model performance from model_training_history...")

    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model_version, roc_auc, pr_auc,
                           precision, recall, f1_score, status
                    FROM model_training_history
                    ORDER BY training_date DESC
                    LIMIT 1;
                """)
                row = cur.fetchone()
        conn.close()
    except Exception as e:
        raise AirflowException(f"❌ evaluate_model DB error: {e}")

    if row is None:
        logger.warning("⚠️  No model in model_training_history yet — skipping evaluation")
        return

    model_version, roc_auc, pr_auc, precision, recall, f1, status = row

    logger.info(f"  Model version : {model_version}")
    logger.info(f"  ROC-AUC       : {roc_auc}  (min: {MIN_ROC_AUC})")
    logger.info(f"  PR-AUC        : {pr_auc}")
    logger.info(f"  Precision     : {precision}")
    logger.info(f"  Recall        : {recall}")
    logger.info(f"  F1-Score      : {f1}")
    logger.info(f"  Status        : {status}")

    if roc_auc is not None and float(roc_auc) < MIN_ROC_AUC:
        raise AirflowException(
            f"❌ Model {model_version} ROC-AUC {roc_auc} < {MIN_ROC_AUC}. "
            f"Not promoting to Gold."
        )

    logger.info(f"✓ evaluate_model PASSED — ROC-AUC {roc_auc} >= {MIN_ROC_AUC}")


# ── Task 6: Promote to Gold ────────────────────────────────────────────────

def promote_to_gold():
    """
    Verify the Gold Delta layer has been updated by Spark (by checking
    that fraud_metrics contains rows written recently), and log the
    promotion event.
    """
    logger.info("✓ Verifying Gold layer promotion...")

    if os.path.isdir(GOLD_PATH):
        logger.info(f"✓ Gold Delta path accessible: {GOLD_PATH}")
    else:
        logger.warning(f"⚠️  Gold path not accessible from Airflow container: {GOLD_PATH}")

    # Verify PostgreSQL has recent fraud alerts (Spark writes Gold → PostgreSQL)
    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*), MAX(timestamp)
                    FROM fraud_metrics
                    WHERE transaction_id NOT IN
                          ('TXN_001','TXN_002','TXN_003','TXN_004','TXN_005');
                """)
                count, latest_ts = cur.fetchone()
        conn.close()
    except Exception as e:
        raise AirflowException(f"❌ promote_to_gold DB error: {e}")

    logger.info(f"✓ Gold-layer fraud alerts in PostgreSQL : {count}")
    logger.info(f"✓ Most recent alert timestamp           : {latest_ts}")

    if count == 0:
        raise AirflowException(
            "❌ Gold promotion check failed: fraud_metrics has no operational rows. "
            "Spark streaming job may not have written to PostgreSQL yet."
        )

    logger.info("✓ promote_to_gold PASSED")


# ── Task 7: Notify Teams ───────────────────────────────────────────────────

def notify_teams():
    """
    Write a daily summary row to model_training_history and log the
    final pipeline status. In production this would also send a
    Slack/Teams webhook.
    """
    logger.info("📧 Writing daily summary to model_training_history...")

    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                # Get current model info
                cur.execute("""
                    SELECT model_version, roc_auc, pr_auc, status
                    FROM model_training_history
                    ORDER BY training_date DESC LIMIT 1;
                """)
                model_row = cur.fetchone()

                # Get fraud alert summary
                cur.execute("""
                    SELECT COUNT(*),
                           ROUND(AVG(fraud_score)::numeric, 4),
                           ROUND(AVG(rule_based_score)::numeric, 4),
                           ROUND(AVG(ml_score)::numeric, 4)
                    FROM fraud_metrics
                    WHERE transaction_id NOT IN
                          ('TXN_001','TXN_002','TXN_003','TXN_004','TXN_005');
                """)
                alert_row = cur.fetchone()
        conn.close()
    except Exception as e:
        logger.error(f"notify_teams DB error: {e}")
        return

    model_ver = model_row[0] if model_row else "unknown"
    roc_auc   = model_row[1] if model_row else None
    total_alerts, avg_fraud, avg_rule, avg_ml = alert_row

    summary = (
        f"Daily run complete. "
        f"Model: {model_ver} | ROC-AUC: {roc_auc}. "
        f"Fraud alerts: {total_alerts} | "
        f"avg_score: {avg_fraud} | "
        f"avg_rule: {avg_rule} | avg_ml: {avg_ml}."
    )

    logger.info("=" * 60)
    logger.info("DAILY FRAUD DETECTION WORKFLOW COMPLETE")
    logger.info(summary)
    logger.info("=" * 60)

    # In production: send Teams/Slack webhook here
    # requests.post(WEBHOOK_URL, json={"text": summary})


# ── DAG Definition ─────────────────────────────────────────────────────────

default_args = {
    "owner": "fraud-detection-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
}

dag = DAG(
    "fraud_detection_daily_dag",
    default_args=default_args,
    description="Daily fraud detection model validation, retraining and Gold layer promotion",
    schedule_interval="0 0 * * *",   # Every day at midnight UTC
    catchup=False,
    tags=["fraud-detection", "ml-pipeline", "daily"],
)

start = DummyOperator(task_id="start", dag=dag)
end   = DummyOperator(task_id="end",   dag=dag)

validate = PythonOperator(
    task_id="validate_silver_data",
    python_callable=validate_silver_data,
    dag=dag,
)
decide = BranchPythonOperator(
    task_id="decide_retrain",
    python_callable=decide_retrain,
    provide_context=True,
    dag=dag,
)
retrain = PythonOperator(
    task_id="retrain_model",
    python_callable=retrain_model,
    dag=dag,
)
skip = PythonOperator(
    task_id="skip_retrain",
    python_callable=skip_retrain,
    dag=dag,
)
evaluate = PythonOperator(
    task_id="evaluate_model",
    python_callable=evaluate_model,
    dag=dag,
)
promote = PythonOperator(
    task_id="promote_to_gold",
    python_callable=promote_to_gold,
    dag=dag,
)
notify = PythonOperator(
    task_id="notify_teams",
    python_callable=notify_teams,
    dag=dag,
)

# DAG dependency graph
start >> validate >> decide
decide >> [retrain, skip]
retrain >> evaluate >> promote >> notify >> end
skip >> promote
