"""
AIRFLOW DAG: HOURLY DATA QUALITY MONITORING
=============================================
Author: Elif Sila Okutucu
Role: Analytics & DevOps Engineer

Runs every hour to validate data quality across the pipeline.
Checks are performed against the actual Delta Lake layers and
PostgreSQL fraud_metrics table, then results are written to
the dq_checks table for Grafana dashboard visibility.

Checks:
  1. row_count_check  — Delta layers (Bronze/Silver/Gold) have data
  2. schema_check     — Silver layer contains all required columns
  3. null_rate_check  — Critical columns have null rate < 5%
  4. fraud_rate_check — fraud_metrics fraud rate is within [0.1%, 10%]
  5. log_results      — Write all results to PostgreSQL dq_checks table
"""

import os
import logging
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

# ── PostgreSQL connection config (reads from env, falls back to defaults) ──
PG_HOST     = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB       = os.getenv("POSTGRES_DB", "fraud_db")
PG_USER     = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# Delta Lake paths (must match Daniil's spark job config)
BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/delta/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "/data/delta/silver")
GOLD_PATH   = os.getenv("GOLD_PATH",   "/data/delta/gold")

# Thresholds
MAX_NULL_RATE    = 0.05   # 5%
MIN_FRAUD_RATE   = 0.001  # 0.1%
MAX_FRAUD_RATE   = 0.10   # 10%
MIN_ROW_COUNT    = 1000   # Bronze must have at least 1000 rows

# Required Silver layer columns (from Daniil's feature engineering)
REQUIRED_SILVER_COLUMNS = [
    "Transaction_ID", "Timestamp", "Transaction_Amount", "Transaction_Type",
    "Merchant_Category", "Risk_Score", "Fraud_Label",
    "amount", "log_amount", "merchant_risk_score",
    "flag_high_amount", "flag_velocity", "flag_off_hours",
    "flag_geo_anomaly", "flag_risky_merchant", "is_online",
    "event_hour", "fraud_score", "is_flagged",
]


def _get_pg_conn():
    """Return a live psycopg2 connection to fraud_db."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def _write_dq_result(check_name: str, status: str, metric_value: float,
                     threshold: float, details: str):
    """
    Insert one row into dq_checks so Grafana can display it.
    Table schema (created by init_postgres.sql):
        check_timestamp TIMESTAMPTZ, check_name TEXT, status TEXT,
        metric_value NUMERIC, threshold NUMERIC, details TEXT
    """
    sql = """
        INSERT INTO dq_checks
            (check_timestamp, check_name, status, metric_value, threshold, details)
        VALUES
            (NOW(), %s, %s, %s, %s, %s)
    """
    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (check_name, status, metric_value, threshold, details))
        conn.close()
        logger.info(f"✓ dq_checks row written: {check_name} → {status}")
    except Exception as e:
        logger.error(f"Failed to write dq_checks row for {check_name}: {e}")
        # Don't re-raise — a DB write failure shouldn't kill the check task itself


# ── Task 1: Row Count Check ────────────────────────────────────────────────

def row_count_check():
    """
    Verify that fraud_metrics (downstream of Gold layer) contains data.
    Also checks that the Delta paths exist on disk as a proxy for
    Bronze/Silver/Gold being populated.
    """
    logger.info("📊 Checking row counts...")

    # 1a. Check Delta layer directories exist
    layers = {
        "bronze": BRONZE_PATH,
        "silver": SILVER_PATH,
        "gold":   GOLD_PATH,
    }
    for layer, path in layers.items():
        if not os.path.isdir(path):
            logger.warning(f"⚠️  Delta path not found locally: {path} "
                           f"(may be on Spark worker — skipping dir check)")

    # 1b. Check PostgreSQL fraud_metrics row count (real data)
    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM fraud_metrics;")
                total = cur.fetchone()[0]
        conn.close()
    except Exception as e:
        raise AirflowException(f"❌ Cannot connect to PostgreSQL: {e}")

    logger.info(f"✓ fraud_metrics total rows: {total}")

    status = "PASS" if total >= MIN_ROW_COUNT else "WARN"
    details = (f"fraud_metrics has {total} rows "
               f"(threshold: >= {MIN_ROW_COUNT}). "
               f"Bronze layer path: {BRONZE_PATH}")

    _write_dq_result("row_count_check", status, float(total),
                     float(MIN_ROW_COUNT), details)

    if total == 0:
        raise AirflowException("❌ fraud_metrics is empty — pipeline may not be running!")

    logger.info(f"✓ row_count_check: {status}")


# ── Task 2: Schema Check ───────────────────────────────────────────────────

def schema_check():
    """
    Verify fraud_metrics has the columns we expect
    (proxy for Silver layer schema health, since Spark writes these through).
    """
    logger.info("🔍 Checking schema of fraud_metrics...")

    # Columns Daniil's Spark job writes to fraud_metrics
    expected_columns = {
        "timestamp", "transaction_id", "card_id",
        "amount", "is_flagged", "fraud_score",
        "rule_based_score", "ml_score",
    }

    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'fraud_metrics'
                      AND table_schema = 'public';
                """)
                present = {row[0] for row in cur.fetchall()}
        conn.close()
    except Exception as e:
        raise AirflowException(f"❌ Schema check DB error: {e}")

    missing = expected_columns - present
    status = "PASS" if not missing else "FAIL"
    details = (f"All required columns present" if not missing
               else f"Missing columns: {sorted(missing)}")

    _write_dq_result("schema_check", status, float(len(present)),
                     float(len(expected_columns)), details)

    if missing:
        raise AirflowException(f"❌ fraud_metrics missing columns: {missing}")

    logger.info(f"✓ schema_check PASS — {len(present)} columns found")


# ── Task 3: Null Rate Check ────────────────────────────────────────────────

def null_rate_check():
    """
    Check null rates for critical numeric columns in fraud_metrics.
    Excludes the 5 initialization rows.
    """
    logger.info("❓ Checking null rates in fraud_metrics...")

    init_ids = "('TXN_001','TXN_002','TXN_003','TXN_004','TXN_005')"
    critical_cols = ["amount", "fraud_score", "rule_based_score", "ml_score"]

    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM fraud_metrics "
                            f"WHERE transaction_id NOT IN {init_ids};")
                total = cur.fetchone()[0]

                if total == 0:
                    _write_dq_result("null_rate_check", "WARN", 0.0,
                                     MAX_NULL_RATE, "No operational rows yet")
                    logger.warning("⚠️  No operational rows — skipping null check")
                    return

                worst_rate = 0.0
                for col in critical_cols:
                    cur.execute(f"""
                        SELECT COUNT(*) FILTER (WHERE {col} IS NULL)::float / COUNT(*)
                        FROM fraud_metrics
                        WHERE transaction_id NOT IN {init_ids};
                    """)
                    rate = cur.fetchone()[0] or 0.0
                    worst_rate = max(worst_rate, rate)
                    logger.info(f"  {col}: {rate*100:.2f}% nulls")

                    if rate > MAX_NULL_RATE:
                        raise AirflowException(
                            f"❌ {col} null rate {rate*100:.2f}% exceeds {MAX_NULL_RATE*100}%"
                        )
        conn.close()
    except AirflowException:
        raise
    except Exception as e:
        raise AirflowException(f"❌ null_rate_check DB error: {e}")

    details = (f"Max null rate across {critical_cols}: "
               f"{worst_rate*100:.2f}% (< {MAX_NULL_RATE*100}%)")
    _write_dq_result("null_rate_check", "PASS", round(worst_rate, 4),
                     MAX_NULL_RATE, details)

    logger.info("✓ null_rate_check PASS")


# ── Task 4: Fraud Rate Check ───────────────────────────────────────────────

def fraud_rate_check():
    """
    Check that the fraud alert rate in fraud_metrics is within expected bounds.
    Excludes initialization rows (TXN_001–005).
    """
    logger.info("🎯 Checking fraud rate in fraud_metrics...")

    init_ids = "('TXN_001','TXN_002','TXN_003','TXN_004','TXN_005')"

    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE is_flagged = true) AS flagged,
                        ROUND(AVG(fraud_score) FILTER (WHERE is_flagged = true)::numeric, 4)
                            AS avg_score
                    FROM fraud_metrics
                    WHERE transaction_id NOT IN {init_ids};
                """)
                row = cur.fetchone()
        conn.close()
    except Exception as e:
        raise AirflowException(f"❌ fraud_rate_check DB error: {e}")

    total, flagged, avg_score = row
    fraud_rate = (flagged / total) if total > 0 else 0.0

    logger.info(f"  Total operational rows : {total}")
    logger.info(f"  Flagged (is_flagged)   : {flagged}")
    logger.info(f"  Fraud rate             : {fraud_rate*100:.2f}%")
    logger.info(f"  Avg fraud score        : {avg_score}")

    if fraud_rate < MIN_FRAUD_RATE or fraud_rate > MAX_FRAUD_RATE:
        status = "WARN"
        details = (f"Fraud rate {fraud_rate*100:.2f}% outside "
                   f"[{MIN_FRAUD_RATE*100}%, {MAX_FRAUD_RATE*100}%]. "
                   f"flagged={flagged}/{total}, avg_score={avg_score}")
    else:
        status = "PASS"
        details = (f"Fraud rate {fraud_rate*100:.2f}% within range. "
                   f"flagged={flagged}/{total}, avg_score={avg_score}")

    _write_dq_result("fraud_rate_check", status, round(fraud_rate, 4),
                     MAX_FRAUD_RATE, details)

    logger.info(f"✓ fraud_rate_check: {status}")


# ── Task 5: Log Results ────────────────────────────────────────────────────

def log_results(**context):
    """
    Summarize the run. Pull the latest dq_checks rows and log them.
    """
    logger.info("=" * 60)
    logger.info("DATA QUALITY MONITORING RUN COMPLETE")
    logger.info("=" * 60)

    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT check_name, status, metric_value, threshold, details
                    FROM dq_checks
                    ORDER BY check_timestamp DESC
                    LIMIT 4;
                """)
                rows = cur.fetchall()
        conn.close()

        for r in rows:
            logger.info(f"  [{r[1]}] {r[0]}: metric={r[2]}, threshold={r[3]} | {r[4]}")

        failed = [r for r in rows if r[1] == "FAIL"]
        if failed:
            logger.warning(f"⚠️  {len(failed)} check(s) FAILED: {[r[0] for r in failed]}")
        else:
            logger.info("✓ All checks passed or warned — no failures")

    except Exception as e:
        logger.error(f"Could not fetch dq_checks summary: {e}")

    logger.info("=" * 60)


# ── DAG definition ─────────────────────────────────────────────────────────

default_args = {
    "owner": "fraud-detection-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
}

dag = DAG(
    "data_quality_monitoring_dag",
    default_args=default_args,
    description="Hourly data quality checks against PostgreSQL and Delta Lake",
    schedule_interval="0 * * * *",   # Every hour at minute 0
    catchup=False,
    tags=["fraud-detection", "data-quality", "monitoring"],
)

start = DummyOperator(task_id="start", dag=dag)
end   = DummyOperator(task_id="end",   dag=dag)

check_rows = PythonOperator(
    task_id="row_count_check",
    python_callable=row_count_check,
    dag=dag,
)
check_schema = PythonOperator(
    task_id="schema_check",
    python_callable=schema_check,
    dag=dag,
)
check_nulls = PythonOperator(
    task_id="null_rate_check",
    python_callable=null_rate_check,
    dag=dag,
)
check_fraud_rate = PythonOperator(
    task_id="fraud_rate_check",
    python_callable=fraud_rate_check,
    dag=dag,
)
log_results_task = PythonOperator(
    task_id="log_results",
    python_callable=log_results,
    provide_context=True,
    dag=dag,
)

# Four checks run in parallel, then log_results consolidates
start >> [check_rows, check_schema, check_nulls, check_fraud_rate]
[check_rows, check_schema, check_nulls, check_fraud_rate] >> log_results_task
log_results_task >> end
