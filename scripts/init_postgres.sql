-- PostgreSQL Initialization Script
-- Creates databases and tables for:
-- 1. Airflow metadata
-- 2. MLflow tracking
-- 3. Fraud detection metrics
-- 4. Data quality checks

-- Create databases
CREATE DATABASE airflow;
CREATE DATABASE mlflow;

-- Connect to airflow database
\c airflow;

-- Create Airflow tables (will be auto-initialized by Airflow)
-- This is just a marker - Airflow will create its own schema

-- Connect to fraud_db (application database)
CREATE DATABASE fraud_db;
\c fraud_db;

-- =====================================================
-- FRAUD METRICS TABLE
-- Stores real-time fraud detection metrics
-- =====================================================
CREATE TABLE IF NOT EXISTS fraud_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transaction_id VARCHAR(100),
    card_id VARCHAR(50),
    amount DECIMAL(10, 2),
    fraud_score DECIMAL(5, 4),
    rule_based_score DECIMAL(5, 4),
    ml_score DECIMAL(5, 4),
    is_flagged BOOLEAN,
    rule_names VARCHAR(500)
);

-- Create index for faster queries
CREATE INDEX idx_fraud_metrics_timestamp ON fraud_metrics(timestamp);
CREATE INDEX idx_fraud_metrics_flagged ON fraud_metrics(is_flagged);

-- =====================================================
-- DATA QUALITY CHECKS TABLE
-- Stores hourly DQ monitoring results
-- =====================================================
CREATE TABLE IF NOT EXISTS dq_checks (
    id SERIAL PRIMARY KEY,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    check_name VARCHAR(100),
    status VARCHAR(20),  -- PASS or FAIL
    metric_value DECIMAL(10, 4),
    threshold DECIMAL(10, 4),
    details TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dq_checks_timestamp ON dq_checks(check_timestamp);
CREATE INDEX idx_dq_checks_status ON dq_checks(status);

-- =====================================================
-- MODEL TRAINING HISTORY TABLE
-- Tracks model versions and performance
-- =====================================================
CREATE TABLE IF NOT EXISTS model_training_history (
    id SERIAL PRIMARY KEY,
    training_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_version VARCHAR(50),
    roc_auc DECIMAL(5, 4),
    pr_auc DECIMAL(5, 4),
    precision DECIMAL(5, 4),
    recall DECIMAL(5, 4),
    f1_score DECIMAL(5, 4),
    n_samples_trained INT,
    status VARCHAR(50),
    notes TEXT
);

CREATE INDEX idx_model_history_date ON model_training_history(training_date);

-- =====================================================
-- SAMPLE DATA FOR TESTING
-- =====================================================

-- Insert sample fraud metrics
INSERT INTO fraud_metrics (transaction_id, card_id, amount, fraud_score, rule_based_score, ml_score, is_flagged, rule_names)
VALUES
    ('TXN_001', 'CARD_1234', 5000.00, 0.85, 0.80, 0.90, true, 'flag_high_amount'),
    ('TXN_002', 'CARD_5678', 45.50, 0.12, 0.10, 0.15, false, 'none'),
    ('TXN_003', 'CARD_1234', 3500.00, 0.75, 0.70, 0.80, true, 'flag_high_amount,flag_velocity'),
    ('TXN_004', 'CARD_9999', 120.00, 0.35, 0.30, 0.40, true, 'flag_risky_merchant'),
    ('TXN_005', 'CARD_2222', 89.99, 0.05, 0.03, 0.08, false, 'none');

-- Insert sample DQ checks
INSERT INTO dq_checks (check_name, status, metric_value, threshold, details)
VALUES
    ('row_count_check', 'PASS', 150000, 1000, 'Bronze layer has 150000 rows'),
    ('schema_check', 'PASS', 1.0, 1.0, 'All required columns present'),
    ('null_rate_check', 'PASS', 0.002, 0.05, 'Null rate 0.2% (< 5%)'),
    ('fraud_rate_check', 'PASS', 0.015, 0.10, 'Fraud rate 1.5% (within range)');

-- Insert sample model training history
INSERT INTO model_training_history (model_version, roc_auc, pr_auc, precision, recall, f1_score, n_samples_trained, status)
VALUES
    ('v1.0', 0.8200, 0.7850, 0.7500, 0.8100, 0.7750, 50000, 'ACTIVE'),
    ('v0.9', 0.8050, 0.7650, 0.7200, 0.7900, 0.7500, 45000, 'ARCHIVED');

-- =====================================================
-- GRANTS (for application user if needed)
-- =====================================================
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
