"""
ML EVALUATION MODULE - Model evaluation and promotion
=====================================================
Loads a trained fraud detection model, evaluates it on a held-out test set,
compares PR-AUC against the current Production model in the MLflow registry,
and promotes to Production if the improvement is >= 2%.

Usage:
    python evaluate_model.py --dataset-path data/synthetic_fraud_dataset.csv
"""

import os
import pickle
import logging
import argparse
from datetime import datetime
from typing import Optional

import pandas as pd
import psycopg2
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report, confusion_matrix, roc_auc_score,
    precision_recall_curve, auc
)
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REGISTERED_MODEL_NAME = "fraud-detection-model"
PROMOTION_THRESHOLD = 0.02  # require >= 2% PR-AUC improvement to promote


class FraudModelEvaluator:
    """Evaluates a trained fraud model and manages MLflow registry promotion."""

    FEATURES = [
        'amount', 'log_amount', 'merchant_risk_score',
        'flag_high_amount', 'flag_velocity', 'flag_off_hours',
        'flag_geo_anomaly', 'flag_risky_merchant',
        'is_online', 'event_hour'
    ]
    TARGET = 'label'

    def __init__(
        self,
        model_path: Optional[str] = None,
        mlflow_uri: Optional[str] = None,
        pg_host: Optional[str] = None,
        pg_user: Optional[str] = None,
        pg_password: Optional[str] = None,
        pg_db: Optional[str] = None,
    ):
        self.model_path = model_path or os.getenv('MODEL_PATH', '/data/models/fraud_model.pkl')
        self.mlflow_uri = mlflow_uri or os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5001')
        self.pg_host = pg_host or os.getenv('POSTGRES_HOST', 'postgres')
        self.pg_user = pg_user or os.getenv('POSTGRES_USER', 'postgres')
        self.pg_password = pg_password or os.getenv('POSTGRES_PASSWORD', '')
        self.pg_db = pg_db or os.getenv('POSTGRES_DB', 'fraud_db')

        mlflow.set_tracking_uri(self.mlflow_uri)
        self.client = MlflowClient()
        logger.info(f"✓ Evaluator initialized | model={self.model_path} | mlflow={self.mlflow_uri}")

    def load_model(self):
        """Load pickled model from disk."""
        if not os.path.exists(self.model_path):
            raise FileNotFoundError(f"Model not found at {self.model_path}")
        with open(self.model_path, 'rb') as f:
            model = pickle.load(f)
        logger.info(f"✓ Model loaded from {self.model_path}")
        return model

    def load_and_prepare_data(self, dataset_path: str):
        """
        Load CSV and return the same held-out test split used during training.
        Uses identical random_state and test_size so the split is reproducible.
        """
        # Import here to avoid circular dependency issues when running standalone
        from train_model import FraudDetectionModelTrainer

        raw = pd.read_csv(dataset_path)
        # Reuse the trainer's feature engineering logic
        trainer = FraudDetectionModelTrainer.__new__(FraudDetectionModelTrainer)
        df = trainer.prepare_kaggle_dataset(raw)

        X = df[self.FEATURES]
        y = df[self.TARGET]

        # Must match the split in train_model.py exactly
        _, X_test, _, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        logger.info(f"✓ Test set: {len(X_test)} samples, fraud rate {y_test.mean()*100:.2f}%")
        return X_test, y_test

    def evaluate(self, model, X_test, y_test) -> dict:
        """Compute all evaluation metrics on the test set."""
        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]

        roc_auc = roc_auc_score(y_test, y_proba)
        precision_vals, recall_vals, _ = precision_recall_curve(y_test, y_proba)
        pr_auc = auc(recall_vals, precision_vals)

        report = classification_report(y_test, y_pred, output_dict=True)
        tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()

        metrics = {
            'roc_auc': roc_auc,
            'pr_auc': pr_auc,
            'precision': report['1']['precision'],
            'recall': report['1']['recall'],
            'f1_score': report['1']['f1-score'],
            'true_positive_rate': tp / (tp + fn) if (tp + fn) > 0 else 0.0,
            'false_positive_rate': fp / (fp + tn) if (fp + tn) > 0 else 0.0,
            'n_test_samples': len(y_test),
        }

        logger.info("\n📊 Evaluation metrics:")
        for key, val in metrics.items():
            if isinstance(val, float):
                logger.info(f"   {key}: {val:.4f}")
        return metrics

    def get_production_pr_auc(self) -> Optional[float]:
        """Return PR-AUC of the current Production model from the MLflow registry, or None."""
        try:
            versions = self.client.get_latest_versions(REGISTERED_MODEL_NAME, stages=["Production"])
            if not versions:
                logger.info("No Production model in registry — will register as first.")
                return None
            run_id = versions[0].run_id
            pr_auc = self.client.get_run(run_id).data.metrics.get('pr_auc')
            logger.info(f"✓ Production model PR-AUC: {pr_auc:.4f} (run_id={run_id})")
            return pr_auc
        except mlflow.exceptions.MlflowException as e:
            logger.warning(f"Could not fetch production model metrics: {e}")
            return None

    def register_and_maybe_promote(
        self, run_id: str, new_pr_auc: float, production_pr_auc: Optional[float]
    ) -> str:
        """
        Register the model version from run_id.
        Promote to Production if improvement over current Production >= 2%,
        otherwise move to Staging.
        """
        model_uri = f"runs:/{run_id}/model"
        mv = mlflow.register_model(model_uri, REGISTERED_MODEL_NAME)
        version = mv.version
        logger.info(f"✓ Registered as version {version}")

        should_promote = (
            production_pr_auc is None
            or (new_pr_auc - production_pr_auc) >= PROMOTION_THRESHOLD
        )

        if should_promote:
            self.client.transition_model_version_stage(
                name=REGISTERED_MODEL_NAME,
                version=version,
                stage="Production",
                archive_existing_versions=True,
            )
            reason = (
                "first registration"
                if production_pr_auc is None
                else f"+{(new_pr_auc - production_pr_auc)*100:.2f}% >= 2% threshold"
            )
            logger.info(f"✓ Version {version} promoted to Production ({reason})")
            return "promoted"
        else:
            self.client.transition_model_version_stage(
                name=REGISTERED_MODEL_NAME,
                version=version,
                stage="Staging",
            )
            gap = (new_pr_auc - production_pr_auc) * 100
            logger.info(f"↩ Version {version} moved to Staging — improvement {gap:.2f}% < 2%")
            return "staging"

    def write_to_postgres(self, metrics: dict, status: str):
        """Insert evaluation result into the model_training_history table."""
        try:
            conn = psycopg2.connect(
                host=self.pg_host,
                user=self.pg_user,
                password=self.pg_password,
                dbname=self.pg_db,
            )
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO model_training_history
                    (model_version, roc_auc, pr_auc, precision, recall,
                     f1_score, n_samples_trained, status, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    datetime.now().strftime("eval_%Y%m%d_%H%M%S"),
                    round(metrics['roc_auc'], 4),
                    round(metrics['pr_auc'], 4),
                    round(metrics['precision'], 4),
                    round(metrics['recall'], 4),
                    round(metrics['f1_score'], 4),
                    metrics['n_test_samples'],
                    status.upper(),
                    (
                        f"Held-out test evaluation. "
                        f"TPR={metrics['true_positive_rate']:.4f} "
                        f"FPR={metrics['false_positive_rate']:.4f}"
                    ),
                ),
            )
            conn.commit()
            cur.close()
            conn.close()
            logger.info("✓ Result written to PostgreSQL model_training_history")
        except Exception as e:
            logger.error(f"❌ PostgreSQL write failed: {e}")

    def run(self, dataset_path: str):
        """Full evaluation pipeline: evaluate → compare → register → write DB."""
        logger.info("=" * 60)
        logger.info("FRAUD MODEL EVALUATION")
        logger.info("=" * 60)

        model = self.load_model()
        X_test, y_test = self.load_and_prepare_data(dataset_path)
        metrics = self.evaluate(model, X_test, y_test)

        production_pr_auc = self.get_production_pr_auc()

        mlflow.set_experiment("fraud-detection")
        with mlflow.start_run(run_name=f"eval_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as run:
            mlflow.log_metrics({k: v for k, v in metrics.items() if isinstance(v, float)})
            status = self.register_and_maybe_promote(
                run.info.run_id, metrics['pr_auc'], production_pr_auc
            )

        self.write_to_postgres(metrics, status)

        logger.info("=" * 60)
        logger.info(f"✓ EVALUATION COMPLETE — status: {status.upper()}")
        logger.info("=" * 60)
        return metrics, status


def main():
    parser = argparse.ArgumentParser(description="Evaluate fraud detection model")
    parser.add_argument('--dataset-path', required=True, help='Path to CSV dataset')
    parser.add_argument('--model-path', default=None, help='Override model path')
    args = parser.parse_args()

    evaluator = FraudModelEvaluator(model_path=args.model_path)
    evaluator.run(dataset_path=args.dataset_path)


if __name__ == '__main__':
    main()
