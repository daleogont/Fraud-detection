"""
ML TRAINING MODULE - XGBoost Model Training
============================================
Trains a fraud detection model using XGBoost.

For ML students: This is how to build a fraud detection model:
1. Load labeled data (transactions with fraud labels)
2. Select features (columns the model uses)
3. Split into train/test
4. Train XGBoost classifier
5. Evaluate with fraud-specific metrics (precision, recall, ROC-AUC, PR-AUC)
6. Track metrics in MLflow
7. Save model for serving

Why XGBoost for fraud?
✓ Fast training
✓ Good with tabular data
✓ Feature importance
✓ Handles imbalanced classes
✓ Production-ready
"""

import os
import pickle
import logging
from datetime import datetime
import argparse

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    classification_report, confusion_matrix, roc_auc_score, 
    roc_curve, precision_recall_curve, auc
)
import xgboost as xgb
import mlflow
import mlflow.sklearn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FraudDetectionModelTrainer:
    """Trains XGBoost model for fraud detection."""
    
    # These are the features the Spark job engineers
    FEATURES = [
        'amount', 'log_amount', 'merchant_risk_score',
        'flag_high_amount', 'flag_velocity', 'flag_off_hours', 
        'flag_geo_anomaly', 'flag_risky_merchant', 
        'is_online', 'event_hour'
    ]
    TARGET = 'label'  # Fraud ground truth
    RISKY_MERCHANT_CATEGORIES = {'crypto', 'gambling', 'wire transfer', 'wire_transfer', 'adult'}

    KAGGLE_COLUMN_MAP = {
        'Transaction_ID': 'transaction_id',
        'User_ID': 'user_id',
        'Transaction_Amount': 'amount',
        'Transaction_Type': 'transaction_type',
        'Timestamp': 'timestamp',
        'Account_Balance': 'account_balance',
        'Device_Type': 'device_type',
        'Location': 'location',
        'Merchant_Category': 'merchant_category',
        'IP_Address_Flag': 'ip_address_flag',
        'Previous_Fraudulent_Activity': 'previous_fraudulent_activity',
        'Daily_Transaction_Count': 'daily_transaction_count',
        'Avg_Transaction_Amount_7d': 'avg_transaction_amount_7d',
        'Failed_Transaction_Count_7d': 'failed_transaction_count_7d',
        'Card_Type': 'card_type',
        'Card_Age': 'card_age',
        'Transaction_Distance': 'transaction_distance',
        'Authentication_Method': 'authentication_method',
        'Risk_Score': 'risk_score',
        'Is_Weekend': 'is_weekend',
        'Fraud_Label': 'label',
    }
    
    def __init__(self, model_path: str = None, mlflow_uri: str = None):
        """
        Initialize trainer and connect to MLflow.

        Args:
            model_path: Destination path for the pickled model file.
                        Falls back to MODEL_PATH env var, then /data/models/fraud_model.pkl.
            mlflow_uri: MLflow tracking server URI.
                        Falls back to MLFLOW_TRACKING_URI env var, then http://mlflow:5001.
        """
        self.model_path = model_path or os.getenv('MODEL_PATH', '/data/models/fraud_model.pkl')
        self.mlflow_uri = mlflow_uri or os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5001')
        
        # Create model directory if needed
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        
        mlflow.set_tracking_uri(self.mlflow_uri)
        mlflow.set_experiment("fraud-detection")
        
        logger.info(f"✓ Trainer initialized")
        logger.info(f"  Model path: {self.model_path}")
        logger.info(f"  MLflow URI: {self.mlflow_uri}")

    def load_csv_data(self, dataset_path: str) -> pd.DataFrame:
        """Load the Kaggle fraud dataset from CSV."""
        if not dataset_path:
            raise ValueError("A dataset path is required when source='csv'.")

        if not os.path.exists(dataset_path):
            raise FileNotFoundError(f"Dataset not found: {dataset_path}")

        logger.info(f"📂 Loading dataset from {dataset_path}")
        return pd.read_csv(dataset_path)

    def prepare_kaggle_dataset(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Rename raw dataset columns and engineer all 10 model features in-place.

        Applies the same feature logic as fraud_streaming_job.engineer_features()
        so that training and serving use identical transformations. Thresholds
        that must stay in sync across both files:
            flag_high_amount    >= 3000
            flag_velocity       daily_count >= 6 | failed >= 3 | prior_fraud > 0
            flag_off_hours      hour in [2, 4]
            flag_geo_anomaly    ip_flag == 1 | distance > 75
            merchant_risk_score crypto=0.9, gambling=0.8, wire_transfer=0.75, adult=0.7

        Args:
            data: Raw DataFrame with original Kaggle column names (Transaction_Amount, etc.).

        Returns:
            DataFrame with renamed columns and all 10 FEATURES columns populated.

        Raises:
            ValueError: If any required source column is absent after renaming.
        """
        df = data.rename(columns=self.KAGGLE_COLUMN_MAP).copy()

        required_columns = [
            'amount', 'transaction_type', 'timestamp', 'merchant_category',
            'ip_address_flag', 'previous_fraudulent_activity', 'daily_transaction_count',
            'failed_transaction_count_7d', 'transaction_distance', 'risk_score', 'label'
        ]
        missing_columns = [column for column in required_columns if column not in df.columns]
        if missing_columns:
            raise ValueError(f"Dataset is missing required columns: {missing_columns}")

        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
        df['log_amount'] = np.log1p(df['amount'])

        merchant_risk_lookup = {
            'crypto': 0.9,
            'gambling': 0.8,
            'wire transfer': 0.75,
            'wire_transfer': 0.75,
            'adult': 0.7,
        }
        merchant_risk_score = df['merchant_category'].astype(str).str.lower().map(merchant_risk_lookup).fillna(0.2)
        risk_score = pd.to_numeric(df['risk_score'], errors='coerce').fillna(0.0)
        df['merchant_risk_score'] = np.maximum(merchant_risk_score, risk_score)

        df['flag_high_amount'] = (df['amount'] >= 3000).astype(int)

        daily_transaction_count = pd.to_numeric(df['daily_transaction_count'], errors='coerce').fillna(0)
        failed_transaction_count = pd.to_numeric(df['failed_transaction_count_7d'], errors='coerce').fillna(0)
        previous_fraudulent_activity = pd.to_numeric(df['previous_fraudulent_activity'], errors='coerce').fillna(0)
        df['flag_velocity'] = (
            (daily_transaction_count >= 6) |
            (failed_transaction_count >= 3) |
            (previous_fraudulent_activity > 0)
        ).astype(int)

        df['event_hour'] = df['timestamp'].dt.hour.fillna(0).astype(int)
        df['flag_off_hours'] = ((df['event_hour'] >= 2) & (df['event_hour'] <= 4)).astype(int)

        ip_address_flag = pd.to_numeric(df['ip_address_flag'], errors='coerce').fillna(0)
        transaction_distance = pd.to_numeric(df['transaction_distance'], errors='coerce').fillna(0.0)
        df['flag_geo_anomaly'] = ((ip_address_flag == 1) | (transaction_distance > 75)).astype(int)

        df['flag_risky_merchant'] = df['merchant_category'].astype(str).str.lower().isin(self.RISKY_MERCHANT_CATEGORIES).astype(int)
        df['is_online'] = df['transaction_type'].astype(str).str.lower().eq('online').astype(int)
        df['label'] = pd.to_numeric(df['label'], errors='coerce').fillna(0).astype(int)

        return df
    
    def generate_synthetic_data(self, n_samples: int = 5000) -> pd.DataFrame:
        """
        Generate synthetic training data for demo purposes.
        
        For students: In production, this would come from historical
        transactions in the Silver Delta Lake layer. For this demo,
        we generate realistic synthetic data.
        
        Args:
            n_samples: Number of samples to generate
            
        Returns:
            DataFrame with features and labels
        """
        logger.info(f"🔨 Generating {n_samples} synthetic transactions...")
        
        np.random.seed(42)
        
        data = {
            'amount': np.random.lognormal(3.5, 1.2, n_samples),
            'log_amount': np.log1p(np.random.lognormal(3.5, 1.2, n_samples)),
            'merchant_risk_score': np.random.uniform(0.1, 1.0, n_samples),
            'flag_high_amount': np.random.randint(0, 2, n_samples),
            'flag_velocity': np.random.randint(0, 2, n_samples),
            'flag_off_hours': np.random.randint(0, 2, n_samples),
            'flag_geo_anomaly': np.random.randint(0, 2, n_samples),
            'flag_risky_merchant': np.random.randint(0, 2, n_samples),
            'is_online': np.random.randint(0, 2, n_samples),
            'event_hour': np.random.randint(0, 24, n_samples),
        }
        
        df = pd.DataFrame(data)
        
        # Generate labels: fraud if multiple flags or risky features
        fraud_prob = (
            df['flag_high_amount'] * 0.3 +
            df['flag_velocity'] * 0.25 +
            df['flag_off_hours'] * 0.15 +
            df['flag_geo_anomaly'] * 0.2 +
            df['flag_risky_merchant'] * 0.1
        )
        df['label'] = (fraud_prob + np.random.normal(0, 0.1, n_samples)) > 0.35
        df['label'] = df['label'].astype(int)
        
        fraud_count = df['label'].sum()
        fraud_rate = fraud_count / len(df) * 100
        logger.info(f"✓ Generated data: {len(df)} samples, {fraud_count} fraud ({fraud_rate:.1f}%)")
        
        return df
    
    def train(self, data: pd.DataFrame = None, test_size: float = 0.2, source: str = 'synthetic', dataset_path: str = None):
        """
        Train an XGBoost classifier and log everything to MLflow.

        Data loading priority:
            1. data argument (if provided directly)
            2. CSV file at dataset_path / KAGGLE_DATASET_PATH / DATASET_PATH env vars
            3. Synthetic data (5 000 rows) when source='synthetic'

        MLflow artifacts logged per run:
            Params:   test_size, n_features, feature_importance_<name> (x10)
            Metrics:  roc_auc, pr_auc, precision, recall, f1_score, tpr, fpr
            Artifact: fraud_model.pkl (also registered via mlflow.sklearn.log_model)

        Args:
            data:         Pre-built feature DataFrame (skips loading step if provided).
            test_size:    Fraction of data held out for evaluation (default 0.2).
            source:       'csv' to load from file, 'synthetic' to generate data.
            dataset_path: Path to the CSV dataset; required when source='csv'.

        Returns:
            Trained XGBClassifier instance (also saved to self.model_path).
        """
        # Generate or use provided data
        if data is None:
            if source == 'csv':
                csv_path = dataset_path or os.getenv('KAGGLE_DATASET_PATH') or os.getenv('DATASET_PATH')
                data = self.load_csv_data(csv_path)
            else:
                data = self.generate_synthetic_data()

        if 'Fraud_Label' in data.columns or 'Transaction_Amount' in data.columns:
            data = self.prepare_kaggle_dataset(data)
        
        logger.info(f"\n{'='*60}")
        logger.info("TRAINING XGBOOST FRAUD DETECTION MODEL")
        logger.info(f"{'='*60}")
        
        # Prepare features and target
        X = data[self.FEATURES]
        y = data[self.TARGET]
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42, stratify=y
        )
        
        logger.info(f"\n📊 Data split:")
        logger.info(f"   Train: {len(X_train)} samples")
        logger.info(f"   Test:  {len(X_test)} samples")
        logger.info(f"   Fraud rate (train): {y_train.mean()*100:.2f}%")
        logger.info(f"   Fraud rate (test):  {y_test.mean()*100:.2f}%")
        
        # Start MLflow run
        with mlflow.start_run(run_name=f"fraud_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            
            # Log parameters
            mlflow.log_param("test_size", test_size)
            mlflow.log_param("n_features", len(self.FEATURES))
            
            # Train model
            logger.info(f"\n🤖 Training XGBoost...")
            model = xgb.XGBClassifier(
                n_estimators=100,
                max_depth=5,
                learning_rate=0.1,
                random_state=42,
                scale_pos_weight=len(y_train[y_train==0]) / len(y_train[y_train==1]),  # Handle imbalance
                verbosity=1
            )
            model.fit(X_train, y_train)
            logger.info("✓ Model trained")
            
            # Evaluate
            logger.info(f"\n📈 Evaluating on test set...")
            y_pred = model.predict(X_test)
            y_pred_proba = model.predict_proba(X_test)[:, 1]
            
            # Calculate metrics
            metrics = self._calculate_metrics(y_test, y_pred, y_pred_proba)
            
            # Log metrics
            mlflow.log_metrics(metrics)
            
            # Log feature importance
            logger.info(f"\n📊 Feature importance:")
            for feature, importance in sorted(
                zip(self.FEATURES, model.feature_importances_), 
                key=lambda x: x[1], reverse=True
            ):
                logger.info(f"   {feature}: {importance:.4f}")
                mlflow.log_param(f"feature_importance_{feature}", importance)
            
            # Save model
            logger.info(f"\n💾 Saving model...")
            with open(self.model_path, 'wb') as f:
                pickle.dump(model, f)
            logger.info(f"✓ Model saved to {self.model_path}")
            
            # Log model with MLflow
            mlflow.sklearn.log_model(model, "model")
            mlflow.log_artifact(self.model_path)
            
            logger.info(f"\n{'='*60}")
            logger.info("✓ TRAINING COMPLETE")
            logger.info(f"{'='*60}")
            
            return model
    
    def _calculate_metrics(self, y_true, y_pred, y_pred_proba):
        """
        Calculate fraud-specific metrics.
        
        For students: Different metrics matter for fraud:
        - ROC-AUC: How well does model rank fraud vs normal?
        - PR-AUC: How good at finding fraud among predicted positives?
        - Precision: Of flagged txns, how many are actually fraud?
        - Recall: Of all frauds, how many did we catch?
        - F1: Balance between precision and recall
        
        Args:
            y_true: True labels
            y_pred: Predicted labels (0 or 1)
            y_pred_proba: Predicted probabilities
            
        Returns:
            Dictionary of metrics
        """
        metrics = {}
        
        # ROC-AUC
        metrics['roc_auc'] = roc_auc_score(y_true, y_pred_proba)
        
        # PR-AUC (more important for imbalanced data)
        precision, recall, _ = precision_recall_curve(y_true, y_pred_proba)
        metrics['pr_auc'] = auc(recall, precision)
        
        # Classification report
        report = classification_report(y_true, y_pred, output_dict=True)
        metrics['precision'] = report['1']['precision']
        metrics['recall'] = report['1']['recall']
        metrics['f1_score'] = report['1']['f1-score']
        
        # Confusion matrix metrics
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
        metrics['true_positive_rate'] = tp / (tp + fn) if (tp + fn) > 0 else 0
        metrics['false_positive_rate'] = fp / (fp + tn) if (fp + tn) > 0 else 0
        
        logger.info(f"\n📊 Metrics:")
        logger.info(f"   ROC-AUC:  {metrics['roc_auc']:.4f}")
        logger.info(f"   PR-AUC:   {metrics['pr_auc']:.4f}")
        logger.info(f"   Precision: {metrics['precision']:.4f}")
        logger.info(f"   Recall:    {metrics['recall']:.4f}")
        logger.info(f"   F1-Score:  {metrics['f1_score']:.4f}")
        logger.info(f"   TPR:       {metrics['true_positive_rate']:.4f}")
        logger.info(f"   FPR:       {metrics['false_positive_rate']:.4f}")
        
        return metrics


def main():
    """Entry point for model training."""
    parser = argparse.ArgumentParser(description="Train fraud detection model")
    parser.add_argument('--source', default='synthetic', choices=['synthetic', 'csv', 'delta'], help='Data source')
    parser.add_argument('--dataset-path', default=os.getenv('KAGGLE_DATASET_PATH') or os.getenv('DATASET_PATH'), help='Path to the Kaggle fraud dataset CSV')
    args = parser.parse_args()
    
    trainer = FraudDetectionModelTrainer()
    trainer.train(source=args.source, dataset_path=args.dataset_path)


if __name__ == '__main__':
    main()
