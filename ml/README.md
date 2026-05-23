# ML — Model Training & Evaluation

## Files

| File | Purpose |
|---|---|
| `train_model.py` | Train an XGBoost fraud classifier and log to MLflow |
| `evaluate_model.py` | Evaluate a trained model and promote to Production if improved |

---

## Model features

The model is trained on these 10 engineered features (same as the Spark job):

| Feature | Description |
|---|---|
| `amount` | Transaction amount (double) |
| `log_amount` | `log(amount + 1)` — compresses right-skewed distribution |
| `merchant_risk_score` | Per-category risk: crypto=0.9, gambling=0.8, wire_transfer=0.75, adult=0.7; max with `Risk_Score` |
| `flag_high_amount` | 1 if amount >= 3000 |
| `flag_velocity` | 1 if daily_count >= 6, failed_7d >= 3, or prior fraud > 0 |
| `flag_off_hours` | 1 if transaction hour is 2, 3, or 4 |
| `flag_geo_anomaly` | 1 if IP flagged or transaction distance > 75 km |
| `flag_risky_merchant` | 1 for crypto / gambling / wire_transfer / adult categories |
| `is_online` | 1 if Transaction_Type == "online" |
| `event_hour` | Hour of day extracted from Timestamp (0–23) |

---

## How to train

### Using the real dataset (recommended)

```bash
python ml/train_model.py \
  --source csv \
  --dataset-path data/synthetic_fraud_dataset.csv
```

### Using synthetic data (quick test)

```bash
python ml/train_model.py --source synthetic
```

The trained model is saved to `MODEL_PATH` (default: `/data/models/fraud_model.pkl`).

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `MODEL_PATH` | `/data/models/fraud_model.pkl` | Where to save the model |
| `MLFLOW_TRACKING_URI` | `http://mlflow:5001` | MLflow tracking server |
| `KAGGLE_DATASET_PATH` / `DATASET_PATH` | — | CSV path fallback (used when `--dataset-path` is not passed) |

---

## How to evaluate

Evaluation loads the saved model, runs it on the held-out test set (same 80/20 split as training), compares PR-AUC against the current Production model in the MLflow registry, and promotes if improvement >= 2%.

```bash
python ml/evaluate_model.py \
  --dataset-path data/synthetic_fraud_dataset.csv
```

Override model path:

```bash
python ml/evaluate_model.py \
  --dataset-path data/synthetic_fraud_dataset.csv \
  --model-path /data/models/fraud_model.pkl
```

Results are written to the PostgreSQL `model_training_history` table.

---

## MLflow UI

Access the experiment tracker at:

```
http://localhost:5001
```

Experiment name: `fraud-detection`

Each training run logs:
- **Params:** `test_size`, `n_features`, `feature_importance_<name>` (one per feature)
- **Metrics:** `roc_auc`, `pr_auc`, `precision`, `recall`, `f1_score`, `true_positive_rate`, `false_positive_rate`
- **Artifact:** `fraud_model.pkl`

Each evaluation run logs the same metrics and registers the model version under `fraud-detection-model` in the MLflow Model Registry.
