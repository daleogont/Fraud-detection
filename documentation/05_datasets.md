# Dataset Documentation

## 1. Overview

| Property | Value |
|---|---|
| **File** | `data/synthetic_fraud_dataset.csv` |
| **Type** | Synthetic tabular dataset (simulated financial transactions) |
| **Rows** | 50,000 transactions |
| **Columns** | 21 features + 1 target label |
| **Target** | `Fraud_Label` (binary: 0 = legitimate, 1 = fraud) |
| **Class distribution** | 67.87% legitimate / 32.13% fraud |
| **Date range** | 2023 (synthetic timestamps) |
| **PII** | None — fully anonymized synthetic IDs |

---

## 2. Class Distribution

```
Legitimate (0):  33,933  ████████████████████████████████████████  67.87%
Fraud (1):       16,067  ███████████████████                       32.13%
```

> **Note on class balance**: This dataset has a relatively high fraud rate (32%) compared to real-world fraud rates (0.1–2%). In a production deployment, `scale_pos_weight` in XGBoost would be set much higher (e.g., 49× for a 2% fraud rate). For this educational dataset, class weighting is still applied but at a milder ratio (~2:1).

---

## 3. Feature Schema

### 3.1 Transaction Identifiers

| Column | Type | Description | Example |
|---|---|---|---|
| `Transaction_ID` | String | Unique transaction identifier | `TXN_33553` |
| `User_ID` | String | Anonymized user identifier | `USER_1834` |
| `Timestamp` | Datetime | Transaction time (`YYYY-MM-DD HH:MM:SS`) | `2023-08-14 19:30:00` |

### 3.2 Transaction Features

| Column | Type | Range / Values | Description |
|---|---|---|---|
| `Transaction_Amount` | Float | 0.00 – 1,174.14 | Transaction value in USD |
| `Transaction_Type` | Categorical | POS, Online, ATM Withdrawal, Bank Transfer | Channel of transaction |
| `Account_Balance` | Float | Positive | Account balance at time of transaction |
| `Merchant_Category` | Categorical | Clothing, Groceries, Travel, Restaurants, Electronics | Merchant industry |
| `Is_Weekend` | Binary | 0, 1 | Whether the transaction occurred on a weekend |

### 3.3 Device and Location Features

| Column | Type | Range / Values | Description |
|---|---|---|---|
| `Device_Type` | Categorical | Tablet, Mobile, Laptop | Device used for transaction |
| `Location` | String | City name (e.g., Sydney, New York) | Transaction location |
| `IP_Address_Flag` | Binary | 0, 1 | 1 = IP flagged as suspicious/proxy |
| `Transaction_Distance` | Float | 0.0 – 2,500+ | Distance (km) from account home location |

### 3.4 Behavioral / Risk Features

| Column | Type | Range / Values | Description |
|---|---|---|---|
| `Daily_Transaction_Count` | Integer | 1 – 20+ | Number of transactions by this user today |
| `Avg_Transaction_Amount_7d` | Float | Positive | User's average transaction amount over last 7 days |
| `Failed_Transaction_Count_7d` | Integer | 0 – 10+ | Failed transactions by this user in last 7 days |
| `Previous_Fraudulent_Activity` | Binary | 0, 1 | 1 = user has prior fraud history |
| `Risk_Score` | Float | 0.0 – 1.0 | Pre-computed risk score (source system) |

### 3.5 Card Features

| Column | Type | Range / Values | Description |
|---|---|---|---|
| `Card_Type` | Categorical | Visa, Mastercard, Amex | Payment card network |
| `Card_Age` | Integer | Days | Age of the card in days |
| `Authentication_Method` | Categorical | Password, OTP, Biometric | How the transaction was authenticated |

### 3.6 Target Label

| Column | Type | Values | Description |
|---|---|---|---|
| `Fraud_Label` | Binary | 0 = legitimate, 1 = fraud | Ground truth fraud indicator |

---

## 4. Descriptive Statistics

### Transaction Amount

| Statistic | Value |
|---|---|
| Minimum | $0.00 |
| 25th percentile | $28.68 |
| Median | $69.67 |
| Mean | $99.41 |
| 75th percentile | $138.86 |
| Maximum | $1,174.14 |

The distribution is right-skewed — a small number of high-value transactions pull the mean above the median. This is typical of real transaction data.

### Transaction Type Distribution

| Type | Count | Percentage |
|---|---|---|
| POS | 12,549 | 25.1% |
| Online | 12,546 | 25.1% |
| ATM Withdrawal | 12,453 | 24.9% |
| Bank Transfer | 12,452 | 24.9% |

Evenly distributed across channels.

### Merchant Category Distribution

| Category | Count | Percentage |
|---|---|---|
| Clothing | 10,033 | 20.1% |
| Groceries | 10,019 | 20.0% |
| Travel | 10,015 | 20.0% |
| Restaurants | 9,976 | 20.0% |
| Electronics | 9,957 | 19.9% |

Evenly distributed (synthetic; real data would be heavily weighted toward groceries/restaurants).

### Device Type Distribution

| Device | Count | Percentage |
|---|---|---|
| Tablet | 16,779 | 33.6% |
| Mobile | 16,640 | 33.3% |
| Laptop | 16,581 | 33.2% |

Evenly distributed (real data would be ~60–70% mobile).

---

## 5. Feature Relationships with Fraud

Based on domain knowledge and exploratory analysis:

| Feature | Fraud Signal | Notes |
|---|---|---|
| `Transaction_Amount` | High amounts more likely fraud | Rule: > $3,000 triggers flag (max in this dataset is ~$1,174) |
| `Daily_Transaction_Count` | High counts indicate velocity | Rule: ≥ 6 per day triggers flag |
| `Failed_Transaction_Count_7d` | Card testing behavior | Rule: ≥ 3 failures triggers flag |
| `Previous_Fraudulent_Activity` | Strong predictor | Rule: > 0 triggers velocity flag |
| `IP_Address_Flag` | Proxy / suspicious IP | Rule: contributes to geo anomaly flag |
| `Transaction_Distance` | Geo impossibility | Rule: > 75 km triggers geo flag |
| `Merchant_Category` | Crypto/gambling high-risk | Note: this dataset's categories are low-risk (groceries, clothing, etc.) |
| `Timestamp (hour)` | Off-hours activity | Rule: 2–4 AM triggers off-hours flag |
| `Risk_Score` | Pre-computed signal | Included as a feature in ML model |

---

## 6. Feature Engineering Applied

The Silver layer derives these additional features from the raw columns:

| Derived Feature | Source | Description |
|---|---|---|
| `log_amount` | `Transaction_Amount` | `log1p(amount)` — normalizes right-skewed distribution |
| `event_hour` | `Timestamp` | Hour of day (0–23) |
| `amount_bucket` | `Transaction_Amount` | Bucketed: low(<$50), medium(<$200), high(<$500), very_high |
| `merchant_risk_score` | `Merchant_Category` | Lookup: gambling=0.9, crypto=0.85, wire=0.8, others=0.1 |
| `is_online` | `Transaction_Type` | 1 if Online, 0 otherwise |
| `flag_high_amount` | `Transaction_Amount` | 1 if > $3,000 |
| `flag_velocity` | `Daily_Transaction_Count`, `Failed_Transaction_Count_7d`, `Previous_Fraudulent_Activity` | 1 if velocity threshold exceeded |
| `flag_off_hours` | `Timestamp` | 1 if hour ∈ {2, 3} |
| `flag_geo_anomaly` | `IP_Address_Flag`, `Transaction_Distance` | 1 if IP flagged OR distance > 75 |
| `flag_risky_merchant` | `Merchant_Category` | 1 if crypto/gambling/wire |

---

## 7. Dataset Limitations and Biases

### 7.1 Class Imbalance vs Reality
- This dataset: ~32% fraud rate.
- Real-world: 0.1–2% fraud rate.
- **Impact**: ML model may not generalize to a real deployment without adjusting `scale_pos_weight` significantly.

### 7.2 Even Distribution of Categorical Features
- Transaction types, merchant categories, and device types are uniformly distributed.
- In real data, POS and mobile transactions would dominate.
- **Impact**: The model may underfit patterns related to channel-specific fraud behaviors.

### 7.3 Missing High-Risk Merchant Categories
- The dataset contains only: Clothing, Groceries, Travel, Restaurants, Electronics.
- Real high-risk categories (gambling, crypto exchanges, wire transfers) are absent.
- **Impact**: The `flag_risky_merchant` rule will never fire on this dataset.

### 7.4 Capped Transaction Amount
- Maximum transaction amount is ~$1,174 — well below the $3,000 threshold for `flag_high_amount`.
- **Impact**: The high-amount rule will never fire on this dataset.

### 7.5 Synthetic Timestamps
- Timestamps are synthetic and may not reflect realistic intraday patterns.
- **Impact**: The `flag_off_hours` (2–4 AM) rule may not align with real fraud timing patterns.

### 7.6 No Network / Graph Features
- No cross-user relationship data (e.g., shared devices, linked accounts).
- **Impact**: Graph-based fraud rings (a common real-world pattern) cannot be detected.

---

## 8. Data Quality Checks

The `data_quality_monitoring_dag` validates the following on the Silver Delta table every hour:

| Check | Condition | Pass Threshold |
|---|---|---|
| Row count | All 3 layers readable + non-empty | count > 0 |
| Schema check | Silver has all expected columns | 100% column presence |
| Null rate | Critical columns (`Transaction_ID`, `User_ID`, `Transaction_Amount`, `Fraud_Label`) | < 5% nulls |
| Fraud rate | Silver layer fraud rate | 0.1% – 10% |

---

## 9. Data Lineage

```
data/synthetic_fraud_dataset.csv
          │
          │ producer/transaction_generator.py
          │ (row-by-row replay at configurable TPS)
          ▼
Kafka: raw-transactions
          │
          │ spark_jobs/fraud_streaming_job.py
          │ (Bronze: schema enforcement + append)
          ▼
Delta: /data/delta/bronze
          │
          │ spark_jobs/fraud_streaming_job.py
          │ (Silver: feature engineering + scoring)
          ▼
Delta: /data/delta/silver
          │
          ├──► ml/train_model.py  (daily retrain via Airflow)
          │         │
          │         ▼
          │    models-data/fraud_model.pkl
          │
          │ spark_jobs/fraud_streaming_job.py
          │ (Gold: filter is_flagged=1)
          ▼
Delta: /data/delta/gold
          │
          ├──► Kafka: flagged-transactions
          └──► PostgreSQL: fraud_metrics
                    │
                    └──► Grafana: Live Dashboard
```
