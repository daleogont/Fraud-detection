# Problem Analysis: Real-Time Financial Fraud Detection

## 1. Problem Statement

Financial fraud is one of the fastest-growing threats in digital commerce. According to industry reports, global payment fraud losses exceed **$40 billion annually**, with card-not-present (online) fraud accounting for the majority of incidents. Traditional batch-based detection systems flag suspicious transactions only hours after they occur — by which point the damage is done.

The core challenge this project addresses:

> **How do we detect fraudulent financial transactions in real-time (sub-second) while keeping false-positive rates low enough that legitimate customers are not disrupted?**

---

## 2. Business Context

| Stakeholder | Pain Point |
|---|---|
| Bank / Payment Processor | Financial losses from undetected fraud |
| Merchant | Chargebacks and reputational damage |
| Cardholder | Unauthorized charges, account lockouts |
| Fraud Analyst | Alert fatigue from too many false positives |
| Compliance / Risk | Regulatory reporting obligations (PCI-DSS, AML) |

A fraud detection system must balance:
- **Sensitivity** (catch as much fraud as possible — recall)
- **Specificity** (avoid blocking legitimate customers — precision)
- **Speed** (flag fraud before the transaction settles)
- **Explainability** (analysts and regulators need to understand why a transaction was flagged)

---

## 3. Scope of the Problem

### 3.1 Transaction Volume
- Modern payment networks process thousands to millions of transactions per second globally.
- This system is scoped to **a single institution's transaction stream**, targeting 10–1,000 TPS (transactions per second) with burst tolerance.

### 3.2 Fraud Rate
- Typical real-world fraud rates: **0.1% – 2%** of all transactions.
- This class imbalance (99:1 legitimate vs fraud) is the central ML challenge.

### 3.3 Fraud Patterns Targeted

| Pattern | Description | Example |
|---|---|---|
| **High-Amount Spike** | Transaction far above the user's normal spend | $5,000 single purchase for a user whose avg is $50 |
| **Velocity Attack** | Many transactions in a short window | 12 transactions in 30 minutes |
| **Off-Hours Activity** | Transactions at unusual times (2–4 AM) | Late-night wire transfer |
| **Geographic Anomaly** | Transaction far from the account's home location | Card used in Lagos 1 hour after a NYC transaction |
| **Risky Merchant Category** | Crypto exchanges, gambling, wire transfers | Large transfer to a crypto exchange |

### 3.4 Detection Approach

This system uses a **hybrid detection strategy**:

1. **Rule-Based Engine** — Fast, transparent, deterministic. Produces a score from weighted fraud signals. Catches known patterns immediately, even with no ML model trained.
2. **ML Model (XGBoost)** — Learns complex non-linear patterns from historical labelled data. Improves over time through daily retraining.
3. **Ensemble Decision** — Final fraud score = `max(rule_score, ml_score)`. Threshold: ≥ 0.35 flags the transaction.

---

## 4. Key Challenges

### 4.1 Class Imbalance
Fraud is rare. A naive model predicting "no fraud" for every transaction would achieve ~99% accuracy while being completely useless. Solutions:
- Use **PR-AUC** (Precision-Recall AUC) as the primary metric, not accuracy.
- Apply class-weighting (`scale_pos_weight` in XGBoost) to penalize missed fraud more.

### 4.2 Concept Drift
Fraud patterns evolve constantly as fraudsters adapt to detection. The daily retraining DAG in Airflow addresses this by refreshing the model with fresh Silver-layer data every night.

### 4.3 Latency vs Accuracy Trade-off
- A complex deep learning model might squeeze out extra AUC but adds 50–200 ms of latency.
- XGBoost achieves near-state-of-the-art on tabular data with sub-millisecond inference — the right trade-off for streaming.

### 4.4 Feature Engineering on a Stream
Traditional ML pipelines have access to the full historical dataset at feature-computation time. In streaming, features must be computed **incrementally** over sliding windows. Spark Structured Streaming's stateful aggregations handle this.

### 4.5 Alert Fatigue
Too many false positives burn out fraud analysts and train them to ignore alerts. A precision target of **≥ 80%** on flagged transactions is the practical minimum for analyst trust.

---

## 5. Success Metrics

| Metric | Target | Rationale |
|---|---|---|
| ROC-AUC | ≥ 0.90 | Overall discriminative power |
| PR-AUC | ≥ 0.75 | Performance on imbalanced classes |
| Recall (fraud) | ≥ 0.80 | Catch most fraud |
| Precision (fraud) | ≥ 0.80 | Avoid analyst fatigue |
| Streaming latency | < 10 s | Micro-batch window for near-real-time |
| System uptime | ≥ 99.5% | Production reliability |
| DQ check pass rate | ≥ 95% | Data pipeline health |

---

## 6. Assumptions and Constraints

- **Dataset**: Synthetic dataset with 21 features and ground-truth `Fraud_Label`. Realistic class distribution (~1-5% fraud rate).
- **Infrastructure**: Single-machine Docker Compose deployment (development/educational scope).
- **Latency SLA**: 10-second micro-batch (not millisecond streaming); acceptable for flagging before settlement.
- **No PII**: The dataset uses synthetic user IDs and transaction IDs; no real customer data.
- **Retraining cadence**: Daily (midnight UTC) — sufficient given concept drift timescales in practice.

---

## 7. What This Problem Is NOT

- This is **not** a real-time authorization system (that requires sub-50ms hard latency).
- This is **not** an AML (Anti-Money Laundering) graph-analysis system (which requires cross-customer network analysis).
- This is **not** a production-grade deployment (no TLS, no HA Kafka, no multi-node Spark).

The scope is an **end-to-end educational reference architecture** that demonstrates every component of a production fraud detection pipeline.
