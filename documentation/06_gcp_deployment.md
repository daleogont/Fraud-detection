# Running the Fraud Detection System on Google Cloud Platform

## Two Approaches

| Approach | Effort | Cost | Code Changes |
|---|---|---|---|
| **A — Docker Compose on a GCE VM** | ~30 min | ~$3–5/day | None |
| **B — Cloud-native managed services** | ~2–3 days | ~$10–20/day | Moderate |

**Start with Approach A.** It is identical to running locally — just on a cloud VM. Approach B is the production-grade path and is covered in Part 2 of this guide.

---

# Part 1 — Docker Compose on a GCE VM (Recommended for learning)

## Prerequisites

- A Google account
- [Google Cloud SDK (`gcloud`)](https://cloud.google.com/sdk/docs/install) installed locally
- A GCP project created (or use the default one)

---

## Step 1 — Set Up Your GCP Project

```bash
# Authenticate
gcloud auth login

# Create a new project (or skip if you have one)
gcloud projects create fraud-detection-demo --name="Fraud Detection"

# Set it as active
gcloud config set project fraud-detection-demo

# Enable Compute Engine API
gcloud services enable compute.googleapis.com

# Check your billing account is linked (required for VMs)
gcloud billing accounts list
gcloud billing projects link fraud-detection-demo \
  --billing-account=YOUR_BILLING_ACCOUNT_ID
```

---

## Step 2 — Create the VM

```bash
gcloud compute instances create fraud-detection-vm \
  --zone=us-central1-a \
  --machine-type=n2-standard-4 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --boot-disk-type=pd-ssd \
  --tags=fraud-detection \
  --metadata=startup-script='#!/bin/bash
    apt-get update -y
    apt-get install -y docker.io docker-compose-v2 git make
    systemctl enable docker
    systemctl start docker
    usermod -aG docker $USER'
```

> **Why n2-standard-4?** 4 vCPUs + 16 GB RAM. This project runs 13 Docker containers; anything smaller will run out of memory. With GCP's $300 free credit this costs ~$0 for new accounts.

---

## Step 3 — Open Required Ports (Firewall Rules)

```bash
gcloud compute firewall-rules create fraud-detection-ports \
  --target-tags=fraud-detection \
  --allow=tcp:3000,tcp:5001,tcp:8080,tcp:8081,tcp:8082 \
  --source-ranges=0.0.0.0/0 \
  --description="Grafana, MLflow, Kafka UI, Spark UI, Airflow"
```

> **Security note**: `0.0.0.0/0` opens ports to the public internet — fine for a demo. For anything real, restrict to your IP: `--source-ranges=YOUR.IP.ADDRESS/32`

---

## Step 4 — SSH Into the VM

```bash
gcloud compute ssh fraud-detection-vm --zone=us-central1-a
```

All remaining commands run **inside the VM**.

---

## Step 5 — Clone the Repo and Configure

```bash
# Verify Docker is ready
docker --version
docker compose version

# Clone your repo
git clone https://github.com/khurshidnm/fraud-detection-system.git
cd fraud-detection-system

# Create your environment file
cp .env.example .env

# Generate required secrets
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python3 -c "import secrets, base64; print(base64.b64encode(secrets.token_bytes(24)).decode())"

# Edit .env and fill in the generated values
nano .env
```

Your `.env` should look like:
```env
POSTGRES_USER=fraud_admin
POSTGRES_PASSWORD=choose_a_strong_password
AIRFLOW_FERNET_KEY=<output from first python3 command>
AIRFLOW_SECRET_KEY=<output from second python3 command>
AIRFLOW_ADMIN_PASSWORD=choose_a_password
GRAFANA_PASSWORD=choose_a_password
TRANSACTIONS_PER_SECOND=10
FRAUD_RATE=0.01
```

---

## Step 6 — Start the System

```bash
# Start all 13 services (takes 3–5 minutes on first run)
make up

# Watch the logs to confirm everything started
make logs

# Confirm all containers are healthy
make status
```

Expected output from `make status`:
```
fraud-detection-system-kafka-1            running
fraud-detection-system-spark-master-1     running
fraud-detection-system-spark-worker-1     running
fraud-detection-system-spark-streaming-1  running
fraud-detection-system-transaction-producer-1  running
fraud-detection-system-postgres-1         running
fraud-detection-system-airflow-webserver-1  running
fraud-detection-system-airflow-scheduler-1  running
fraud-detection-system-mlflow-1           running
fraud-detection-system-grafana-1          running
...
```

---

## Step 7 — Train the ML Model

```bash
# In a second SSH session or after logs look stable
make train-kaggle
```

This trains XGBoost on `data/synthetic_fraud_dataset.csv`, logs the run to MLflow, and saves the model to the shared Docker volume.

---

## Step 8 — Access the UIs

Get your VM's external IP:
```bash
# Run this locally (not inside the VM)
gcloud compute instances describe fraud-detection-vm \
  --zone=us-central1-a \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

Then open these URLs in your browser (replace `EXTERNAL_IP`):

| Service | URL | Credentials |
|---|---|---|
| Grafana (dashboard) | `http://EXTERNAL_IP:3000` | admin / your `GRAFANA_PASSWORD` |
| Airflow | `http://EXTERNAL_IP:8082` | admin / your `AIRFLOW_ADMIN_PASSWORD` |
| MLflow | `http://EXTERNAL_IP:5001` | — |
| Kafka UI | `http://EXTERNAL_IP:8080` | — |
| Spark Master UI | `http://EXTERNAL_IP:8081` | — |

---

## Step 9 — Stop the VM When Not In Use

GCP charges while the VM is running even if idle.

```bash
# Stop (preserves disk, stops billing for compute)
gcloud compute instances stop fraud-detection-vm --zone=us-central1-a

# Start again later
gcloud compute instances start fraud-detection-vm --zone=us-central1-a

# Delete permanently (also deletes disk)
gcloud compute instances delete fraud-detection-vm --zone=us-central1-a
```

---

## Estimated Cost (Approach A)

| Resource | Spec | Cost |
|---|---|---|
| n2-standard-4 VM | 4 vCPU, 16 GB RAM | ~$0.19/hr (~$4.50/day) |
| 50 GB SSD boot disk | pd-ssd | ~$0.09/day |
| Egress (minimal) | UI browsing | ~$0.01/day |
| **Total** | | **~$4.60/day** |

With GCP's **$300 free credit** for new accounts, you can run this for ~65 days at no cost.

---

# Part 2 — Cloud-Native Managed Services

This approach replaces the Docker containers with GCP managed services. The architecture maps as follows:

```
Docker Compose          →   GCP Managed Service
─────────────────────────────────────────────────
Kafka + ZooKeeper       →   Confluent Cloud (Kafka-as-a-service on GCP)
Spark Structured Stream →   Cloud Dataproc (managed Spark cluster)
Delta Lake (local)      →   Delta Lake on Google Cloud Storage (GCS)
Airflow                 →   Cloud Composer 2
PostgreSQL              →   Cloud SQL (PostgreSQL 15)
MLflow                  →   Cloud Run (containerized MLflow server)
Grafana                 →   Grafana Cloud (free tier) or GCE micro instance
```

---

## Architecture Diagram

```
[GCS Bucket: raw-data/]
  synthetic_fraud_dataset.csv
          │
          │ Dataproc job (producer)
          ▼
[Confluent Cloud Kafka]
  Topic: raw-transactions
          │
          │ Dataproc Structured Streaming job
          ▼
[GCS Bucket: delta-lake/]
  bronze/ silver/ gold/   (Delta Lake tables)
          │
  ┌───────┴──────────────────────┐
  │                              │
  ▼                              ▼
[Cloud SQL]              [Cloud Composer]
 fraud_metrics            Daily retrain DAG
 dq_checks                Hourly DQ DAG
          │                      │
          │               [Dataproc: ML training]
          │               [Cloud Storage: model.pkl]
          ▼
      [Grafana]
      Dashboard
```

---

## Step-by-Step: Cloud-Native Deployment

### 2.1 — Enable Required APIs

```bash
gcloud services enable \
  dataproc.googleapis.com \
  composer.googleapis.com \
  sqladmin.googleapis.com \
  storage.googleapis.com \
  run.googleapis.com \
  secretmanager.googleapis.com
```

---

### 2.2 — Create GCS Buckets

```bash
PROJECT_ID=$(gcloud config get-value project)
REGION=us-central1

# Delta Lake storage
gsutil mb -l $REGION gs://${PROJECT_ID}-delta-lake

# Spark job scripts
gsutil mb -l $REGION gs://${PROJECT_ID}-spark-jobs

# ML models
gsutil mb -l $REGION gs://${PROJECT_ID}-models

# Upload source files
gsutil cp spark_jobs/fraud_streaming_job.py gs://${PROJECT_ID}-spark-jobs/
gsutil cp ml/train_model.py                 gs://${PROJECT_ID}-spark-jobs/
gsutil cp data/synthetic_fraud_dataset.csv  gs://${PROJECT_ID}-delta-lake/data/
```

---

### 2.3 — Create Cloud SQL (PostgreSQL)

```bash
gcloud sql instances create fraud-detection-db \
  --database-version=POSTGRES_15 \
  --tier=db-f1-micro \
  --region=$REGION \
  --storage-size=10GB \
  --storage-type=SSD

# Set the postgres user password
gcloud sql users set-password postgres \
  --instance=fraud-detection-db \
  --password=your_strong_password

# Create the fraud database
gcloud sql databases create fraud --instance=fraud-detection-db

# Get the connection name (you'll need this later)
gcloud sql instances describe fraud-detection-db --format='get(connectionName)'
```

Run the init SQL to create tables:
```bash
gcloud sql connect fraud-detection-db --user=postgres < scripts/init_postgres.sql
```

---

### 2.4 — Create a Dataproc Cluster (Spark)

```bash
gcloud dataproc clusters create fraud-spark-cluster \
  --region=$REGION \
  --zone=${REGION}-a \
  --master-machine-type=n2-standard-4 \
  --worker-machine-type=n2-standard-4 \
  --num-workers=2 \
  --image-version=2.1-debian11 \
  --optional-components=JUPYTER \
  --metadata='PIP_PACKAGES=delta-spark==3.0.0 xgboost==2.0.0 scikit-learn==1.4.0 mlflow==2.10.0 psycopg2-binary==2.9.9 confluent-kafka==2.3.0' \
  --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh \
  --properties=spark:spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark:spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --enable-component-gateway
```

> **Cost note**: This cluster runs 3 VMs. Stop it when not in use: `gcloud dataproc clusters stop fraud-spark-cluster --region=$REGION`

---

### 2.5 — Modify Spark Job for GCS

The streaming job needs two small changes to work with GCS instead of local paths:

```python
# In spark_jobs/fraud_streaming_job.py, update these paths:

BRONZE_PATH = "gs://YOUR_PROJECT_ID-delta-lake/bronze"
SILVER_PATH = "gs://YOUR_PROJECT_ID-delta-lake/silver"
GOLD_PATH   = "gs://YOUR_PROJECT_ID-delta-lake/gold"
CHECKPOINT  = "gs://YOUR_PROJECT_ID-delta-lake/checkpoints"

# Update Kafka bootstrap servers to your Confluent Cloud endpoint:
KAFKA_BOOTSTRAP = "pkc-xxxxx.us-central1.gcp.confluent.cloud:9092"
```

Submit the streaming job to Dataproc:
```bash
gcloud dataproc jobs submit pyspark \
  gs://${PROJECT_ID}-spark-jobs/fraud_streaming_job.py \
  --cluster=fraud-spark-cluster \
  --region=$REGION \
  --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.32.2.jar \
  --properties=spark.jars.packages=io.delta:delta-spark_2.12:3.0.0
```

---

### 2.6 — Create Cloud Composer (Airflow)

```bash
gcloud composer environments create fraud-composer \
  --location=$REGION \
  --image-version=composer-2.6.4-airflow-2.7.3 \
  --environment-size=small \
  --service-account=YOUR_SERVICE_ACCOUNT
```

> This takes ~20 minutes to provision.

Upload DAGs to the Composer GCS bucket:
```bash
# Get the Composer GCS bucket
COMPOSER_BUCKET=$(gcloud composer environments describe fraud-composer \
  --location=$REGION --format='get(config.dagGcsPrefix)')

# Upload DAGs (no code changes needed!)
gsutil cp dags/fraud_detection_daily_dag.py     ${COMPOSER_BUCKET}/
gsutil cp dags/data_quality_monitoring_dag.py   ${COMPOSER_BUCKET}/
```

Set Airflow variables via the Cloud Composer UI or CLI:
```bash
gcloud composer environments run fraud-composer \
  --location=$REGION variables set -- \
  POSTGRES_CONN "postgresql+psycopg2://postgres:PASSWORD@/fraud?host=/cloudsql/CONNECTION_NAME"
```

---

### 2.7 — Train the ML Model on Dataproc

```bash
gcloud dataproc jobs submit pyspark \
  gs://${PROJECT_ID}-spark-jobs/train_model.py \
  --cluster=fraud-spark-cluster \
  --region=$REGION \
  -- \
  --data-path=gs://${PROJECT_ID}-delta-lake/data/synthetic_fraud_dataset.csv \
  --model-output=gs://${PROJECT_ID}-models/fraud_model.pkl
```

---

## Cloud-Native Cost Estimate

| Service | Spec | Cost/day |
|---|---|---|
| Dataproc cluster (3 VMs) | n2-standard-4 × 3 | ~$4.50 (stop when idle) |
| Cloud Composer | small | ~$3.00 |
| Cloud SQL | db-f1-micro | ~$0.20 |
| GCS storage | ~10 GB | ~$0.07 |
| Confluent Cloud Kafka | Basic cluster | ~$1.50 |
| **Total (running)** | | **~$9.30/day** |

With GCP free credits ($300), this runs for ~32 days.

---

## Comparison Summary

| | Approach A (VM + Docker) | Approach B (Managed) |
|---|---|---|
| Setup time | 30 minutes | 2–3 days |
| Code changes | None | ~10–15 lines |
| Daily cost | ~$4.60 | ~$9.30 |
| Reliability | Medium (single VM) | High (managed SLAs) |
| Scalability | Manual | Automatic |
| Best for | Learning, portfolio demo | Production patterns |

**Recommendation**: Use Approach A to get the system running today. Use Approach B as a stretch goal to demonstrate cloud-native architecture knowledge on your portfolio.

---

## Troubleshooting

**VM runs out of memory**
```bash
# Upgrade to a larger machine type (stop VM first)
gcloud compute instances stop fraud-detection-vm --zone=us-central1-a
gcloud compute instances set-machine-type fraud-detection-vm \
  --zone=us-central1-a --machine-type=n2-standard-8
gcloud compute instances start fraud-detection-vm --zone=us-central1-a
```

**Can't connect to UI ports**
```bash
# Verify firewall rule exists
gcloud compute firewall-rules list --filter="name=fraud-detection-ports"

# Check your VM's external IP hasn't changed (it changes on stop/start)
gcloud compute instances describe fraud-detection-vm \
  --zone=us-central1-a \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

**Use a static IP to avoid it changing on restart**
```bash
gcloud compute addresses create fraud-detection-ip --region=us-central1
gcloud compute instances delete-access-config fraud-detection-vm \
  --zone=us-central1-a --access-config-name="External NAT"
gcloud compute instances add-access-config fraud-detection-vm \
  --zone=us-central1-a \
  --address=$(gcloud compute addresses describe fraud-detection-ip \
              --region=us-central1 --format='get(address)')
```

**Docker permission denied inside VM**
```bash
sudo usermod -aG docker $USER
newgrp docker
```
