# Spark Streaming Job — Real-Time Fraud Detection

## What it does

`fraud_streaming_job.py` is the core streaming pipeline. It runs continuously and processes every incoming transaction in near real-time:

1. **Ingest** — reads raw JSON transactions from the Kafka topic `raw-transactions`
2. **Bronze** — writes immutable raw records (+ `_ingested_at`, `_record_id`) to Delta Lake
3. **Feature engineering** — computes 10 ML features and 5 business-rule flags
4. **Rule scoring** — combines flags into a weighted `rule_based_score` (0–1)
5. **ML scoring** — runs the XGBoost model via `pandas_udf` → `ml_score` (0.0 if no model)
6. **Combine** — `fraud_score = max(rule_based_score, ml_score)`
7. **Silver** — writes all enriched records to Delta Lake
8. **Gold** — writes only flagged transactions (`fraud_score >= threshold`) to Delta Lake
9. **Alert** — publishes flagged transactions as JSON to Kafka topic `flagged-transactions`
10. **PostgreSQL** — inserts flagged transactions into the `fraud_metrics` table

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `BRONZE_PATH` | `/data/delta/bronze` | Delta Lake Bronze layer path |
| `SILVER_PATH` | `/data/delta/silver` | Delta Lake Silver layer path |
| `GOLD_PATH` | `/data/delta/gold` | Delta Lake Gold layer path |
| `FRAUD_THRESHOLD` | `0.35` | Minimum `fraud_score` to flag a transaction |
| `MODEL_PATH` | `/data/models/fraud_model.pkl` | Path to the trained XGBoost model pickle |
| `POSTGRES_HOST` | `postgres` | PostgreSQL host |
| `POSTGRES_USER` | `postgres` | PostgreSQL user |
| `POSTGRES_PASSWORD` | _(empty)_ | PostgreSQL password |
| `POSTGRES_DB` | `fraud_db` | PostgreSQL database name |

All variables are optional — the defaults work inside the Docker Compose stack.

---

## How to run

### Inside Docker Compose (recommended)

```bash
docker compose up -d
```

The `spark-streaming` service starts the job automatically.

### Manual spark-submit

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-spark_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark_jobs/fraud_streaming_job.py
```

Override any env var at submit time:

```bash
FRAUD_THRESHOLD=0.40 MODEL_PATH=/data/models/fraud_model.pkl spark-submit ...
```

---

## Common troubleshooting

### Job exits immediately with Kafka connection error
- Confirm Kafka is healthy: `docker compose ps kafka`
- Check `KAFKA_BOOTSTRAP_SERVERS` points to the right host and port
- Verify the topic exists: `kafka-topics.sh --list --bootstrap-server kafka:9092`

### `ml_score` is always 0.0
- The model file is missing at `MODEL_PATH`
- Train the model first: `python ml/train_model.py --source csv --dataset-path data/synthetic_fraud_dataset.csv`
- Confirm the file exists: `ls /data/models/fraud_model.pkl`

### Delta Lake write fails with `_delta_log` errors
- Checkpoint directories may be stale from a previous run
- Remove them: `rm -rf /data/delta/bronze_checkpoint /data/delta/silver_checkpoint /data/delta/gold_checkpoint /tmp/alert_checkpoint /tmp/postgres_checkpoint`

### PostgreSQL write errors in logs
- Confirm `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` are set correctly
- Check the `fraud_metrics` table exists: `psql -U $POSTGRES_USER -d fraud_db -c "\dt"`
- Run the init script if needed: `psql -U $POSTGRES_USER -f scripts/init_postgres.sql`

### High latency / micro-batches taking too long
- The `pandas_udf` loads the model once per executor partition via `_MODEL_CACHE`; if you see repeated load messages, the executor is being restarted — check Spark executor logs
- Reduce parallelism or increase executor memory in the `docker-compose.yml` Spark environment section
