# Team Member Task Summary Cards

Quick reference for each team member's responsibilities and deliverables.

---

## 🎯 Khurshid Normurodov - Previous Project Lead / Data Architect

**Title**: Initial Data Architect / Repository Contributor 
**Responsibility**: Initial architecture support, repository setup, early project planning

### Core Tasks

| Week | Task Group | Deliverable                                                   | Status     |
| ---- | ---------- | ------------------------------------------------------------- | ---------- |
| 1    | 1.1        | Initial GitHub repository setup and project structure         | ✅ Complete |
| 1    | 1.1        | Basic repository documentation and team access setup          | ✅ Complete |


### Key Responsibilities

* ✅ Created the initial GitHub repository structure
* ✅ Set up basic project folders and documentation files
* ✅ Added team members to the repository
* ✅ Contributed to the initial system architecture discussion
* ✅ Helped prepare the initial project structure for team collaboration


### Collaboration Points
- **Elif**:  Environment planning and documentation alignment
- **Farzaneh**: Docker setup & Dataset validation & testing
- **Hontar**: Architecture review & integration
- **All**: Weekly sync meetings (Mon & Thu)

### Success Metrics
- All team members productive on first day
- No blocking architectural issues
- Daily standup cadence maintained
---

## 📨 Farzaneh Barzegar - Project Lead / Data Ingestion Engineer

**Title**: Project Lead / Data Ingestion Engineer
**Responsibility**: Team coordination, GCP VM ingestion setup, Kafka/Zookeeper setup, chunk-based CSV-to-Kafka producer, raw transaction pipeline, and ingestion documentation

### Core Tasks

| Week | Task Group | Deliverable                                                              | Status     |
| ---- | ---------- | ------------------------------------------------------------------------ | ---------- |
| 1    | 1.4        | Dataset upload, validation, and ingestion preparation                    | ✅ Complete |
| 1    | 1.5        | GCP VM setup for Kafka-based ingestion                                   | ✅ Complete |
| 2    | 2.1        | Kafka and Zookeeper setup for raw transaction streaming                  | ✅ Complete |
| 2    | 2.1        | Kafka topic `raw-transactions` creation and validation                   | ✅ Complete |
| 2    | 2.1        | CSV-to-Kafka producer implementation                                     | ✅ Complete |
| 3    | 2.1        | Chunk-based / micro-batch ingestion update after professor feedback      | ✅ Complete |
| 3    | 2.1        | Streamed 50,000 transaction records into `raw-transactions`              | ✅ Complete |
| 3-6  | Support    | Handoff documentation, GitHub updates, and pipeline coordination support | ✅ Complete |

### Key Responsibilities

* ✅ Set up the GCP VM environment for the ingestion layer

* ✅ Configure Kafka and Zookeeper for transaction streaming

* ✅ Create and validate Kafka topic `raw-transactions`

* ✅ Create `csv_to_kafka.py` as the updated Kafka producer:

  * Read `synthetic_fraud_dataset.csv`
  * Process the CSV file in chunks instead of loading the full dataset at once
  * Convert each transaction row into JSON
  * Send transaction records to Kafka topic `raw-transactions`
  * Support command-line arguments for topic, max rows, chunk size, and delay between chunks

* ✅ Improve the ingestion design after professor feedback:

  * Replaced full CSV loading with chunk-based / micro-batch ingestion
  * Reduced memory usage
  * Made the ingestion flow closer to a real-world ETL / streaming pipeline

* ✅ Stream 50,000 records into Kafka and verify message offsets

* ✅ Add producer dependencies in `producer/requirements.txt`

* ✅ Add logging for chunk processing and total sent records

* ✅ Validate the data flow from CSV to Kafka

* ✅ Prepare ingestion handoff notes for the downstream Spark / ML pipeline

* ✅ Update `producer/README.md`, main `README.md`, and related documentation to reflect the latest ingestion workflow

* ✅ Support final pipeline coordination between ingestion, Spark processing, PostgreSQL outputs, and dashboard validation

The topic was used to stream and verify 50,000 transaction records.

### Collaboration Points

* **Daniil**: Kafka input schema, `raw-transactions` topic, Spark ingestion handoff, and pipeline validation
* **Elif/Sila**: Support for final validation after Kafka/Spark/PostgreSQL pipeline fixes
* **All**: GitHub documentation updates, final integration checks, and project coordination

### Definition of Done

* GCP VM prepared for ingestion
* Kafka and Zookeeper running successfully
* Kafka topic `raw-transactions` created and validated
* CSV producer reads data in chunks instead of loading the full file at once
* 50,000 records streamed successfully into Kafka
* Kafka offsets verified after ingestion
* Producer dependencies and documentation updated
* Handoff notes shared with downstream Spark / ML team
* Ingestion workflow aligned with professor feedback

### Success Metrics

* 50,000 transaction records streamed into Kafka successfully
* Kafka topic `raw-transactions` verified with correct offsets
* Producer sends valid JSON messages with the expected schema
* Chunk-based ingestion works without loading the full dataset into memory
* Ingestion pipeline is documented and ready for downstream Spark processing
* Final ingestion design reflects professor feedback
---

## 🔄 Daniil Hontar - Data Processing & ML Engineer

**Title**: Data Processing & ML Engineer / Development Team
**Responsibility**: Spark Streaming pipeline, Bronze/Silver/Gold data layers, feature engineering, fraud scoring, XGBoost model training, and PostgreSQL/Kafka outputs

### Core Tasks

| Week | Task Group | Deliverable                                                        | Status     |
| ---- | ---------- | ------------------------------------------------------------------ | ---------- |
| 2    | 2.2        | Bronze Layer for raw Kafka transactions                            | ✅ Complete |
| 2    | 2.3        | Silver Layer with feature engineering and rule-based scoring       | ✅ Complete |
| 2    | 2.4        | ML score integration and fraud scoring logic                       | ✅ Complete |
| 2-3  | 2.5        | Gold Layer for flagged transactions and output writing             | ✅ Complete |
| 3    | 3.1        | XGBoost model training                                             | ✅ Complete |
| 3    | 3.2        | MLflow tracking support                                            | ✅ Complete |
| 3    | 3.3        | Model evaluation workflow                                          | ✅ Complete |
| 4-6  | Support    | Pipeline debugging, PostgreSQL validation, and integration support | ✅ Complete |

### Key Responsibilities

#### Spark Streaming Job (`fraud_streaming_job.py`)

**Bronze Layer**

* Read raw transaction events from Kafka topic `raw-transactions`
* Apply schema validation and type handling
* Store raw transaction records in the Bronze Delta Lake layer
* Preserve the original input data for traceability

**Silver Layer**

* Clean and transform transaction data
* Create fraud-related features such as:

  * `log_amount`
  * `event_hour`
  * `amount_bucket`
  * `merchant_risk_score`
  * `flag_high_amount`
  * `flag_velocity`
  * `flag_off_hours`
  * `flag_geo_anomaly`
  * `flag_risky_merchant`
  * `is_online`
* Calculate rule-based fraud scores using fraud signal flags
* Prepare processed data for ML scoring and downstream outputs

**ML Scoring**

* Integrate XGBoost-based fraud scoring
* Combine rule-based score and ML score into a final `fraud_score`
* Use the final score to decide whether a transaction should be flagged

**Gold Layer**

* Store only flagged transactions where `fraud_score >= 0.35`
* Write flagged transactions to:

  * Delta Lake Gold layer: `/data/delta/gold`
  * Kafka topic: `flagged-transactions`
  * PostgreSQL table: `fraud_metrics`

#### ML Training (`train_model.py`)

* Train an XGBoost fraud detection model
* Use selected fraud-related features from the processed data
* Handle class imbalance during model training
* Save the trained model for use in the pipeline

#### Model Evaluation (`evaluate_model.py`)

* Evaluate the trained model using classification metrics
* Compare model performance across runs
* Support model validation before using it in the pipeline

### Data Layers Owned

```text
Kafka topic: raw-transactions
  ↓
Bronze Layer
  Raw transaction records
  ↓
Silver Layer
  Cleaned data + engineered features + fraud scores
  ↓
Gold Layer
  Flagged transactions only
  ↓
Outputs
  Kafka flagged-transactions + PostgreSQL fraud_metrics + Delta Gold
```

### Collaboration Points

* **Farzaneh**: Consumes transaction data from Kafka topic `raw-transactions`, validates input schema, and coordinates ingestion handoff
* **Elif/Sila**: Provides PostgreSQL `fraud_metrics` output for Grafana dashboard validation and Airflow monitoring
* **All**: Supports final integration testing and pipeline validation

### Definition of Done

* Spark job reads transaction records from `raw-transactions`
* Bronze, Silver, and Gold layers are created correctly
* Fraud-related features and rule-based scores are calculated
* Final `fraud_score` is generated for processed transactions
* Flagged transactions are written to the Gold layer
* Flagged transactions are written to PostgreSQL table `fraud_metrics`
* Kafka output topic `flagged-transactions` is available for flagged events
* PostgreSQL output is validated for dashboard usage

### Success Metrics

* Spark pipeline successfully consumes records from `raw-transactions`
* Fraud-related features are generated in the Silver layer
* Flagged transactions are correctly separated in the Gold layer
* PostgreSQL `fraud_metrics` stores real flagged transaction outputs
* Grafana can use `fraud_metrics` as the source for fraud alert dashboards
* Pipeline behavior is documented and understandable for downstream team members


## 📊 Elif Sila Okutucu - Analytics & DevOps Engineer / Scrum Master

**Title**: Analytics & DevOps Engineer / Scrum Master
**Responsibility**: PostgreSQL validation, Airflow DAG validation, Grafana dashboard development, monitoring documentation, and team progress tracking

### Core Tasks

| Week | Task Group | Deliverable                                        | Status     |
| ---- | ---------- | -------------------------------------------------- | ---------- |
| 1    | 1.3        | PostgreSQL initialization and table validation     | ✅ Complete |
| 4    | 4.1        | Airflow setup and DAG validation                   | ✅ Complete |
| 4    | 4.2        | Daily retraining DAG validation                    | ✅ Complete |
| 4    | 4.3        | Hourly data quality monitoring DAG validation      | ✅ Complete |
| 4    | 4.4        | PostgreSQL integration and query validation        | ✅ Complete |
| 5    | 5.1        | Grafana fraud alerts monitoring dashboard          | ✅ Complete |
| 5    | 5.1        | Dashboard validation using `fraud_metrics` outputs | ✅ Complete |
| 6    | 6.3        | Monitoring documentation and evidence collection   | ✅ Complete |

### Key Responsibilities

#### PostgreSQL Validation

* Validate PostgreSQL tables used by the pipeline:

  * `fraud_metrics`
  * `dq_checks`
  * `model_registry`
* Check table schemas and query outputs
* Verify that `fraud_metrics` contains real processed pipeline outputs after the Kafka/Spark pipeline is running
* Distinguish between sample rows and real processed records
* Confirm that `fraud_metrics` is designed for flagged transactions only

#### Airflow DAG Validation

* Check that Airflow DAGs are available and runnable
* Validate the daily retraining workflow
* Validate the hourly data quality monitoring workflow
* Review DAG execution status and basic task behavior
* Support documentation of Airflow workflows and expected outputs

#### Grafana Dashboard Development

* Connect Grafana to PostgreSQL
* Build the fraud alerts monitoring dashboard using `fraud_metrics`
* Create dashboard panels for:

  * Total flagged transactions
  * Average fraud score
  * Average flagged transaction amount
  * Fraud alerts over time
  * Recent flagged transaction records
  * Data quality check results, where available
* Validate dashboard queries against PostgreSQL outputs
* Ensure the dashboard reflects actual pipeline data rather than only sample records

#### Monitoring Documentation

* Document dashboard usage and data sources
* Collect evidence/screenshots for the final project submission
* Explain how Grafana reads from PostgreSQL and visualizes fraud alerts
* Support final validation after Kafka, Spark, and PostgreSQL outputs are confirmed

#### Scrum Master / Coordination Support

* Track team progress and blockers
* Follow up on task status and sprint updates
* Support team communication around integration issues
* Help identify blockers in the final pipeline validation stage

### Services and Outputs Supported

```text
PostgreSQL
  ├── fraud_metrics  → used for fraud alerts dashboard
  ├── dq_checks      → used for data quality monitoring
  └── model_registry → used for model tracking information

Airflow
  ├── fraud_detection_daily_dag
  └── data_quality_monitoring_dag

Grafana
  └── Fraud alerts monitoring dashboard
```

### Collaboration Points

* **Farzaneh**: Validates dashboard results after ingestion and Kafka pipeline updates
* **Daniil**: Uses Spark/PostgreSQL outputs, especially `fraud_metrics`, for dashboard validation
* **All**: Supports final integration checks, documentation, and evidence collection

### Definition of Done

* PostgreSQL tables are accessible and validated
* `fraud_metrics` output is checked before dashboard finalization
* Grafana is connected to PostgreSQL successfully
* Fraud alerts dashboard uses real processed records from `fraud_metrics`
* Airflow DAGs are visible and validation status is documented
* Dashboard queries are checked against PostgreSQL results
* Monitoring documentation and evidence are prepared for final submission
* Scrum/task tracking is updated during final project coordination

### Success Metrics

* Grafana connects successfully to PostgreSQL
* Dashboard panels query the correct PostgreSQL tables
* `fraud_metrics` is validated as the source for flagged transaction alerts
* Sample rows and real processed rows are clearly distinguished
* Airflow DAG validation is documented
* Dashboard is ready for final demonstration using actual pipeline outputs
* Monitoring documentation is clear and aligned with the final system behavior
  

## 🔄 Critical Path Dependencies

```text
1. Initial Repository and Project Structure Setup
   └─→ GitHub repository, folders, and basic documentation prepared
       ↓
2. Ingestion Environment Setup (Farzaneh)
   ├─→ GCP VM prepared for ingestion
   ├─→ Kafka and Zookeeper configured
   └─→ Kafka topic `raw-transactions` created
       ↓
3. Chunk-Based Data Ingestion (Farzaneh)
   ├─→ `csv_to_kafka.py` reads the CSV dataset in chunks
   ├─→ Transaction records are converted to JSON
   └─→ 50,000 records streamed into `raw-transactions`
       ↓
4. Spark Streaming and Fraud Processing (Daniil)
   ├─→ Read from Kafka topic `raw-transactions`
   ├─→ Create Bronze, Silver, and Gold layers
   ├─→ Generate fraud-related features and scores
   └─→ Write flagged transactions to PostgreSQL `fraud_metrics`
       ↓
5. Airflow and Monitoring Validation (Elif/Sila)
   ├─→ Validate Airflow DAGs
   ├─→ Check PostgreSQL outputs
   └─→ Validate data quality monitoring flow
       ↓
6. Grafana Dashboard Validation (Elif/Sila)
   ├─→ Connect Grafana to PostgreSQL
   ├─→ Build fraud alerts dashboard using `fraud_metrics`
   └─→ Validate dashboard using real processed records
       ↓
7. Final Integration, Documentation, and Presentation (All)
   ├─→ Component README updates
   ├─→ System documentation updates
   ├─→ Final validation evidence
   └─→ Presentation preparation
```
---

## ✅ Weekly Checklist for Each Person

### Khurshid's Checklist
- Week 1: GitHub repo created 

### Farzaneh's Checklist
- Week 1: docker-compose + Dataset validation + data quality checks
- Week 2: Producer implementation + logging + error handling
- Week 3: Producer enhancements + optional fraud injection
- Week 4-6: Support + testing + documentation

### Hontar's Checklist
- Week 2: Bronze + Silver layers implemented
- Week 3: Gold layer + ML training + MLflow
- Week 4: ML integration + Airflow support
- Week 5: Unit tests + load tests + performance tuning
- Week 6: Load testing completion + documentation

### Elif's Checklist
- Week 1: PostgreSQL
- Week 4: Airflow setup + daily DAG + DQ DAG
- Week 5: Grafana + dashboards + alerts
- Week 6: Monitoring + documentation + support
---
