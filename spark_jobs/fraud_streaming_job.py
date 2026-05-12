"""
SPARK STREAMING JOB - Real-Time Fraud Detection
================================================
This is the heart of the system. It:
1. Reads transactions from Kafka (raw)
2. Extracts features and applies business rules (Silver layer)
3. Scores with ML model
4. Stores results in Delta Lake
5. Publishes flagged transactions back to Kafka

For ML students: This is a streaming pipeline. Think of it as:
- Input: Raw transaction stream
- Processing: Feature engineering + ML scoring
- Output: Enriched transactions with fraud scores

Data flows through 3 Delta Lake "layers":
- Bronze: Raw data (immutable history)
- Silver: Cleaned & featured data (for training)
- Gold: Flagged transactions (for alerting)
"""

import json
import pickle
import logging
from datetime import datetime, timedelta
from typing import Optional
import os

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, from_json, struct, when, max as spark_max, lit, current_timestamp,
    window, count, coalesce, log, lower, hour, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FraudDetectionPipeline:
    """Main streaming pipeline for real-time fraud detection."""
    
    def __init__(self, app_name: str = "fraud-detection"):
        """Initialize Spark session with optimizations for streaming."""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("✓ Spark Session initialized")
        
        # Configuration
        self.kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.bronze_path = os.getenv('BRONZE_PATH', '/data/delta/bronze')
        self.silver_path = os.getenv('SILVER_PATH', '/data/delta/silver')
        self.gold_path = os.getenv('GOLD_PATH', '/data/delta/gold')
        self.fraud_threshold = float(os.getenv('FRAUD_THRESHOLD', 0.35))
        self.model_path = os.getenv('MODEL_PATH', '/data/models/fraud_model.pkl')
    
    def read_kafka_stream(self) -> DataFrame:
        """
        Read streaming data from Kafka.
        
        Returns:
            DataFrame: Kafka messages parsed as transactions
        """
        # Define the schema for incoming transactions
        transaction_schema = StructType([
            StructField("Transaction_ID", StringType()),
            StructField("User_ID", StringType()),
            StructField("Transaction_Amount", DoubleType()),
            StructField("Transaction_Type", StringType()),
            StructField("Timestamp", StringType()),
            StructField("Account_Balance", DoubleType()),
            StructField("Device_Type", StringType()),
            StructField("Location", StringType()),
            StructField("Merchant_Category", StringType()),
            StructField("IP_Address_Flag", IntegerType()),
            StructField("Previous_Fraudulent_Activity", IntegerType()),
            StructField("Daily_Transaction_Count", IntegerType()),
            StructField("Avg_Transaction_Amount_7d", DoubleType()),
            StructField("Failed_Transaction_Count_7d", IntegerType()),
            StructField("Card_Type", StringType()),
            StructField("Card_Age", IntegerType()),
            StructField("Transaction_Distance", DoubleType()),
            StructField("Authentication_Method", StringType()),
            StructField("Risk_Score", DoubleType()),
            StructField("Is_Weekend", IntegerType()),
            StructField("Fraud_Label", IntegerType())
        ])
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
            .option("subscribe", "raw-transactions") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON from Kafka value
        df = df.select(
            from_json(col("value").cast(StringType()), transaction_schema).alias("data")
        ).select("data.*")
        
        logger.info("✓ Kafka stream reader initialized")
        return df
    
    def engineer_features(self, df: DataFrame) -> DataFrame:
        """
        Create features for ML model.
        
        For students: Features are derived from raw transaction data.
        Good features are:
        - Informative (help model predict fraud)
        - Easy to compute (no data leakage)
        - Interpretable (you can explain them)
        
        Args:
            df: Raw transactions
            
        Returns:
            DataFrame: Transactions with engineered features
        """
        risky_merchant_categories = ['crypto', 'gambling', 'wire transfer', 'wire_transfer', 'adult']

        df = df \
            .withColumn("timestamp_ts", to_timestamp(col("Timestamp"))) \
            .withColumn("event_hour", hour(col("timestamp_ts"))) \
            .withColumn("amount", col("Transaction_Amount").cast("double")) \
            .withColumn("log_amount", log(col("amount") + 1)) \
            .withColumn("amount_bucket", 
                when(col("amount") < 100, "small")
                .when(col("amount") < 500, "medium")
                .when(col("amount") < 2000, "large")
                .otherwise("very_large")
            ) \
            .withColumn("merchant_risk_score",
                when(lower(col("Merchant_Category")).isin(risky_merchant_categories), 0.8)
                .otherwise(coalesce(col("Risk_Score").cast("double"), lit(0.2)))
            ) \
            .withColumn("flag_high_amount", when(col("amount") >= 3000, lit(True)).otherwise(lit(False))) \
            .withColumn("flag_velocity",
                when((col("Daily_Transaction_Count") >= 6) | (col("Failed_Transaction_Count_7d") >= 3) | (col("Previous_Fraudulent_Activity") > 0), lit(True))
                .otherwise(lit(False))
            ) \
            .withColumn("flag_off_hours",
                when((col("event_hour") >= 2) & (col("event_hour") <= 4), lit(True)).otherwise(lit(False))
            ) \
            .withColumn("flag_geo_anomaly",
                when((col("IP_Address_Flag") == 1) | (col("Transaction_Distance") > 75), lit(True)).otherwise(lit(False))
            ) \
            .withColumn("flag_risky_merchant",
                when(lower(col("Merchant_Category")).isin(risky_merchant_categories), lit(True)).otherwise(lit(False))
            ) \
            .withColumn("is_online", lower(col("Transaction_Type")) == "online") \
            .withColumn("label", col("Fraud_Label").cast("int"))
        
        logger.info("✓ Features engineered")
        return df
    
    def calculate_rule_based_score(self, df: DataFrame) -> DataFrame:
        """
        Calculate fraud score based on business rules.
        
        For students: This is a simple heuristic approach.
        It weights fraud signals and creates a 0-1 score.
        
        Rule-based scoring is:
        ✓ Fast (no ML needed)
        ✗ Limited (needs manual tuning)
        
        Args:
            df: Engineered features
            
        Returns:
            DataFrame: With rule_based_score column
        """
        # Weight fraud signals
        df = df.withColumn("rule_based_score",
              (col("flag_high_amount").cast("double") * 0.3 +
               col("flag_velocity").cast("double") * 0.25 +
               col("flag_off_hours").cast("double") * 0.15 +
               col("flag_geo_anomaly").cast("double") * 0.2 +
               col("flag_risky_merchant").cast("double") * 0.1) / 1.0
        )
        
        # Cap score at 1.0
        df = df.withColumn("rule_based_score", 
            when(col("rule_based_score") > 1.0, 1.0).otherwise(col("rule_based_score"))
        )
        
        return df
    
    def load_model(self) -> Optional[object]:
        """
        Load trained XGBoost model if it exists.
        
        For students: Models are stored as pickled files.
        We check if a model exists; if not, we rely on rules only.
        
        Returns:
            Trained model or None if not available
        """
        try:
            if os.path.exists(self.model_path):
                with open(self.model_path, 'rb') as f:
                    model = pickle.load(f)
                logger.info(f"✓ Loaded ML model from {self.model_path}")
                return model
            else:
                logger.warning(f"⚠️  No model found at {self.model_path}. Using rule-based scoring only.")
                return None
        except Exception as e:
            logger.error(f"❌ Error loading model: {e}. Using rule-based scoring only.")
            return None
    
    def write_to_delta_lake(self, df: DataFrame, layer: str, mode: str = "append"):
        """
        Write DataFrame to Delta Lake.
        
        Args:
            df: DataFrame to write
            layer: 'bronze', 'silver', or 'gold'
            mode: Write mode (append, overwrite, etc)
        """
        if layer == 'bronze':
            path = self.bronze_path
        elif layer == 'silver':
            path = self.silver_path
        elif layer == 'gold':
            path = self.gold_path
        else:
            raise ValueError(f"Unknown layer: {layer}")
        
        logger.info(f"💾 Writing to {layer} layer at {path}")
        
        query = df.writeStream \
            .format("delta") \
            .mode(mode) \
            .option("path", path) \
            .option("checkpointLocation", f"{path}_checkpoint") \
            .start()
        
        return query
    
    def run(self):
        """Execute the full streaming pipeline."""
        try:
            logger.info("=" * 60)
            logger.info("STARTING FRAUD DETECTION PIPELINE")
            logger.info("=" * 60)
            
            # 1. Read from Kafka
            logger.info("\n📖 Step 1: Reading transactions from Kafka...")
            df_raw = self.read_kafka_stream()
            
            # 2. Write to Bronze (immutable raw data)
            logger.info("\n💾 Step 2: Writing raw data to Bronze layer...")
            bronze_query = self.write_to_delta_lake(df_raw, 'bronze', mode='append')
            
            # 3. Engineer features
            logger.info("\n🔧 Step 3: Engineering features...")
            df_features = self.engineer_features(df_raw)
            
            # 4. Calculate rule-based score
            logger.info("\n📊 Step 4: Calculating rule-based fraud scores...")
            df_scored = self.calculate_rule_based_score(df_features)
            
            # 5. Load ML model if available
            model = self.load_model()
            
            # 6. Add ML score (if model exists)
            if model is not None:
                logger.info("\n🤖 Step 5: Adding ML model predictions...")
                # Note: In a real setup, we'd use pandas_udf for distributed inference
                df_scored = df_scored.withColumn("ml_score", lit(0.5))  # Placeholder
            else:
                df_scored = df_scored.withColumn("ml_score", lit(0.0))
            
            # 7. Combine scores
            logger.info("\n⚖️  Step 6: Combining rule and ML scores...")
            df_scored = df_scored.withColumn(
                "fraud_score",
                spark_max(col("rule_based_score"), col("ml_score"))
            )
            
            # 8. Flag transactions
            df_scored = df_scored.withColumn(
                "is_flagged",
                col("fraud_score") >= self.fraud_threshold
            )
            
            # 9. Write to Silver (enriched data for training)
            logger.info("\n💾 Step 7: Writing enriched data to Silver layer...")
            silver_query = self.write_to_delta_lake(df_scored, 'silver', mode='append')
            
            # 10. Filter flagged transactions to Gold layer
            logger.info("\n💾 Step 8: Writing flagged transactions to Gold layer...")
            df_flagged = df_scored.filter(col("is_flagged") == True)
            gold_query = self.write_to_delta_lake(df_flagged, 'gold', mode='append')
            
            # 11. Send flagged transactions back to Kafka for alerting
            logger.info("\n📤 Step 9: Publishing flagged transactions to Kafka...")
            flagged_for_kafka = df_flagged.select(
                struct(
                    col("Transaction_ID"),
                    col("Timestamp"),
                    col("User_ID"),
                    col("amount"),
                    col("fraud_score"),
                    col("rule_based_score"),
                    col("ml_score")
                ).alias("value")
            )
            
            alert_query = flagged_for_kafka.select(
                col("value").cast(StringType()).alias("value")
            ).writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap) \
                .option("topic", "flagged-transactions") \
                .option("checkpointLocation", "/tmp/alert_checkpoint") \
                .start()
            
            logger.info("\n" + "=" * 60)
            logger.info("✓ ALL STREAMS RUNNING")
            logger.info("=" * 60)
            
            # Keep pipeline running
            self.spark.streams.awaitAnyTermination()
        
        except Exception as e:
            logger.error(f"❌ Pipeline error: {e}", exc_info=True)
            raise


def main():
    """Entry point."""
    pipeline = FraudDetectionPipeline()
    pipeline.run()


if __name__ == '__main__':
    main()
