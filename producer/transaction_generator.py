"""
TRANSACTION GENERATOR - Kafka Producer
========================================
Generates synthetic financial transactions with 5 fraud patterns.
This mimics real-world transaction data for the ML pipeline.

For ML students: Think of this as a data simulator that creates:
1. Normal transactions (99%)
2. Fraudulent transactions (1% + intentional patterns)

Fraud patterns simulated:
- High amount (>$3000)
- Velocity attack (many txns in 2 min)
- Off-hours (02:00-04:00 UTC)
- Geo-anomaly (far from home)
- Risky merchant (crypto, gambling, wires)
"""

import csv
import itertools
import json
import random
from datetime import datetime, timedelta
from typing import Dict, Any
from kafka import KafkaProducer
import logging
import time
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TransactionGenerator:
    """Generates realistic financial transaction data."""

    CSV_NUMERIC_FIELDS = {
        'Transaction_Amount',
        'Account_Balance',
        'Previous_Fraudulent_Activity',
        'Daily_Transaction_Count',
        'Avg_Transaction_Amount_7d',
        'Failed_Transaction_Count_7d',
        'Card_Age',
        'Transaction_Distance',
        'Risk_Score',
    }

    CSV_INTEGER_FIELDS = {
        'IP_Address_Flag',
        'Is_Weekend',
        'Fraud_Label',
    }

    REQUIRED_CSV_COLUMNS = {
        'Transaction_ID', 'User_ID', 'Transaction_Amount', 'Transaction_Type',
        'Timestamp', 'Account_Balance', 'Device_Type', 'Location',
        'Merchant_Category', 'IP_Address_Flag', 'Previous_Fraudulent_Activity',
        'Daily_Transaction_Count', 'Avg_Transaction_Amount_7d',
        'Failed_Transaction_Count_7d', 'Card_Type', 'Card_Age',
        'Transaction_Distance', 'Authentication_Method', 'Risk_Score',
        'Is_Weekend', 'Fraud_Label'
    }
    
    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        """Initialize Kafka producer."""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        self.dataset_path = os.getenv('TRANSACTION_DATASET_PATH')
        self.replay_exact_dataset = os.getenv('TRANSACTION_DATASET_LOOP', 'true').lower() == 'true'
        self._dataset_rows = self._load_dataset_rows(self.dataset_path) if self.dataset_path else []
        if self.dataset_path and not self._dataset_rows:
            raise FileNotFoundError(f"Dataset not found or unreadable: {self.dataset_path}")
        if self._dataset_rows:
            self._dataset_iter = itertools.cycle(self._dataset_rows) if self.replay_exact_dataset else iter(self._dataset_rows)
        else:
            self._dataset_iter = None

        logger.info(f"✓ Connected to Kafka at {bootstrap_servers}")
        if self._dataset_rows:
            logger.info(f"✓ Loaded exact Kaggle dataset from {self.dataset_path} ({len(self._dataset_rows)} rows)")
        else:
            logger.info("✓ No dataset path configured; using synthetic generator for local demos.")

    def _load_dataset_rows(self, dataset_path: str):
        """Load the exact Kaggle dataset if it exists."""
        if not dataset_path or not os.path.exists(dataset_path):
            return []

        rows = []
        with open(dataset_path, newline='', encoding='utf-8') as file_handle:
            reader = csv.DictReader(file_handle)
            headers = set(reader.fieldnames or [])
            missing = self.REQUIRED_CSV_COLUMNS - headers
            if missing:
                raise ValueError(f"Dataset is missing required columns: {sorted(missing)}")

            for row in reader:
                normalized = {}
                for key, value in row.items():
                    if key in self.CSV_NUMERIC_FIELDS:
                        normalized[key] = float(value) if value not in (None, '') else 0.0
                    elif key in self.CSV_INTEGER_FIELDS:
                        normalized[key] = int(float(value)) if value not in (None, '') else 0
                    else:
                        normalized[key] = value
                rows.append(normalized)

        return rows

    def _generate_synthetic_transaction(self) -> Dict[str, Any]:
        """Generate the original synthetic event shape used for local demos."""
        timestamp = datetime.utcnow()
        card_id = f"CARD_{random.randint(1000, 9999)}"
        merchant_id = f"MERCH_{random.randint(1, 500)}"

        base_amount = random.lognormvariate(3.5, 1.2)
        amount = min(base_amount, 10000)

        flags = {
            'flag_high_amount': False,
            'flag_velocity': False,
            'flag_off_hours': False,
            'flag_geo_anomaly': False,
            'flag_risky_merchant': False
        }

        if random.random() < 0.02:
            if random.random() < 0.7:
                amount = random.uniform(3000, 9999)
                flags['flag_high_amount'] = True

        if random.random() < 0.01:
            flags['flag_velocity'] = True

        hour = timestamp.hour
        if 2 <= hour <= 4:
            if random.random() < 0.05:
                flags['flag_off_hours'] = True

        if random.random() < 0.01:
            flags['flag_geo_anomaly'] = True

        risky_merchants = ['CRYPTO', 'GAMBLING', 'WIRE_TRANSFER', 'ADULT']
        merchant_type = random.choice(risky_merchants) if random.random() < 0.05 else 'RETAIL'
        if merchant_type in risky_merchants:
            if random.random() < 0.1:
                flags['flag_risky_merchant'] = True

        is_fraud = any(flags.values()) or (random.random() < 0.001)

        return {
            'transaction_id': f"TXN_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            'timestamp': timestamp.isoformat(),
            'card_id': card_id,
            'merchant_id': merchant_id,
            'merchant_type': merchant_type,
            'amount': round(amount, 2),
            'is_online': random.choice([True, False]),
            'location': f"LOC_{random.randint(1, 100)}",
            'home_location': f"LOC_{random.randint(1, 20)}",
            **flags,
            'label': int(is_fraud)
        }
        
    def generate_transaction(self) -> Dict[str, Any]:
        """
        Generate a single transaction.
        
        Returns:
            dict: Transaction in exact Kaggle schema, or the original synthetic shape if the dataset is unavailable
        """
        if self._dataset_iter is not None:
            return next(self._dataset_iter)

        return self._generate_synthetic_transaction()
    
    def produce_stream(self, tps: int = 10, duration_seconds: int = None):
        """
        Continuously produce transactions to Kafka.
        
        Args:
            tps: Transactions per second
            duration_seconds: How long to run (None = forever)
        """
        start_time = time.time()
        tx_count = 0
        
        try:
            stream_mode = 'exact Kaggle dataset' if self._dataset_iter is not None else 'synthetic generator'
            logger.info(f"🚀 Starting transaction stream at {tps} TPS using {stream_mode}...")
            
            while True:
                # Check duration
                if duration_seconds and (time.time() - start_time) > duration_seconds:
                    logger.info(f"Duration reached. Stopping after {tx_count} transactions.")
                    break
                
                # Generate and send transaction
                try:
                    tx = self.generate_transaction()
                except StopIteration:
                    logger.info("Dataset replay completed.")
                    break
                self.producer.send('raw-transactions', value=tx)
                
                tx_count += 1
                if tx_count % 100 == 0:
                    tx_id = tx.get('Transaction_ID', tx.get('transaction_id'))
                    logger.info(f"📊 Produced {tx_count} transactions | Last TXN: {tx_id}")
                
                # Rate limiting: space out transactions to match TPS
                time.sleep(1.0 / tps)
        
        except KeyboardInterrupt:
            logger.info(f"\n⏹️  Stopped. Total transactions: {tx_count}")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("✓ Producer closed")


def main():
    """Entry point for the transaction generator."""
    # Read config from environment
    tps = int(os.getenv('TRANSACTIONS_PER_SECOND', 10))
    kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    
    logger.info(f"Configuration:")
    logger.info(f"  - TPS: {tps}")
    logger.info(f"  - Kafka: {kafka_bootstrap}")
    
    # Create and run generator
    generator = TransactionGenerator(bootstrap_servers=kafka_bootstrap)
    generator.produce_stream(tps=tps)


if __name__ == '__main__':
    main()
