import argparse
import json
import time

import pandas as pd
from kafka import KafkaProducer


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Chunk-based CSV to Kafka producer for fraud transaction ingestion."
    )

    parser.add_argument(
        "--csv-file",
        default="synthetic_fraud_dataset.csv",
        help="Path to the input CSV dataset."
    )

    parser.add_argument(
        "--topic",
        default="raw-transactions",
        help="Kafka topic name."
    )

    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092",
        help="Kafka bootstrap server."
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Number of CSV rows to read per chunk."
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Maximum number of rows to send. Use no value to send the full dataset."
    )

    parser.add_argument(
        "--delay-between-chunks",
        type=float,
        default=2.0,
        help="Delay in seconds between chunks to simulate micro-batch streaming."
    )

    return parser.parse_args()


def validate_columns(df):
    required_columns = [
        "Transaction_ID",
        "User_ID",
        "Transaction_Amount",
        "Transaction_Type",
        "Timestamp",
        "Fraud_Label"
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def create_producer(bootstrap_server):
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8")
    )


def main():
    args = parse_arguments()

    print("Starting chunk-based CSV to Kafka producer...")
    print(f"CSV file: {args.csv_file}")
    print(f"Kafka topic: {args.topic}")
    print(f"Bootstrap server: {args.bootstrap_server}")
    print(f"Chunk size: {args.chunk_size}")
    print(f"Delay between chunks: {args.delay_between_chunks} seconds")
    print(f"Max rows: {args.max_rows if args.max_rows is not None else 'Full dataset'}")

    producer = create_producer(args.bootstrap_server)

    total_sent = 0
    chunk_number = 0

    try:
        for chunk in pd.read_csv(args.csv_file, chunksize=args.chunk_size):
            chunk_number += 1

            validate_columns(chunk)

            # Remove fully empty rows
            chunk = chunk.dropna(how="all")

            # Convert NaN values to None so JSON uses null
            chunk = chunk.where(pd.notnull(chunk), None)

            if args.max_rows is not None:
                remaining_rows = args.max_rows - total_sent

                if remaining_rows <= 0:
                    break

                chunk = chunk.head(remaining_rows)

            for _, row in chunk.iterrows():
                message = row.to_dict()
                producer.send(args.topic, value=message)
                total_sent += 1

            producer.flush()

            print(
                f"Chunk {chunk_number} processed | "
                f"Chunk rows sent: {len(chunk)} | "
                f"Total messages sent: {total_sent}"
            )

            if args.max_rows is not None and total_sent >= args.max_rows:
                break

            time.sleep(args.delay_between_chunks)

    finally:
        producer.flush()
        producer.close()

    print(f"Finished. Total messages sent: {total_sent}")


if __name__ == "__main__":
    main()

