import json
import time
import pandas as pd
from kafka import KafkaProducer

CSV_FILE = "synthetic_fraud_dataset.csv"
TOPIC_NAME = "raw-transactions"
BOOTSTRAP_SERVER = "localhost:9092"

START_ROW = 0
MAX_ROWS = None


DELAY_SECONDS = 0.01


def main():
    print("Reading CSV file...")
    df = pd.read_csv(CSV_FILE)

    print(f"Total rows in dataset: {len(df)}")

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

    print("Required columns are available.")


    df = df.dropna(how="all")


    df = df.where(pd.notnull(df), None)

    df_to_send = df.iloc[START_ROW:]

    if MAX_ROWS is not None:
      df_to_send = df_to_send.head(MAX_ROWS)

    print(f"Rows to send: {len(df_to_send)}")
    print("Connecting to Kafka...")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVER,
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8")
    )

    print(f"Connected to Kafka. Sending messages to topic: {TOPIC_NAME}")

    sent_count = 0

    for index, row in df_to_send.iterrows():
        message = row.to_dict()
        producer.send(TOPIC_NAME, value=message)
        sent_count += 1

        if sent_count % 10 == 0:
            print(f"Sent {sent_count} messages...")

        time.sleep(DELAY_SECONDS)

    producer.flush()
    producer.close()

    print(f"Finished. Total messages sent: {sent_count}")


if __name__ == "__main__":
    main()
