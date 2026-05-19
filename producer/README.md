# Producer Layer

This folder contains the data ingestion producer scripts for the Real-Time Financial Fraud Detection System.

## CSV to Kafka Producer

`csv_to_kafka.py` reads the fraud transaction dataset from a CSV file and streams each transaction row into Kafka as a JSON message.

## Kafka Topic

`raw-transactions`

## Message Format

Each Kafka message represents one transaction record in JSON format.

## Verification

The full dataset was streamed into Kafka successfully.

Total records sent: `50,000`

Kafka offsets verified:

```text
raw-transactions:0:16653
raw-transactions:1:16722
raw-transactions:2:16625