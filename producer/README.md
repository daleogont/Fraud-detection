# Producer Layer

This folder contains the data ingestion producer scripts for the Real-Time Financial Fraud Detection System.

## CSV to Kafka Producer

`csv_to_kafka.py` reads the fraud transaction dataset from a CSV file and streams each transaction row into Kafka as a JSON message.

After the feedback from the progress presentation, the producer was improved from a full CSV loading approach to a chunk-based / micro-batch ingestion approach.

Instead of loading the full dataset into memory at once, the updated producer reads the CSV file in smaller chunks and sends each chunk to Kafka. This makes the ingestion process more scalable, reduces memory usage, and better represents a real-world ETL / streaming pipeline.

## Kafka Topic

`raw-transactions`

## Message Format

Each Kafka message represents one transaction record in JSON format.

## Updated Ingestion Method

The producer uses chunk-based ingestion:

- Reads the CSV dataset in chunks
- Converts each transaction row into a JSON message
- Sends messages to Kafka topic `raw-transactions`
- Adds a delay between chunks to simulate micro-batch streaming

## Example Run

```bash
python csv_to_kafka.py --topic raw-transactions --max-rows 50000 --chunk-size 1000 --delay-between-chunks 2
