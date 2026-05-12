.PHONY: help up down restart logs clean train train-kaggle

help: ## Show help
	@echo "Fraud Detection System - Make Commands"
	@echo "======================================"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start all services
	@echo "🚀 Starting fraud detection system..."
	docker-compose up -d
	@echo "✓ System starting. Check 'make logs' to see progress"
	@echo ""
	@echo "Service URLs:"
	@echo "  Kafka UI:        http://localhost:8080"
	@echo "  Spark Master:    http://localhost:8081"
	@echo "  Airflow:         http://localhost:8082 (admin/$$AIRFLOW_ADMIN_PASSWORD)"
	@echo "  MLflow:          http://localhost:5001"
	@echo "  Grafana:         http://localhost:3000 (admin/$$GRAFANA_PASSWORD)"
	@echo "  PostgreSQL:      localhost:5432"

down: ## Stop all services
	@echo "⏹️  Stopping services..."
	docker-compose down
	@echo "✓ Services stopped"

restart: down up ## Restart all services

logs: ## Tail all service logs
	docker-compose logs -f

logs-producer: ## Tail producer logs
	docker-compose logs -f transaction-producer

logs-spark: ## Tail Spark streaming logs
	docker-compose logs -f spark-streaming

logs-airflow: ## Tail Airflow logs
	docker-compose logs -f airflow-webserver

status: ## Show service health status
	@echo "📊 Service Status:"
	@docker-compose ps

clean: ## Remove all containers, volumes, and data
	@echo "🧹 Cleaning system (this will remove all data)..."
	docker-compose down -v
	rm -rf ./data ./logs
	@echo "✓ Clean complete"

train: ## Train ML model (default source)
	@echo "🤖 Starting model training..."
	docker-compose run --rm airflow-webserver python /opt/ml/train_model.py
	@echo "✓ Training complete"

train-kaggle: ## Train ML model using the Kaggle fraud dataset
	@echo "🤖 Starting Kaggle dataset training..."
	docker-compose run --rm airflow-webserver python /opt/ml/train_model.py --source csv --dataset-path /data/synthetic_fraud_dataset.csv
	@echo "✓ Kaggle training complete"

kafka-topics: ## List Kafka topics
	docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

kafka-consume: ## Peek at raw transactions (first 10 messages)
	docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic raw-transactions --from-beginning --max-messages 10 --timeout-ms 5000 | head -20

kafka-consume-fraud: ## Peek at flagged transactions (first 10 messages)
	docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic flagged-transactions --from-beginning --max-messages 10 --timeout-ms 5000 | head -20

shell-postgres: ## Connect to PostgreSQL (fraud_db)
	docker-compose exec postgres psql -U ${POSTGRES_USER} -d fraud_db

shell-spark: ## Shell into Spark master
	docker-compose exec spark-master /bin/bash

build: ## Rebuild Docker images
	docker-compose build --no-cache

validate-env: ## Check if .env file is configured
	@if [ ! -f .env ]; then \
		echo "❌ .env file not found!"; \
		echo "Creating from .env.example..."; \
		cp .env.example .env; \
		echo "⚠️  Please edit .env and fill in required values"; \
		exit 1; \
	else \
		echo "✓ .env file found"; \
	fi
