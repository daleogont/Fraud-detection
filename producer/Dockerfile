FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY transaction_generator.py .

# Run producer
CMD ["python", "transaction_generator.py"]
