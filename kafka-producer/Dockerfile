FROM python:3.11-slim

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN set -ex; \
  	pip install --no-cache-dir -r requirements.txt

# Copy resources
WORKDIR /
COPY wait-for-it.sh wait-for-it.sh

# Copy the producer script
ADD python-producer.py .

# Execute the producer script
CMD ./wait-for-it.sh -s -t 10 $ZOOKEEPER_SERVER -- ./wait-for-it.sh -s -t 30 $KAFKA_SERVER -- python python-producer.py