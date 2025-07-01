#!/bin/bash

# Script to create a topic with 3 partitions for better consumer group demonstration

echo "Creating topic 'web-events-partitioned' with 3 partitions..."

# First, delete the topic if it exists (optional)
docker compose exec kafka kafka-topics --delete \
    --topic web-events-partitioned \
    --bootstrap-server localhost:9092 \
    2>/dev/null || true

# Wait a moment for deletion to complete
sleep 2

# Create new topic with 3 partitions
docker compose exec kafka kafka-topics --create \
    --topic web-events-partitioned \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

echo "Topic created successfully!"
echo ""

# List topic details
echo "Topic details:"
docker compose exec kafka kafka-topics --describe \
    --topic web-events-partitioned \
    --bootstrap-server localhost:9092

echo ""
echo "You can now run the producer and consumer group to see partition distribution!"