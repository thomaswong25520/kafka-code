#!/usr/bin/env python3
"""
Consumer individuel - à lancer dans des terminaux séparés
Usage: python3 single_consumer.py <consumer_id>
Example: python3 single_consumer.py 1
"""

from kafka import KafkaConsumer
import json
import sys
from datetime import datetime

# ANSI color codes
COLORS = {
    1: '\033[91m',  # Red
    2: '\033[92m',  # Green
    3: '\033[94m',  # Blue
    'RESET': '\033[0m'
}

def run_consumer(consumer_id):
    """
    Run a single consumer with the given ID
    """
    color = COLORS.get(consumer_id, '')
    reset = COLORS['RESET']
    
    print(f"{color}Starting Consumer {consumer_id}{reset}")
    print(f"{color}Connecting to Kafka...{reset}")
    
    consumer = KafkaConsumer(
        'web-events-partitioned',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='web-events-group',  # Même groupe pour tous!
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=1000  # Pour permettre de voir les assignations
    )
    
    print(f"{color}[Consumer {consumer_id}] Connected! Waiting for partition assignment...{reset}")
    
    # Première poll pour déclencher l'assignation des partitions
    consumer.poll(timeout_ms=1000)
    
    # Afficher les partitions assignées
    assigned_partitions = consumer.assignment()
    if assigned_partitions:
        partition_list = [p.partition for p in assigned_partitions]
        print(f"{color}[Consumer {consumer_id}] ✓ Assigned partitions: {partition_list}{reset}")
    else:
        print(f"{color}[Consumer {consumer_id}] ⚠ No partitions assigned yet{reset}")
    
    print(f"{color}[Consumer {consumer_id}] Ready to consume messages...{reset}")
    print("-" * 60)
    
    message_count = 0
    try:
        while True:
            # Poll for messages
            messages = consumer.poll(timeout_ms=1000)
            
            for topic_partition, records in messages.items():
                for message in records:
                    event = message.value
                    message_count += 1
                    
                    print(f"\n{color}[Consumer {consumer_id}] Message #{message_count}{reset}")
                    print(f"{color}  ├─ Event Type: {event.get('event_type')}{reset}")
                    print(f"{color}  ├─ User ID: {event.get('user_id')}{reset}")
                    print(f"{color}  ├─ Timestamp: {event.get('timestamp')}{reset}")
                    print(f"{color}  ├─ Partition: {message.partition}{reset}")
                    print(f"{color}  └─ Offset: {message.offset}{reset}")
                    
    except KeyboardInterrupt:
        print(f"\n{color}[Consumer {consumer_id}] Shutting down gracefully...{reset}")
    finally:
        consumer.close()
        print(f"{color}[Consumer {consumer_id}] Disconnected.{reset}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 single_consumer.py <consumer_id>")
        print("Example: python3 single_consumer.py 1")
        sys.exit(1)
    
    try:
        consumer_id = int(sys.argv[1])
        if consumer_id not in [1, 2, 3]:
            print("Consumer ID must be 1, 2, or 3")
            sys.exit(1)
    except ValueError:
        print("Consumer ID must be a number (1, 2, or 3)")
        sys.exit(1)
    
    run_consumer(consumer_id)