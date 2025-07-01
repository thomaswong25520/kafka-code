#!/usr/bin/env python3
"""
Consumer Group Demo with Partitioned Topic
Shows how Kafka distributes partitions among consumers
"""

from kafka import KafkaConsumer
import json
import sys
import threading
import time
from datetime import datetime

# ANSI color codes for better visualization
COLORS = {
    1: '\033[91m',  # Red
    2: '\033[92m',  # Green
    3: '\033[94m',  # Blue
    'RESET': '\033[0m'
}

def create_consumer(consumer_id, group_id='web-events-group'):
    """
    Create a consumer with a specific ID belonging to a consumer group
    """
    color = COLORS.get(consumer_id, '')
    reset = COLORS['RESET']
    
    consumer = KafkaConsumer(
        'web-events-partitioned',  # Using partitioned topic
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"{color}[Consumer {consumer_id}] Started in group '{group_id}'{reset}")
    
    # Get partition assignment (this happens after first poll)
    consumer.poll(timeout_ms=1000)
    assigned_partitions = consumer.assignment()
    if assigned_partitions:
        partition_list = [p.partition for p in assigned_partitions]
        print(f"{color}[Consumer {consumer_id}] Assigned partitions: {partition_list}{reset}")
    
    print(f"{color}[Consumer {consumer_id}] Waiting for messages...{reset}")
    
    message_count = 0
    for message in consumer:
        event = message.value
        message_count += 1
        
        print(f"\n{color}[Consumer {consumer_id}] Message #{message_count}{reset}")
        print(f"{color}  ├─ Event Type: {event.get('event_type')}{reset}")
        print(f"{color}  ├─ User ID: {event.get('user_id')}{reset}")
        print(f"{color}  ├─ Timestamp: {event.get('timestamp')}{reset}")
        print(f"{color}  ├─ Partition: {message.partition}{reset}")
        print(f"{color}  └─ Offset: {message.offset}{reset}")
        
        # Simulate processing time
        time.sleep(0.5)

def run_consumer_in_thread(consumer_id):
    """
    Run a consumer in a separate thread
    """
    try:
        create_consumer(consumer_id)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Consumer {consumer_id} error: {e}")

def display_info():
    """Display information about the consumer group setup"""
    print("=" * 70)
    print("KAFKA CONSUMER GROUP DEMONSTRATION")
    print("=" * 70)
    print()
    print("This demo shows how Kafka distributes partitions among consumers:")
    print("- Topic: web-events-partitioned (3 partitions)")
    print("- Consumer Group: web-events-group")
    print("- Number of Consumers: 3")
    print()
    print("Expected behavior:")
    print("- Each consumer will be assigned 1 partition")
    print("- Messages from the same user (key) always go to the same partition")
    print("- If a consumer fails, its partitions are reassigned to others")
    print()
    print("Color coding:")
    print("- \033[91mConsumer 1: Red\033[0m")
    print("- \033[92mConsumer 2: Green\033[0m")
    print("- \033[94mConsumer 3: Blue\033[0m")
    print("=" * 70)
    print()

if __name__ == "__main__":
    display_info()
    
    # Create 3 consumer threads
    threads = []
    for i in range(1, 4):
        thread = threading.Thread(target=run_consumer_in_thread, args=(i,))
        thread.daemon = True  # Allow program to exit even if threads are running
        thread.start()
        threads.append(thread)
        time.sleep(2)  # Delay to see partition assignment clearly
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nShutting down consumers...")
        print("Consumer group demo completed.")
        sys.exit(0)