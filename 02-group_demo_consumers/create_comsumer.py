#!/usr/bin/env python3
"""
Consumer Group Demo: Running 3 consumers in the same group
Each consumer will process a subset of partitions
"""

from kafka import KafkaConsumer
import json
import sys
import threading
import time
from datetime import datetime

def create_consumer(consumer_id, group_id='web-events-group'):
    """
    Create a consumer with a specific ID belonging to a consumer group
    """
    consumer = KafkaConsumer(
        'web-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print(f"Consumer {consumer_id} started in group '{group_id}'")
    print(f"Waiting for messages...")
    
    for message in consumer:
        event = message.value
        print(f"\n[Consumer {consumer_id}] Processing event:")
        print(f"  - Event Type: {event.get('event_type')}")
        print(f"  - User ID: {event.get('user_id')}")
        print(f"  - Timestamp: {event.get('timestamp')}")
        print(f"  - Partition: {message.partition}")
        print(f"  - Offset: {message.offset}")
        
        # Simulate some processing time
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

if __name__ == "__main__":
    print("Starting 3 consumers in the same consumer group...")
    print("All consumers will share the work of processing messages")
    print("-" * 60)
    
    # Create 3 consumer threads
    threads = []
    for i in range(1, 4):
        thread = threading.Thread(target=run_consumer_in_thread, args=(i,))
        thread.start()
        threads.append(thread)
        time.sleep(1)  # Small delay between starting consumers
    
    try:
        # Keep the main thread alive
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("\nShutting down consumers...")
        sys.exit(0)