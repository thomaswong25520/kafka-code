#!/usr/bin/env python3
"""
Enhanced producer that sends to a partitioned topic
Shows which partition each message goes to
"""

from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Event templates similar to your original producer
USERS = ['user_001', 'user_002', 'user_003', 'user_004', 'user_005']
PAGES = ['/home', '/products', '/cart', '/checkout', '/about', '/contact']
PRODUCTS = ['laptop', 'phone', 'tablet', 'headphones', 'keyboard']

def generate_event():
    """Generate a random web event"""
    event_types = ['page_view', 'click', 'purchase']
    event_type = random.choice(event_types)
    
    event = {
        'event_type': event_type,
        'user_id': random.choice(USERS),
        'timestamp': datetime.now().isoformat(),
        'session_id': f"session_{random.randint(1000, 9999)}"
    }
    
    if event_type == 'page_view':
        event['page'] = random.choice(PAGES)
        event['duration_seconds'] = random.randint(5, 300)
    elif event_type == 'click':
        event['element'] = random.choice(['button', 'link', 'image'])
        event['page'] = random.choice(PAGES)
    elif event_type == 'purchase':
        event['product'] = random.choice(PRODUCTS)
        event['price'] = round(random.uniform(10, 1000), 2)
        event['quantity'] = random.randint(1, 3)
    
    return event

def on_send_success(record_metadata):
    """Callback for successful sends"""
    print(f"✓ Message sent to partition {record_metadata.partition} at offset {record_metadata.offset}")

def on_send_error(excp):
    """Callback for failed sends"""
    print(f"✗ Failed to send message: {excp}")

if __name__ == "__main__":
    # Create producer
    producer = KafkaProducer(
        # Kafka broker addresses (host:port) for cluster connection
        bootstrap_servers=['localhost:9092'],
        # Convert Python objects (dict, list) to JSON string, then to UTF-8 bytes for Kafka
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # Use the user_id as key for consistent partition assignment
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    print("Starting partitioned producer...")
    print("Messages will be distributed across 3 partitions based on user_id")
    print("-" * 60)
    
    try:
        while True:
            event = generate_event()
            
            # Use user_id as key for partition assignment
            # This ensures all events from the same user go to the same partition
            key = event['user_id']
            
            print(f"\nSending event: {event['event_type']} from {event['user_id']}")
            
            # Send with callback to see partition assignment
            future = producer.send(
                'web-events-partitioned',
                key=key,
                value=event
            )
            
            # Add callbacks
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)
            
            # Wait a bit before sending next event
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.close()
        print("Producer closed.")