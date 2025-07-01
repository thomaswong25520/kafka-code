import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Kafka cluster connection
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Simulate web events
def generate_web_event():
    events = ['click', 'view', 'scroll', 'purchase', 'signup']
    pages = ['/home', '/products', '/about', '/contact', '/checkout']
    
    return {
        'timestamp': datetime.now().isoformat(),
        'event_type': random.choice(events),
        'page': random.choice(pages),
        'user_id': f"user_{random.randint(1, 1000)}",
        'session_id': f"session_{random.randint(1, 100)}"
    }

# Sending events
print("🚀 Sending web events...")
for i in range(10):
    event = generate_web_event()
    
    # Send to Kafka
    producer.send('web-events', value=event)
    print(f"📤 Sent: {event}")
    
    time.sleep(1)  # Wait 1 sec

# Ensure all has been sent
producer.flush()
print("✅ All the events have been sent!")