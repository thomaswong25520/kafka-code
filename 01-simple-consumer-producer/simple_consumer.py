import json
from kafka import KafkaConsumer

# Create the consumer
consumer = KafkaConsumer(
    'web-events',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='simple-group'
)

print("🎧 Consumer started, listening for events...")
print("👆 Ctrl+C to stop\n")

# Read messages
try:
    for message in consumer:
        event = message.value
        print(f"📨 Received: {event}")
        
except KeyboardInterrupt:
    print("\n👋 Consumer stopped!")
finally:
    consumer.close()