from kafka import KafkaConsumer
import json

# Create a Kafka consumer
consumer = KafkaConsumer(
    'test-topic',  # Topic to listen to
    bootstrap_servers='localhost:9092',  # Update this with your Kafka broker
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id='my-group'  # Consumers in the same group share messages
)

print('Listening for messages...')

# Consume messages
for message in consumer:
    print(f'Received: {message.value}')
