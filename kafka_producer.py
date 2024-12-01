from kafka import KafkaProducer
import json
import time

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'test-topic'

# Send messages to specific partitions
for i in range(10):
    partition = i % 3  
    message = {'number': i, 'message': f'Hello Kafka! Message {i}', "Partition": partition}
    # Send messages to 3 partitions in a round-robin manner
    producer.send(topic_name, value=message, partition=partition)
    print(f'Sent to partition {partition}: {message}')
    time.sleep(1)

producer.close()
