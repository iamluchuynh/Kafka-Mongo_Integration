from confluent_kafka import Consumer
from pymongo import MongoClient
import json

# MongoDB connection configuration
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['Kafka_MongoDB']
collection = db['Messages']

# Target Kafka configuration (reading from topic 'project_kafka_mongo_1')
destination_conf = {
    'bootstrap.servers': 'localhost:9094, localhost:9194, localhost:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',
    'group.id': 'group_mongo_consumer',  # Separate consumer group for MongoDB processing
    'auto.offset.reset': 'earliest'
}
destination_topic = 'project_kafka_mongo_1'

# Initialize Consumer for target Kafka
consumer = Consumer(destination_conf)
consumer.subscribe([destination_topic])

print("Starting consumer: Reading data from Kafka and storing it in MongoDB...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        
        # Decode message
        data = json.loads(msg.value().decode('utf-8'))
        print(f"Received message from target Kafka: {data}")
        
        result = collection.insert_one(data)
        print(f"Document saved to MongoDB with ID: {result.inserted_id}")
except KeyboardInterrupt:
    print("Stopping program")
finally:
    consumer.close()
