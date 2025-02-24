from confluent_kafka import Consumer, Producer

# Config source Kafka 
source_config = {
    'bootstrap.servers': '113.160.15.232:9094,113.160.15.232:9194,113.160.15.232:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024',
    'group.id': 'group_source_consumer',         
    'auto.offset.reset': 'earliest'
}
source_topic = 'product_view'

# Config destination Kafka
destination_config = {
    'bootstrap.servers': 'localhost:9094, localhost:9194, localhost:9294',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'kafka',
    'sasl.password': 'UnigapKafka@2024'
}
destination_topic = 'project_kafka_mongo_1'

# Initialize Producer for target Kafka
producer = Producer(destination_config)

# Initialize Consumer for source Kafka
consumer = Consumer(source_config)
consumer.subscribe([source_topic])

# Callback function for message delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message successfully delivered to {msg.topic()} [{msg.partition()}]")

print("Starting bridge: Reading data from source Kafka and sending to target Kafka...")

try:
    while True:
        msg = consumer.poll(1.0)    # Poll for messages every 1 second
        if msg is None:
            continue
        if msg.error():
            print(f"Eror: {msg.error()}")
            continue
        
        # Decode message (assuming UTF-8 encoding)
        message_value = msg.value().decode('utf-8')
        print(f"Received message from source Kafka: {message_value}")
        
        # Produce message to target Kafka
        producer.produce(destination_topic, msg.value(), callback=delivery_report)
        producer.poll(0)  # Process callback immediately

except KeyboardInterrupt:
    print("Stopping program")

finally:
    consumer.close()
    producer.flush()    # Ensure all messages are sent before existing