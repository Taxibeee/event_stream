from kafka import KafkaProducer
import json
import os

class KafkaOrderProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            client_id='taxibee-order-producer'
        )
        self.topic = os.getenv('KAFKA_ORDERS_TOPIC', 'orders')

    def send_order(self, order_data):
        try:
            # Send the message
            future = self.producer.send(self.topic, value=order_data)
            
            # Wait for the message to be sent
            future.get(timeout=10)
            print(f'Message delivered to {self.topic}')
            
        except Exception as e:
            print(f"Error sending order to Kafka: {e}")

    def __del__(self):
        self.producer.close() 