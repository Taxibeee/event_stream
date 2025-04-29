import os
from dotenv import load_dotenv
from bolt_api.token_manager import BoltTokenManager
from kafka import KafkaProducer
import json
# Load environment variables from .env file
load_dotenv()

token_manager = BoltTokenManager(os.getenv('BOLT_TOKEN_URL'), os.getenv('BOLT_CLIENT_ID'), os.getenv('BOLT_CLIENT_SECRET'))
print(token_manager.get_access_token())

order_data = {
    'order_id': '124',
    'order_amount': 100,
    'order_currency': 'USD',
    'order_status': 'completed'
}

producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    client_id='taxibee-order-producer'  
)

producer.send(os.getenv('KAFKA_ORDERS_TOPIC', 'orders'), value=order_data)
producer.flush()
