from services.kafka_producer import KafkaOrderProducer
from services.token_manager import get_access_token
import requests
import os
from datetime import datetime
import time
from dotenv import load_dotenv

load_dotenv()

COMPANY_ID = os.getenv('COMPANY_ID', '129914')
FLEET_ORDERS_URL = "https://node.bolt.eu/fleet-integration-gateway/fleetIntegration/v1/getFleetOrders"

def fetch_and_send_orders(producer):
    try:
        token = get_access_token()
        end_timestamp = int(datetime.now().timestamp())
        start_timestamp = end_timestamp - 86399

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }   

        payload = {
            "offset": 0,
            "limit": 1000,
            "company_ids": [COMPANY_ID],
            "company_id": COMPANY_ID,
            "start_ts": start_timestamp,
            "end_ts": end_timestamp
        }

        response = requests.post(FLEET_ORDERS_URL, headers=headers, json=payload)
        orders = response.json().get("data", {}).get("orders", [])

        print(f"Fetched {len(orders)} orders")

        for order in orders:
            producer.send_order(order)

    except Exception as e:
        print(f"Error fetching and sending orders: {e}")

def main():
    producer = KafkaOrderProducer()

    # Main loop to fetch and send orders
    while True:
        try:
            fetch_and_send_orders(producer)
            time.sleep(60)  # Wait for 1 minute before next fetch
        except KeyboardInterrupt:
            print("Stopping the service...")
            break
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(60)  # Wait before retrying

if __name__ == "__main__":
    main() 