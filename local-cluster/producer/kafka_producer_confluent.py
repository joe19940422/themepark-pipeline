import requests
import json
import time
import os
from kafka import KafkaProducer

# --- Configuration ---
API_URL = "https://api.themeparks.wiki/v1/entity/6e1464ca-1e9b-49c3-8937-c5c6f6675057/live"

# 1. Update these with your Confluent Cloud details
# It is best practice to use Environment Variables, but you can paste strings here for testing.
KAFKA_BROKER = os.getenv('CONFLUENT_BOOTSTRAP_SERVER', 'pkc-oxqxx9.us-east-1.aws.confluent-kafka.cloud:9092')
KAFKA_API_KEY = os.getenv('CONFLUENT_API_KEY', 'WBQXUHDWIAQXTS3V')
KAFKA_API_SECRET = os.getenv('CONFLUENT_API_SECRET', 'cfltUUFIR+vbtBbciIbuDw6lS/89dTFXo4kcOhY2aMMeJATduL10Yy9NHEi7h2KQ')

KAFKA_TOPIC = "themepark-raw"
POLL_INTERVAL_SECONDS = 300

# Initialize Kafka Producer (moved to main execution block below)
producer = None

def fetch_and_produce_data(producer: KafkaProducer):
    timestamp_ms = int(time.time() * 1000)
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        live_entities = data.get('liveData', [])

        for entity_data in live_entities:
            wait_time = entity_data.get('queue', {}).get('STANDBY', {}).get('waitTime', 0)

            if entity_data.get('entityType') == 'ATTRACTION' and wait_time is not None:
                event = {
                    "entityId": entity_data.get('id'),
                    "timestamp_ms": timestamp_ms,
                    "status": entity_data.get('status'),
                    "name": entity_data.get('name'),
                    "waitTime": int(wait_time),
                    "entityType": entity_data.get('entityType')
                }

                print('--- New Event ---')
                print(event)

                # Send to Kafka using entityId as key
                entity_id = event['entityId']

                producer.send(
                    KAFKA_TOPIC,
                    value=event,
                    key=str(entity_id).encode('utf-8')
                )
                print(f"Produced: {event['name']} | Status: {event['status']} | WaitTime: {event['waitTime']} minutes")

        producer.flush()
        print(f"✅ Successfully fetched and produced {len(live_entities)} data points. Sleeping for {POLL_INTERVAL_SECONDS} seconds...")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    print(f"Starting Confluent Cloud Producer...")

    # Initialize Kafka Producer with SASL_SSL for Confluent Cloud
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            # --- SECURITY SETTINGS START ---
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=KAFKA_API_KEY,
            sasl_plain_password=KAFKA_API_SECRET,
            # --- SECURITY SETTINGS END ---
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        print("✅ Producer connected to Confluent Cloud successfully.")

    except Exception as e:
        print(f"🛑 CRITICAL ERROR: Could not connect to Confluent Cloud. Check your API Key/Secret and Broker URL. Error: {e}")
        exit(1)

    print(f"Sending data to {KAFKA_TOPIC} every {POLL_INTERVAL_SECONDS} seconds.")

    while True:
        fetch_and_produce_data(producer)
        time.sleep(POLL_INTERVAL_SECONDS)