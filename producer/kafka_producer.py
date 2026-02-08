import requests
import json
import time
from kafka import KafkaProducer


# --- Configuration ---
API_URL = "https://api.themeparks.wiki/v1/entity/6e1464ca-1e9b-49c3-8937-c5c6f6675057/live"
# IMPORTANT: Use the address exposed to the host machine for external scripts.
# Based on your Docker Compose: localhost:9092
KAFKA_BROKER = ['localhost:29092']
KAFKA_TOPIC = "themepark-raw"
POLL_INTERVAL_SECONDS = 60  # Fetch data every 15 seconds

# Initialize Kafka Producer (moved to main execution block below)
producer = None

def fetch_and_produce_data(producer: KafkaProducer):
    timestamp_ms = int(time.time() * 1000)
    """
    Fetches API data and sends individual ride/entity updates to Kafka.

    Args:
        producer: The initialized KafkaProducer instance.
    """
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        # --- CORRECTED LOGIC START ---

        # 1. Access the 'liveData' list, which contains the entities
        live_entities = data.get('liveData', [])

        for entity_data in live_entities:
            # Safely extract the wait time, defaulting to 0 if the 'queue' or 'STANDBY' key is missing
            wait_time = entity_data.get('queue', {}).get('STANDBY', {}).get('waitTime', 0)

            # Ensure entityType is ATTRACTION and waitTime is not None (in case the API sends None)
            if entity_data.get('entityType') == 'ATTRACTION' and wait_time is not None:
                # Use a cleaner way to define the event dictionary

                event = {
                    "entityId": entity_data.get('id'),
                    "timestamp_ms": timestamp_ms,  # Event time in ms
                    "status": entity_data.get('status'),
                    "name": entity_data.get('name'),
                    # Ensure wait_time is an integer for consistency, defaulting to 0
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

        # --- CORRECTED LOGIC END ---

        producer.flush()
        print(
            f"✅ Successfully fetched and produced {len(live_entities)} data points. Sleeping for {POLL_INTERVAL_SECONDS} seconds...")

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred during processing or sending: {e}")


if __name__ == "__main__":
    print(f"Starting Kafka Producer. Connecting to broker: {KAFKA_BROKER}")

    # Initialize Kafka Producer here
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,  # Add retry logic
        )
        # Verify connection by sending an initial message (optional but good for debugging)
        # producer.send(KAFKA_TOPIC, value={"message": "Producer started"}, key=b"startup_check").get(timeout=5)
        # print("✅ Producer connected successfully.")

    except Exception as e:
        print(
            f"🛑 CRITICAL ERROR: Could not connect to Kafka broker {KAFKA_BROKER}. Check Docker configuration and port binding. Error: {e}")
        exit(1)

    print(f"Sending data to {KAFKA_TOPIC} every {POLL_INTERVAL_SECONDS} seconds.")

    while True:
        fetch_and_produce_data(producer)
        time.sleep(POLL_INTERVAL_SECONDS)

# --- Console Consumer Command Reminder ---
# To test the output from the Docker network:
# docker exec -it kafka /usr/bin/kafka-console-consumer --bootstrap-server kafka:29092 --topic themepark-raw --from-beginning