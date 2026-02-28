import asyncio
import json
import time
import requests
from confluent_kafka import Producer

API_URL = "https://api.themeparks.wiki/v1/entity/6e1464ca-1e9b-49c3-8937-c5c6f6675057/live"
POLL_INTERVAL_SECONDS = 300

class SimpleThemeParkProducer:
    def __init__(self, config):
        kafka_config = config.get_kafka_config()
        self.producer = Producer(kafka_config)
        self.topic = config.get_topic_names()['raw_data']
        self.running = False
    
    def delivery_callback(self, err, msg):
        if err:
            print(f'❌ Delivery failed: {err}')
        else:
            print(f'✅ Delivered to {msg.topic()}')
    
    async def fetch_and_produce(self):
        self.running = True
        print(f"🎢 Starting theme park data producer (no schema registry)...")
        
        while self.running:
            try:
                timestamp_ms = int(time.time() * 1000)
                response = requests.get(API_URL, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                live_entities = data.get('liveData', [])
                count = 0
                
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
                        
                        self.producer.produce(
                            self.topic,
                            key=str(event['entityId']).encode('utf-8'),
                            value=json.dumps(event).encode('utf-8'),
                            callback=self.delivery_callback
                        )
                        
                        print(f"📊 {event['name']} | {event['status']} | {event['waitTime']} min")
                        count += 1
                
                self.producer.flush()
                print(f"✅ Produced {count} records. Sleeping {POLL_INTERVAL_SECONDS}s...\n")
                
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                
            except requests.exceptions.RequestException as e:
                print(f"❌ API Error: {e}")
                await asyncio.sleep(60)
            except Exception as e:
                print(f"❌ Error: {e}")
                await asyncio.sleep(60)
    
    def stop(self):
        self.running = False
        self.producer.flush()

simple_producer = None
