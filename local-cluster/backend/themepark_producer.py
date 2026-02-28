import asyncio
import json
import time
import requests
from kafka import KafkaProducer

API_URL = "https://api.themeparks.wiki/v1/entity/6e1464ca-1e9b-49c3-8937-c5c6f6675057/live"
POLL_INTERVAL_SECONDS = 300

class ThemeParkProducer:
    def __init__(self, kafka_broker, topic):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        self.topic = topic
        self.running = False
    
    async def fetch_and_produce(self):
        self.running = True
        print(f"🎢 Starting theme park data producer...")
        
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
                        
                        self.producer.send(
                            self.topic,
                            value=event,
                            key=str(event['entityId']).encode('utf-8')
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
