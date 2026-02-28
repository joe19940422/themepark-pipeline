import asyncio
import json
import time
from datetime import datetime
import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

API_URL = "https://api.themeparks.wiki/v1/entity/6e1464ca-1e9b-49c3-8937-c5c6f6675057/live"
POLL_INTERVAL_SECONDS = 300

ATTRACTION_SCHEMA = {
    "title": "AttractionData",
    "type": "object",
    "properties": {
        "entityId": {"type": "string"},
        "timestamp_ms": {"type": "integer"},
        "status": {"type": "string"},
        "name": {"type": "string"},
        "waitTime": {"type": "integer"},
        "entityType": {"type": "string"}
    },
    "required": ["entityId", "timestamp_ms", "status", "name", "waitTime", "entityType"]
}

class ThemeParkProducer:
    def __init__(self, config):
        kafka_config = config.get_kafka_config()
        self.producer = Producer(kafka_config)
        self.topic = config.get_topic_names()['raw_data']
        self.running = False
        
        schema_registry_config = config.get_schema_registry_config()
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        
        self.json_serializer = JSONSerializer(
            json.dumps(ATTRACTION_SCHEMA),
            self.schema_registry_client
        )
    
    def delivery_callback(self, err, msg):
        if err:
            print(f'❌ Delivery failed: {err}')
        else:
            print(f'✅ Delivered to {msg.topic()} [{msg.partition()}]')
    
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
                        
                        serialized_data = self.json_serializer(
                            event,
                            SerializationContext(self.topic, MessageField.VALUE)
                        )
                        
                        self.producer.produce(
                            self.topic,
                            key=str(event['entityId']).encode('utf-8'),
                            value=serialized_data,
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

themepark_producer = None
