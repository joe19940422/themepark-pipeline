import asyncio
from themepark_config import ThemeParkConfig
from themepark_producer import ThemeParkProducer

async def test_producer():
    config = ThemeParkConfig()
    producer = ThemeParkProducer(config)
    
    print("🧪 Testing producer - fetching data once...")
    
    # Fetch and produce data immediately
    import requests
    import time
    from confluent_kafka.serialization import SerializationContext, MessageField
    
    timestamp_ms = int(time.time() * 1000)
    response = requests.get("https://api.themeparks.wiki/v1/entity/6e1464ca-1e9b-49c3-8937-c5c6f6675057/live", timeout=10)
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
            
            serialized_data = producer.json_serializer(
                event,
                SerializationContext(producer.topic, MessageField.VALUE)
            )
            
            producer.producer.produce(
                producer.topic,
                key=str(event['entityId']).encode('utf-8'),
                value=serialized_data,
                callback=producer.delivery_callback
            )
            
            print(f"📊 {event['name']} | {event['status']} | {event['waitTime']} min")
            count += 1
    
    producer.producer.flush()
    print(f"✅ Test complete! Produced {count} records.")

if __name__ == "__main__":
    asyncio.run(test_producer())
