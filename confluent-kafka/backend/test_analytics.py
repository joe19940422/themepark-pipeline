import json
from confluent_kafka import Producer
from themepark_config import ThemeParkConfig

config = ThemeParkConfig()
kafka_config = config.get_kafka_config()
producer = Producer(kafka_config)

# Produce test analytics data directly to output topic
test_data = [
    {"entityId": "test-1", "name": "Space Mountain", "avg_waittime": 45.5},
    {"entityId": "test-2", "name": "Big Thunder Mountain", "avg_waittime": 30.2},
    {"entityId": "test-3", "name": "Pirates of the Caribbean", "avg_waittime": 25.8}
]

topic = config.get_topic_names()['avg_waittime']

for data in test_data:
    producer.produce(
        topic,
        key=data['entityId'].encode('utf-8'),
        value=json.dumps(data).encode('utf-8')
    )
    print(f"✅ Produced: {data['name']} - {data['avg_waittime']} min")

producer.flush()
print(f"\n✅ Test data sent to {topic}")
print("Check http://localhost:8002/api/attractions")
