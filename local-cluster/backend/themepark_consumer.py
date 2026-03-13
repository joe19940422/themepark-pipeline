import asyncio
import json
from kafka import KafkaConsumer

class ThemeParkConsumer:
    def __init__(self, kafka_broker, topic, group_id):
        self.kafka_broker = kafka_broker
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.running = False
        self.avg_waittimes = {}
        self.callbacks = []
    
    def add_callback(self, callback):
        self.callbacks.append(callback)
    
    def start_consuming(self):
        self.running = True
        
        async def consume_loop():
            print(f"🎢 Consumer starting for topic: {self.topic}")
            
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.kafka_broker,
                group_id=self.group_id,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            print(f"✅ Consumer connected to {self.kafka_broker}")
            
            while self.running:
                try:
                    messages = self.consumer.poll(timeout_ms=1000)
                    
                    for topic_partition, records in messages.items():
                        for msg in records:
                            value = msg.value
                            entity_id = value.get('entityId')
                            name = value.get('name')
                            avg_waittime = value.get('avg_waittime')
                            
                            if avg_waittime is not None:
                                self.avg_waittimes[entity_id] = {
                                    'entityId': entity_id,
                                    'name': name,
                                    'avg_waittime': avg_waittime
                                }
                                
                                print(f"📊 {name}: Avg Wait = {avg_waittime:.1f} min")
                                
                                for callback in self.callbacks:
                                    callback(list(self.avg_waittimes.values()))
                    
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    print(f"❌ Error consuming: {e}")
                    await asyncio.sleep(1)
        
        asyncio.create_task(consume_loop())
    
    def get_avg_waittimes(self):
        return list(self.avg_waittimes.values())
    
    def stop_consuming(self):
        self.running = False
        if self.consumer:
            self.consumer.close()
