import asyncio
import json
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

class ThemeParkConsumer:
    def __init__(self, config):
        consumer_config = config.get_kafka_config()
        consumer_config['group.id'] = config.get_consumer_group()
        consumer_config['auto.offset.reset'] = 'earliest'
        consumer_config['enable.auto.commit'] = True
        
        self.consumer = Consumer(consumer_config)
        self.topics = [config.get_topic_names()['avg_waittime']]
        self.running = False
        self.avg_waittimes = {}
        self.callbacks = []
        
        schema_registry_config = config.get_schema_registry_config()
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        
        # Try Avro deserializer (Flink uses Avro by default)
        self.avro_deserializer = AvroDeserializer(
            self.schema_registry_client,
            from_dict=lambda obj, ctx: obj
        )
    
    def add_callback(self, callback):
        self.callbacks.append(callback)
    
    def start_consuming(self):
        self.running = True
        self.consumer.subscribe(self.topics)
        
        async def consume_loop():
            print(f"🎢 Consumer started for topics: {self.topics}")
            
            while self.running:
                try:
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        await asyncio.sleep(0.1)
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            print(f"❌ Consumer error: {msg.error()}")
                            continue
                    
                    # Decode key
                    try:
                        if msg.key():
                            key = self.avro_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                            if isinstance(key, dict):
                                entity_id = key.get('entityId')
                            else:
                                entity_id = str(key)
                        else:
                            entity_id = None
                    except:
                        entity_id = msg.key().decode('utf-8') if msg.key() else None
                    
                    # Try Avro first (Flink default), then JSON fallback
                    value = None
                    try:
                        value = self.avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
                        print(f"✅ Avro deserialized: {value}")
                    except Exception as e:
                        print(f"⚠️ Avro failed: {e}")
                        try:
                            # Fallback to plain JSON
                            value = json.loads(msg.value().decode('utf-8'))
                            print(f"✅ JSON deserialized: {value}")
                        except Exception as e2:
                            print(f"❌ JSON also failed: {e2}")
                            continue
                    
                    if value:
                        avg_waittime = value.get('avg_waittime')
                        name = value.get('name', entity_id)
                        
                        self.avg_waittimes[entity_id] = {
                            'entityId': entity_id,
                            'name': name,
                            'avg_waittime': avg_waittime
                        }
                        
                        self.avg_waittimes[entity_id] = {
                            'entityId': entity_id,
                            'name': name,
                            'avg_waittime': avg_waittime
                        }
                        
                        print(f"📊 {name}: Avg Wait = {avg_waittime:.1f} min")
                        
                        for callback in self.callbacks:
                            callback(list(self.avg_waittimes.values()))
                
                except Exception as e:
                    print(f"❌ Error consuming: {e}")
                    await asyncio.sleep(1)
        
        asyncio.create_task(consume_loop())
    
    def get_avg_waittimes(self):
        return list(self.avg_waittimes.values())
    
    def stop_consuming(self):
        self.running = False
        self.consumer.close()

themepark_consumer = None
