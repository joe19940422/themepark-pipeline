import yaml
from typing import Dict, Any
import os

class ThemeParkConfig:
    def __init__(self, config_path: str = "themepark_config.yaml"):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
    
    def get_kafka_config(self) -> Dict[str, Any]:
        kafka_config = self.config['kafka'].copy()
        kafka_config.pop('schema_registry_url', None)
        kafka_config.pop('schema_registry_api_key', None)
        kafka_config.pop('schema_registry_secret', None)
        kafka_config.pop('topics', None)
        kafka_config.pop('consumer_group', None)
        
        kafka_config['sasl.username'] = os.getenv('KAFKA_API_KEY', kafka_config['sasl.username'])
        kafka_config['sasl.password'] = os.getenv('KAFKA_API_SECRET', kafka_config['sasl.password'])
        kafka_config['bootstrap.servers'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS', kafka_config['bootstrap.servers'])
        return kafka_config
    
    def get_schema_registry_config(self) -> Dict[str, str]:
        schema_config = {
            'url': self.config['kafka']['schema_registry_url'],
            'basic.auth.user.info': f"{self.config['kafka']['schema_registry_api_key']}:{self.config['kafka']['schema_registry_secret']}"
        }
        return schema_config
    
    def get_topic_names(self) -> Dict[str, str]:
        return self.config['kafka']['topics']
    
    def get_consumer_group(self) -> str:
        return self.config['kafka']['consumer_group']

config = None
