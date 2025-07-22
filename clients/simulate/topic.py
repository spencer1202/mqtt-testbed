import threading
import json
import time
import uuid
import paho.mqtt.client as mqtt
from data_classes import BrokerSettings, ClientSettings
from topic_data import TopicDataNumber, TopicDataBool, TopicDataRawValue, TopicDataMathExpression

class Topic(threading.Thread):
    def __init__(self, broker_settings: BrokerSettings, topic_url: str, topic_data: list[object], topic_payload_root: object, client_settings: ClientSettings, metadata_config: dict = None):
        threading.Thread.__init__(self)

        self.broker_settings = broker_settings

        self.topic_url = topic_url
        self.topic_data = self.load_topic_data(topic_data)
        self.topic_payload_root = topic_payload_root

        self.client_settings = client_settings

        self.loop = False
        self.client = None
        self.payload = None

        # Message metadata configuration
        self.metadata_config = metadata_config or {
            'include_message_id': True,
            'include_timestamp': True,
            'metadata_field_prefix': '_'
        }

    def load_topic_data(self, topic_data_object):
        topic_data = []
        for data in topic_data_object:
            data_type = data['TYPE']
            if data_type == 'int' or data_type == 'float':
                topic_data.append(TopicDataNumber(data))
            elif data_type == 'bool':
                topic_data.append(TopicDataBool(data))
            elif data_type == 'raw_values':
                topic_data.append(TopicDataRawValue(data))
            elif data_type == 'math_expression':
                topic_data.append(TopicDataMathExpression(data))
            else:
                raise NameError(f"Data TYPE '{data_type}' is unknown")
        return topic_data

    def connect(self):
        self.loop = True
        clean_session = None if self.broker_settings.protocol == mqtt.MQTTv5 else self.client_settings.clean
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, self.topic_url, protocol=self.broker_settings.protocol, clean_session=clean_session)
        self.client.on_publish = self.on_publish
        self.client.connect(self.broker_settings.url, self.broker_settings.port)
        self.client.loop_start()

    def disconnect(self):
        self.loop = False
        self.client.loop_stop()
        self.client.disconnect()

    def run(self):
        self.connect()
        while self.loop:
            # Generate payload
            payload = self.generate_payload()
            
            # Add message metadata
            prefix = self.metadata_config.get('metadata_field_prefix', '_')
            
            if self.metadata_config.get('include_message_id', True):
                payload[f"{prefix}message_id"] = str(uuid.uuid4())
            
            if self.metadata_config.get('include_timestamp', True):
                # Add timestamp in milliseconds
                payload[f"{prefix}timestamp"] = int(time.time() * 1000)
            
            # Convert to JSON and publish
            payload_json = json.dumps(payload)
            self.client.publish(self.topic_url, payload_json, qos=self.client_settings.qos, retain=self.client_settings.retain)
            
            # Sleep until next interval
            time.sleep(self.client_settings.time_interval)

    def on_publish(self, client, userdata, result):
        print(f'[{time.strftime("%H:%M:%S")}] Data published on: {self.topic_url}')

    def generate_payload(self):
        payload = {}
        payload.update(self.topic_payload_root)
        has_data_active = False
        for data in self.topic_data:
            if data.is_active:
                has_data_active = True
                payload[data.name] = data.generate_value()
        if not has_data_active:
            self.disconnect()
            return
        return payload
