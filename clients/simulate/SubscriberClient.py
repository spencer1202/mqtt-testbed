import json
import time
import paho.mqtt.client as mqtt
from topic import Topic
from data_classes import BrokerSettings, ClientSettings

class SubscriberClient:
    def __init__(self, broker_settings, client_id, topic, data_callback):
        self.broker_settings = broker_settings
        self.client_id = client_id
        self.topic = topic
        self.data_callback = data_callback
        self.client = None
        
        # For data collection
        self.collection_file = None

    def on_connect(self, client, userdata, flags, rc):
        print(f"Subscriber {self.client_id} connected with result code {rc}")
        # Subscribe to the topic upon successful connection
        self.client.subscribe(self.topic)

    def on_message(self, client, userdata, msg):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        self.data_callback(client, msg.topic, msg.payload, timestamp)
        

    def connect(self):
        clean_session = None if self.broker_settings.protocol == mqtt.MQTTv5 else True
        self.client = mqtt.Client(client_id=self.client_id, protocol=self.broker_settings.protocol, clean_session=clean_session)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        self.client.connect(self.broker_settings.url, self.broker_settings.port, 60)
        self.client.loop_start()

    def disconnect(self):
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()