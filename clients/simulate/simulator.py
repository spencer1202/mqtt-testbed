import json
import time
import paho.mqtt.client as mqtt
from topic import Topic
from data_classes import BrokerSettings, ClientSettings
from data_classes.broker_settings import BrokerSettings
from data_classes.client_settings import ClientSettings
from SubscriberClient import SubscriberClient

class Simulator:
    def __init__(self, settings_file):
        self.default_client_settings = ClientSettings(
            clean=True,
            retain=False,
            qos=2,
            time_interval=10
        )
        self.settings_file = settings_file
        self.broker_settings = None
        self.topics = []
        self.subscribers = []
        self.load_configuration()
        self.log_file = "subscriber_log.txt"  # Log file for subscriber messages

    def load_configuration(self):
        with open(self.settings_file) as json_file:
            config = json.load(json_file)
            self.broker_settings = BrokerSettings(
                url=config.get('BROKER_URL', 'localhost'),
                port=config.get('BROKER_PORT', 1883),
                protocol=config.get('PROTOCOL_VERSION', 4)  # mqtt.MQTTv311
            )
            broker_client_settings = self.read_client_settings(config, default=self.default_client_settings)
            
            # Load publisher topics
            if 'TOPICS' in config:
                self.topics = self.load_topics(config['TOPICS'], broker_client_settings)
            
            # Load subscriber configurations
            if 'SUBSCRIBERS' in config:
                self.subscribers = self.load_subscribers(config['SUBSCRIBERS'])

    def read_client_settings(self, settings_dict: dict, default: ClientSettings):
        return ClientSettings(
            clean=settings_dict.get('CLEAN_SESSION', default.clean),
            retain=settings_dict.get('RETAIN', default.retain),
            qos=settings_dict.get('QOS', default.qos),
            time_interval=settings_dict.get('TIME_INTERVAL', default.time_interval)
        )

    def load_topics(self, topics_config, broker_client_settings):
        topics = []
        for topic in topics_config:
            topic_data = topic['DATA']
            topic_payload_root = topic.get('PAYLOAD_ROOT', {})
            topic_client_settings = self.read_client_settings(topic, default=broker_client_settings)
            if topic['TYPE'] == 'single':
                # create single topic with format: /{PREFIX}
                topic_url = topic['PREFIX']
                topics.append(Topic(self.broker_settings, topic_url, topic_data, topic_payload_root, topic_client_settings))
            elif topic['TYPE'] == 'multiple':
                # create multiple topics with format: /{PREFIX}/{id}
                for id in range(topic['RANGE_START'], topic['RANGE_END']+1):
                    topic_url = topic['PREFIX'] + '/' + str(id)
                    topics.append(Topic(self.broker_settings, topic_url, topic_data, topic_payload_root, topic_client_settings))
            elif topic['TYPE'] == 'list':
                # create multiple topics with format: /{PREFIX}/{item}
                for item in topic['LIST']:
                    topic_url = topic['PREFIX'] + '/' + str(item)
                    topics.append(Topic(self.broker_settings, topic_url, topic_data, topic_payload_root, topic_client_settings))
        return topics

    def load_subscribers(self, subscribers_config):
        subscribers = []
        for sub_config in subscribers_config:
            topic_pattern = sub_config['TOPIC']
            num_subscribers = sub_config.get('NUMBER', 1)
            
            for i in range(num_subscribers):
                subscriber = SubscriberClient(
                    broker_settings=self.broker_settings,
                    client_id=f"subscriber-{topic_pattern}-{i}",
                    topic=topic_pattern,
                    data_callback=self.on_message_received
                )
                subscribers.append(subscriber)
        
        return subscribers

    def on_message_received(self, client, topic, payload, timestamp):
        """Callback for when a subscriber receives a message"""
        client_id = client._client_id.decode('utf-8')
        print(f"[{timestamp}] Client {client_id} received message on topic '{topic}': {len(payload)} bytes")
        # In a real implementation, this would save data to collection files
        # Log the received message
        try:
            # Try to decode as JSON for better logging
            payload_str = msg.payload.decode('utf-8')
            try:
                payload_json = json.loads(payload_str)
                payload_formatted = json.dumps(payload_json, indent=2)
            except json.JSONDecodeError:
                payload_formatted = payload_str if len(payload_str) < 100 else f"{payload_str[:97]}..."
        except UnicodeDecodeError:
            # If not text, just log the size
            payload_formatted = f"<binary data: {len(msg.payload)} bytes>"
            
        log_entry = f"[{timestamp}] {client_id} Received message on topic '{msg.topic}':\n{payload_formatted}\n{'-'*40}"
        self._write_to_log(log_entry)

    def _write_to_log(self, message):
        """Write a message to this subscriber's log file"""
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(f"{message}\n")

    def run(self):
        # Start all subscribers
        for subscriber in self.subscribers:
            print(f'Starting subscriber: {subscriber.client_id} for topic {subscriber.topic} ...')
            subscriber.connect()
        
        # Start all publishers
        for topic in self.topics:
            print(f'Starting publisher: {topic.topic_url} ...')
            topic.start()
        
        try:
            # Keep the main thread running
            for topic in self.topics:
                topic.join()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        # Stop all publishers
        for topic in self.topics:
            print(f'Stopping publisher: {topic.topic_url} ...')
            if topic.is_alive():
                topic.disconnect()
        
        # Stop all subscribers
        for subscriber in self.subscribers:
            print(f'Stopping subscriber: {subscriber.client_id} ...')
            subscriber.disconnect()
