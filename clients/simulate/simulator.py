import json
import time
import os
import uuid
import datetime
from pathlib import Path
import paho.mqtt.client as mqtt
from topic import Topic
from data_classes import BrokerSettings, ClientSettings
from data_classes.broker_settings import BrokerSettings
from data_classes.client_settings import ClientSettings
from SubscriberClient import SubscriberClient

class Simulator:
    def __init__(self, settings_file, output_dir=None):
        self.default_client_settings = ClientSettings(
            clean=True,
            retain=False,
            qos=2,
            time_interval=10
        )
        self.settings_file = settings_file
        
        # Set up log directory - default to /logs for Docker, ~/Downloads/mqtt-logs for local
        if output_dir is None:
            # Check if we're in a Docker container (check for /.dockerenv)
            if os.path.exists('/.dockerenv'):
                self.output_dir = "/logs"
            else:
                # Running locally - use Downloads folder
                home_dir = os.path.expanduser("~")
                downloads_dir = os.path.join(home_dir, "Downloads")
                self.output_dir = os.path.join(downloads_dir, "mqtt-logs")
        else:
            self.output_dir = output_dir
        
        # Create the output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.broker_settings = None
        self.topics = []
        self.subscribers = []
        self.load_configuration()

    def load_configuration(self):
        with open(self.settings_file) as json_file:
            config = json.load(json_file)
            self.broker_settings = BrokerSettings(
                url=config.get('BROKER_URL', 'localhost'),
                port=config.get('BROKER_PORT', 1883),
                protocol=config.get('PROTOCOL_VERSION', 4)  # mqtt.MQTTv311
            )
            broker_client_settings = self.read_client_settings(config, default=self.default_client_settings)
            
            # Extract common message metadata configuration
            self.include_message_id = config.get('INCLUDE_MESSAGE_ID', True)
            self.include_timestamp = config.get('INCLUDE_TIMESTAMP', True)
            self.metadata_field_prefix = config.get('METADATA_FIELD_PREFIX', '_')
            
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
            
            # Add message metadata configuration to payload root
            metadata_config = {
                'include_message_id': self.include_message_id,
                'include_timestamp': self.include_timestamp,
                'metadata_field_prefix': self.metadata_field_prefix
            }
            
            topic_client_settings = self.read_client_settings(topic, default=broker_client_settings)
            if topic['TYPE'] == 'single':
                # create single topic with format: /{PREFIX}
                topic_url = topic['PREFIX']
                topics.append(Topic(
                    self.broker_settings, 
                    topic_url, 
                    topic_data, 
                    topic_payload_root, 
                    topic_client_settings,
                    metadata_config
                ))
            elif topic['TYPE'] == 'multiple':
                # create multiple topics with format: /{PREFIX}/{id}
                for id in range(topic['RANGE_START'], topic['RANGE_END']+1):
                    topic_url = topic['PREFIX'] + '/' + str(id)
                    topics.append(Topic(
                        self.broker_settings, 
                        topic_url, 
                        topic_data, 
                        topic_payload_root, 
                        topic_client_settings,
                        metadata_config
                    ))
            elif topic['TYPE'] == 'list':
                # create multiple topics with format: /{PREFIX}/{item}
                for item in topic['LIST']:
                    topic_url = topic['PREFIX'] + '/' + str(item)
                    topics.append(Topic(
                        self.broker_settings, 
                        topic_url, 
                        topic_data, 
                        topic_payload_root, 
                        topic_client_settings,
                        metadata_config
                    ))
        return topics

    def load_subscribers(self, subscribers_config):
        subscribers = []
        for sub_config in subscribers_config:
            topic_pattern = sub_config['TOPIC']
            num_subscribers = sub_config.get('NUMBER', 1)
            description = sub_config.get('DESCRIPTION', '')
            users = sub_config.get('USERS')
            passwords = sub_config.get('PASSWORDS')
            
            # Create a safe topic name for file naming by replacing invalid characters
            safe_topic = topic_pattern.replace('#', 'wildcard').replace('+', 'plus').replace('/', '-')
            
            for i in range(num_subscribers):
                client_id = f"subscriber-{safe_topic}-{i}"
                log_file = os.path.join(self.output_dir, f"{client_id}.log")
                
                subscriber = SubscriberClient(
                    broker_settings=self.broker_settings,
                    client_id=client_id,
                    topic=topic_pattern,
                    data_callback=self.on_message_received,
                    log_file=log_file,
                    description=description,
                    user=users[i],
                    password=passwords[i]
                )
                subscribers.append(subscriber)
        
        return subscribers

    def on_message_received(self, client, topic, payload, timestamp):
        """Callback for when a subscriber receives a message"""
        client_id = client._client_id.decode('utf-8')
        print(f"[{timestamp}] Client {client_id} received message on topic '{topic}': {len(payload)} bytes")
        
        # Calculate latency if the message contains a timestamp
        try:
            payload_json = json.loads(payload)
            
            # Look for timestamp field (with or without prefix)
            prefixes = ['_', '']  # Try with prefix first, then without
            for prefix in prefixes:
                ts_field = f"{prefix}timestamp"
                if ts_field in payload_json:
                    send_time = payload_json[ts_field]
                    
                    # Parse the timestamp and calculate latency
                    current_time = datetime.datetime.now().timestamp() * 1000
                    latency_ms = current_time - send_time
                    
                    # Add latency information to the message
                    payload_json[f"{prefix}latency_ms"] = round(latency_ms, 2)
                    
                    # Log the latency information
                    message_id = payload_json.get(f'{prefix}message_id', 'N/A')
                    latency_info = f"Message latency: {latency_ms:.2f}ms, Message ID: {message_id}"
                    print(latency_info)
                    
                    # Add a log entry to the subscriber's log file
                    try:
                        log_file = os.path.join(self.output_dir, f"{client_id}.latency.log")
                        with open(log_file, "a", encoding="utf-8") as f:
                            # Format: timestamp, topic, message_id, send_time, receive_time, latency_ms
                            log_entry = f"{timestamp},{topic},{message_id},{send_time},{current_time},{latency_ms:.2f}\n"
                            f.write(log_entry)
                    except Exception as e:
                        print(f"Error writing to latency log: {str(e)}")
                    
                    break
        except Exception as e:
            # Failed to parse JSON or calculate latency
            print(f"Error calculating latency: {str(e)}")

    def run(self):
        print(f"Logs will be written to: {self.output_dir}")
        
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
