import json
import time
import os
import datetime
import paho.mqtt.client as mqtt
from topic import Topic
from data_classes import BrokerSettings, ClientSettings

class SubscriberClient:
    def __init__(self, broker_settings, client_id, topic, data_callback, log_file=None, description="", user="", password=""):
        self.broker_settings = broker_settings
        self.client_id = client_id
        self.topic = topic
        self.data_callback = data_callback
        self.description = description
        self.client = None
        self.user = user
        self.password = password
        
        # Set up logging
        self.log_file = log_file or f"{client_id}.log"
        
        # Make sure the directory exists
        log_dir = os.path.dirname(self.log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        # Create initial log file with header
        with open(self.log_file, "w", encoding="utf-8") as f:
            timestamp = self._get_timestamp_ms()
            f.write(f"=== MQTT Subscriber Log: {self.client_id} ===\n")
            f.write(f"Client ID: {self.client_id}\n")
            f.write(f"User: {self.user}\n")
            f.write(f"Password: {self.password}\n")
            f.write(f"Started: {timestamp}\n")
            f.write(f"Topic: {self.topic}\n")
            if self.description:
                f.write(f"Description: {self.description}\n")
            f.write(f"Broker: {broker_settings.url}:{broker_settings.port}\n")
            f.write("=" * 50 + "\n\n")

    def _get_timestamp_ms(self):
        """Get current timestamp with millisecond precision"""
        dt = datetime.datetime.now()
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Truncate to milliseconds

    def on_connect(self, client, userdata, flags, rc):
        timestamp = self._get_timestamp_ms()
        print(f"[{timestamp}] Connected with result code {rc}, subscribed to '{self.topic}' with user '{self.user}' and password '{self.password}'")
        
        # Subscribe to the topic upon successful connection
        self.client.subscribe(self.topic)
        
        # Log the connection event
        self._write_to_log(f"[{timestamp}] Connected with result code {rc}, subscribed to '{self.topic}' with user '{self.user}' and password '{self.password}'")

    def on_message(self, client, userdata, msg):
        # Record receive timestamp with millisecond precision immediately
        receive_timestamp = self._get_timestamp_ms()
        receive_timestamp_epoch_ms = int(datetime.datetime.now().timestamp() * 1000)
        
        # Call the callback function for central processing
        self.data_callback(client, msg.topic, msg.payload, receive_timestamp)
        
        # Log the received message
        try:
            # Try to decode as JSON for better logging
            payload_str = msg.payload.decode('utf-8')
            try:
                payload_json = json.loads(payload_str)
                
                # Calculate latency if message has a timestamp field
                latency_ms = None
                message_id = None
                
                # Look for timestamp field with different possible prefixes
                for prefix in ['_', '']:
                    ts_field = f"{prefix}timestamp"
                    id_field = f"{prefix}message_id"
                    
                    if ts_field in payload_json:
                        send_timestamp_ms = payload_json[ts_field]
                        latency_ms = receive_timestamp_epoch_ms - send_timestamp_ms
                        # Add latency to the JSON for logging
                        payload_json[f"{prefix}latency_ms"] = round(latency_ms, 2)
                        message_id = payload_json.get(id_field, "N/A")
                        break
                
                payload_formatted = json.dumps(payload_json, indent=2)
                
                # Add latency and message ID info to the log entry if available
                latency_info = f" | Latency: {latency_ms:.2f}ms | Message ID: {message_id}" if latency_ms is not None else ""
                
            except json.JSONDecodeError:
                payload_formatted = payload_str if len(payload_str) < 100 else f"{payload_str[:97]}..."
                latency_info = ""
        except UnicodeDecodeError:
            # If not text, just log the size
            payload_formatted = f"<binary data: {len(msg.payload)} bytes>"
            latency_info = ""
            
        log_entry = f"[{receive_timestamp}] Received on '{msg.topic}'{latency_info}\n{payload_formatted}\n{'-'*40}"
        self._write_to_log(log_entry)
        
        # Also write to a dedicated latency log if we have latency information
        if latency_ms is not None:
            try:
                latency_log_file = self.log_file.replace(".log", ".latency.csv")
                # Create header if file doesn't exist
                if not os.path.exists(latency_log_file):
                    with open(latency_log_file, "w", encoding="utf-8") as f:
                        f.write("timestamp,topic,message_id,send_time_ms,receive_time_ms,latency_ms\n")
                
                # Append latency data
                with open(latency_log_file, "a", encoding="utf-8") as f:
                    f.write(f"{receive_timestamp},{msg.topic},{message_id},{send_timestamp_ms},{receive_timestamp_epoch_ms},{latency_ms:.2f}\n")
            except Exception as e:
                print(f"Error writing to latency log: {str(e)}")

    def _write_to_log(self, message):
        """Write a message to this subscriber's log file"""
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(f"{message}\n")

    def connect(self):
        clean_session = None if self.broker_settings.protocol == mqtt.MQTTv5 else True
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id=self.client_id, protocol=self.broker_settings.protocol, clean_session=clean_session)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
        timestamp = self._get_timestamp_ms()
        self._write_to_log(f"[{timestamp}] Attempting connection to {self.broker_settings.url}:{self.broker_settings.port} with client ID '{self.client_id}' and user '{self.user}' and password '{self.password}'")
        self.client.username_pw_set(self.user, self.password)
        self.client.connect(self.broker_settings.url, self.broker_settings.port, 60)
        self.client.loop_start()

    def disconnect(self):
        if self.client:
            timestamp = self._get_timestamp_ms()
            self._write_to_log(f"[{timestamp}] Disconnecting from broker")
            self.client.loop_stop()
            self.client.disconnect()