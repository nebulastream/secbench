import paho.mqtt.client as mqtt
import json
from datetime import datetime

MQTT_BROKER = "172.26.0.1"
#MQTT_BROKER = "192.168.0.1"
MQTT_PORT = 1882
#MQTT_PORT = 1883
MQTT_TOPIC_PROCESSED = "q1-results"  # Assuming the processed data is sent to this topic
#MQTT_TOPIC_PROCESSED = "nexmark/output"

# Function to calculate latency
def calculate_latency(message):
    #publish_timestamp = datetime.fromtimestamp(message["exampleSource$timestamp"])

    f = open("outlog_sink.txt", "a")

    #print(message["bid$timestamp"], type(message["bid$timestamp"]))

    current_timestamp = datetime.now()
    publish_timestamp = datetime.fromtimestamp(float(message["bid$timestamp"])/1000)
    #publish_timestamp = datetime.fromtimestamp(float(message["powerReadings$timestamp"])/1000)
    publish_timestamp = datetime.fromtimestamp(float(message["timestamp"])/1000)

    print('###')
    print(publish_timestamp)
    print(current_timestamp)
    print('###')
    latency = (current_timestamp - publish_timestamp).total_seconds() * 1000 # Latency in milliseconds
    f.write(str(latency))
    f.write("\n")
    f.close()
    return latency

# MQTT on_message callback
def on_message(client, userdata, msg):
    try:
        # Load message data
        data = json.loads(msg.payload)
        #print(msg.payload)

        latency = calculate_latency(data)
        #print(f"E2E event latency for message {data['bid$auctionId']}: {latency} ms")
        #print(f"E2E event latency for message {data['powerReadings$timestamp']}: {latency} ms")
        print(f"E2E event latency for message {data['auctionId']}: {latency} ms")

    except Exception as e:
        print(f"Error processing message: {e}")

# Setup MQTT client for subscribing
client = mqtt.Client()
client.on_message = on_message

# Connect and subscribe to the processed data topic
client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.subscribe(MQTT_TOPIC_PROCESSED)

# Start the loop
client.loop_forever()

