import paho.mqtt.client as mqtt
import csv
from datetime import datetime
from io import StringIO

MQTT_BROKER = "172.26.0.1"
#MQTT_BROKER = "192.168.0.1"
MQTT_PORT = 1882
MQTT_TOPIC_PROCESSED = "nexmark/output"
#MQTT_TOPIC_PROCESSED = "q1-results"
MQTT_TOPIC_PROCESSED = "bid"

# Update this to match the exact CSV columns you publish
FIELDNAMES = ["auctionId", "cart", "cort", "curt", "price", "timestamp"]
#FIELDNAMES = ["timestamp","count"]

# Function to calculate latency
def calculate_latency(message):
    with open("outlog_sink.txt", "a") as f:
        # parse publish timestamp in ms
        publish_ts_ms = float(message["timestamp"])
        publish_dt = datetime.fromtimestamp(publish_ts_ms / 1000.0)
        now = datetime.now()
        print("###")
        print(f"now: {now}")
        print(f"last: {publish_dt}")
        print("###")

        latency_ms = (now - publish_dt).total_seconds() * 1000
        f.write(f"{latency_ms}\n")

    return latency_ms

# MQTT on_message callback
def on_message(client, userdata, msg):
    try:
        # Decode payload to text
        text = msg.payload.decode("utf-8").strip()
        # Use csv.DictReader over a single line
        reader = csv.DictReader(StringIO(text), fieldnames=FIELDNAMES)
        data = next(reader)

        # Optionally, you can log the parsed values:
        # print(f"Parsed CSV: {data}")

        latency = calculate_latency(data)
        print(f"E2E event latency for auction {data['auctionId']}: {latency:.2f} ms")
        #print(f"E2E event latency for auction {data['timestamp']}: {latency:.2f} ms")

    except Exception as e:
        print(f"Error processing message: {e}")

# Setup MQTT client for subscribing
client = mqtt.Client()
client.on_message = on_message

client.connect(MQTT_BROKER, MQTT_PORT, 60)
client.subscribe(MQTT_TOPIC_PROCESSED)

client.loop_forever()
