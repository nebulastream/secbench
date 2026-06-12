import paho.mqtt.client as mqtt
from datetime import datetime
import threading

MQTT_BROKER = "172.20.0.1"
MQTT_PORT = 1882
MQTT_TOPIC_PROCESSED = "nexmark/output"

# Globals for throughput measurement
msg_count = 0
throughput_start = datetime.now()
lock = threading.Lock()
LOG_FILE = "throughput_sink.txt"

def record_throughput():
    """
    Every second, compute, print, and log throughput.
    """
    global msg_count, throughput_start

    # Schedule next run
    threading.Timer(1.0, record_throughput).start()

    with lock:
        now = datetime.now()
        elapsed = (now - throughput_start).total_seconds()
        if elapsed > 0:
            tput = msg_count / elapsed
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
            line = f"{timestamp}, {tput:.2f}\n"

            # Print to console
            print(f"[{now.strftime('%H:%M:%S')}] Throughput: {tput:.2f} tuples/sec")

            # Append to log file
            with open(LOG_FILE, "a") as f:
                f.write(line)

            # Reset for next interval
            msg_count = 0
            throughput_start = now

def on_message(client, userdata, msg):
    global msg_count
    with lock:
        msg_count += 1

if __name__ == "__main__":
    # Initialize (create/clear) log file
    with open(LOG_FILE, "w") as f:
        f.write("timestamp, throughput_tuples_per_sec\n")

    # Start periodic logging
    record_throughput()

    client = mqtt.Client()
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC_PROCESSED)
    client.loop_forever()
