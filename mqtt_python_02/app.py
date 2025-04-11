import paho.mqtt.client as mqtt
import json
import time
import threading

# MQTT Broker Configuration
BROKER = "localhost"  # Change to IP or hostname of MQTT Broker
PORT = 1883           # Port of MQTT Broker (default is 1883)
TOPIC_PREFIX = "iot_sensors/iot_got1"  # Prefix for topic1
NUM_TOPICS = 100       # Number of topics for topic1
PUBLISH_INTERVAL = 3   # Seconds for topic1

TOPIC_PREFIX2 = "iot_sensors/device_alive/got1_"  # Prefix for topic2
NUM_TOPICS2 = 100       # Number of topics for topic2
PUBLISH_INTERVAL2 = 30   # Seconds for topic2

count = 0

# Payload Templates
def generate_payload(topic_id, count):
    return {
        "master": f"data_{topic_id}",
        "data_id": count,
        "master_id": "test",
        "data_4": 1000000,
        "data_5": 1000000,
        "data_6": 1000000,
        "data_7": 1000000,
        "data_8": 1000000,
        "data_9": 1000000,
        "data_11": 1000000.058,
    }

def generate_payload2(topic_id, count):
    return {
        "master": f"data_{topic_id}",
        "data_id": count,
        "master_id": "test",
        "cpu": 1000000,
        "ram": 1000000,
    }

# Publish Messages to MQTT Broker
def publish_topic(client, topic_id, count):
    topic = f"{TOPIC_PREFIX}/mc_{topic_id}"
    payload = generate_payload(topic_id, count)
    print(payload)
    client.publish(topic, json.dumps(payload))
    print(f"Published to {topic}: {payload}")

def publish_topic2(client, topic_id, count):
    topic2 = f"{TOPIC_PREFIX2}mc_{topic_id}"
    payload = generate_payload2(topic_id, count)
    print(payload)
    client.publish(topic2, json.dumps(payload))
    print(f"Published to {topic2}: {payload}")

# Thread for publishing topic1 messages continuously
def publish_all_topics(client):
    global count
    while True:
        count += 1
        for topic_id in range(1, NUM_TOPICS + 1):
            publish_topic(client, topic_id, count)
        time.sleep(PUBLISH_INTERVAL)

# Thread for publishing topic2 messages continuously
def publish_all_topics2(client):
    global count
    while True:
        for topic_id in range(1, NUM_TOPICS2 + 1):
            publish_topic2(client, topic_id, count)
        time.sleep(PUBLISH_INTERVAL2)

# Main Function
def main():
    # Create MQTT Client
    client = mqtt.Client()
    client.connect(BROKER, PORT)
    client.loop_start()

    # Start publishing topic1 and topic2 in separate threads
    threading.Thread(target=publish_all_topics, args=(client,), daemon=True).start()
    threading.Thread(target=publish_all_topics2, args=(client,), daemon=True).start()

    try:
        while True:
            time.sleep(1)  # Keep the main thread alive
    except KeyboardInterrupt:
        print("Stopping publisher...")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()