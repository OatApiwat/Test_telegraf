import paho.mqtt.client as mqtt
import json
import time
import threading

# MQTT Broker Configuration
BROKER = "localhost"  # เปลี่ยนเป็น IP หรือ hostname ของ MQTT Broker
PORT = 1883           # พอร์ตของ MQTT Broker (ค่าเริ่มต้นคือ 1883)
TOPIC_PREFIX = "iot_sensors/iot_got1"  # Prefix สำหรับ topic
NUM_TOPICS = 200       # จำนวน topics
PUBLISH_INTERVAL = 3   # วินาที

TOPIC_PREFIX2 = "iot_sensors/device_log/got_"  # Prefix สำหรับ topic
NUM_TOPICS2 = 200       # จำนวน topics
PUBLISH_INTERVAL2 = 30   # วินาที

count = 0
# Payload Template
def generate_payload(topic_id,count):
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
def generate_payload2(topic_id2,count):
    return {
        "master": f"data_{topic_id2}",
        "data_id": count,
        "master_id": "test",
        "cpu": 1000000,
        "ram": 1000000,
    }

# Publish Messages to MQTT Broker
def publish_topic(client, topic_id,count):
    topic = f"{TOPIC_PREFIX}/mc_{topic_id}"
    payload = generate_payload(topic_id,count)
    print(payload)
    client.publish(topic, json.dumps(payload))
    print(f"Published to {topic}: {payload}")

def publish_topic2(client, topic_id,count):
    topic2 = f"{TOPIC_PREFIX2}/mc_{topic_id}"
    payload = generate_payload2(topic_id,count)
    print(payload)
    client.publish(topic2, json.dumps(payload))
    print(f"Published to {topic2}: {payload}")

# Thread for publishing messages continuously
def publish_all_topics(client):
    global count
    while True:
        count += 1
        for topic_id in range(1, NUM_TOPICS + 1):
            publish_topic(client, topic_id,count)
        time.sleep(PUBLISH_INTERVAL)

# Main Function
def main():
    # Create MQTT Client
    client = mqtt.Client()
    client.connect(BROKER, PORT)
    client.loop_start()

    # Start publishing in a separate thread
    threading.Thread(target=publish_all_topics, args=(client,), daemon=True).start()

    try:
        while True:
            time.sleep(1)  # Keep the main thread alive
    except KeyboardInterrupt:
        print("Stopping publisher...")
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()
