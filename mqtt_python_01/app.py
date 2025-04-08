import paho.mqtt.client as mqtt
import json
import time
import threading

# MQTT Broker Configuration
BROKER = "localhost"  # เปลี่ยนเป็น IP หรือ hostname ของ MQTT Broker
PORT = 1883           # พอร์ตของ MQTT Broker (ค่าเริ่มต้นคือ 1883)
TOPIC_PREFIX = "iot_sensors/test_data"  # Prefix สำหรับ topic
NUM_TOPICS = 200       # จำนวน topics
PUBLISH_INTERVAL = 1   # วินาที

count = 0
# Payload Template
def generate_payload(topic_id,count):
    return {
        "master": f"data_{topic_id}",
        "data_2": count,
        "data_3": 1000000,
        "data_4": 1000000,
        "data_5": 1000000,
        "data_6": 1000000,
        "data_7": 1000000,
        "data_8": 1000000,
        "data_9": 1000000,
        "data_10": 1000000,
    }

# Publish Messages to MQTT Broker
def publish_topic(client, topic_id,count):
    topic = f"{TOPIC_PREFIX}/mc_{topic_id}"
    payload = generate_payload(topic_id,count)
    print(payload)
    client.publish(topic, json.dumps(payload))
    print(f"Published to {topic}: {payload}")

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
