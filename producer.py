
import base64
import json
import signal
import time
from datetime import datetime

import requests
from confluent_kafka import Producer

#Configuration
KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC = "traffic-feed"
API_URL = "https://api.data.gov.sg/v1/transport/traffic-images"
POLL_INTERVAL = 60         #seconds between API polls
MAX_CAMERAS = 10           #number of cameras to process per poll
REQUEST_TIMEOUT = 15       #seconds for HTTP requests

#Target traffic bottlenecks in Singapore
HOTSPOT_CAMERAS = {
    "2701",  # Woodlands Causeway (Towards Johor)
    "2702",  # Woodlands Checkpoint (Towards BKE)
    "4703",  # Tuas Second Link
    "4713",  # Tuas Checkpoint
    "1701",  # CTE: Moulmein Flyover
    "1705",  # CTE: Ang Mo Kio Ave 5 Flyover
    "8701",  # KPE: KPE/ECP
    "9701",  # MCE: Marina Coastal Expressway
    "3704",  # AYE: Entrance from Benoi Rd
    "1111",  # TPE: Tampines Expressway
}

#Kafka Producer Setup
producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "traffic-camera-producer",
    "acks": "all",
    "linger.ms": 50,
    "batch.num.messages": 10,
}

producer = Producer(producer_conf)
session = requests.Session()

#Shutdown
running = True

def shutdown_handler(signum, frame):
    global running
    print("\n[Producer] Shutting down...")
    running = False

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

#Delivery Callback
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for camera {msg.key().decode()}: {err}")
    else:
        print(f"Delivered camera {msg.key().decode()} → "
              f"partition {msg.partition()} offset {msg.offset()}")


#Core Logic
def fetch_camera_data():
    #Fetch the latest camera snapshot metadata from the API.
    resp = session.get(API_URL, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    cameras = data["items"][0]["cameras"]
    timestamp = data["items"][0]["timestamp"]

    return cameras, timestamp

#Download image
def download_image(image_url):
    #Download an image and return its bytes, or None on failure.
    try:
        resp = session.get(image_url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as e:
        print(f"  ⚠ Failed to download {image_url}: {e}")
        return None

#Poll and publish
def poll_and_publish():
    #Single poll cycle: fetch cameras, download images, publish to Kafka.
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Polling API...")

    try:
        cameras, timestamp = fetch_camera_data()
    except requests.RequestException as e:
        print(f"API request failed: {e}")
        return 0

    #Filter to hotspot cameras only and sort deterministically
    cameras = [c for c in cameras if str(c.get("camera_id")) in HOTSPOT_CAMERAS]
    cameras = sorted(cameras, key=lambda c: str(c.get("camera_id", "")))
    if MAX_CAMERAS:
        cameras = cameras[:MAX_CAMERAS]

    published = 0
    for cam in cameras:
        camera_id = cam["camera_id"]
        image_url = cam["image"]
        location = cam.get("location", {})

        #Download the image
        image_bytes = download_image(image_url)
        if image_bytes is None:
            continue

        #Build the message payload
        payload = {
            "camera_id": camera_id,
            "timestamp": timestamp,
            "location": {
                "latitude": location.get("latitude"),
                "longitude": location.get("longitude"),
            },
            "image_b64": base64.b64encode(image_bytes).decode("utf-8"),
            "image_width": cam.get("image_metadata", {}).get("width"),
            "image_height": cam.get("image_metadata", {}).get("height"),
        }

        #Publish to Kafka
        producer.produce(
            topic=KAFKA_TOPIC,
            key=camera_id,
            value=json.dumps(payload),
            callback=delivery_report,
        )
        published += 1

    #Flush to ensure all messages are delivered
    producer.flush(timeout=10)
    print(f"Published {published}/{len(cameras)} camera snapshots")
    return published


#Main Loop
def main():
    print("=" * 60)
    print("Traffic Camera Producer")
    print(f"Kafka: {KAFKA_BROKER} | Topic: {KAFKA_TOPIC}")
    print(f"API: {API_URL}")
    print(f"Poll interval: {POLL_INTERVAL}s | Max cameras: {MAX_CAMERAS}")
    print("=" * 60)

    cycle = 0
    while running:
        cycle += 1
        print(f"\n── Cycle {cycle} ──")
        poll_and_publish()

        #Wait for next poll, checking for shutdown every second
        for _ in range(POLL_INTERVAL):
            if not running:
                break
            time.sleep(1)

    #Final flush on shutdown
    remaining = producer.flush(timeout=5)
    if remaining > 0:
        print(f"Warning: {remaining} messages still in queue at shutdown")
    print("Stopped.")


if __name__ == "__main__":
    main()
