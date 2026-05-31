import base64
import json
import logging
import os
import subprocess
import sys
import time

import cv2
import numpy as np
from confluent_kafka import Consumer, Producer, KafkaError
from sahi import AutoDetectionModel
from sahi.predict import get_sliced_prediction

from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import MapFunction, RuntimeContext

#Configuration
KAFKA_BROKER = "localhost:29092"
INPUT_TOPIC = "traffic-feed"
OUTPUT_TOPIC = "traffic-alerts"
CONSUMER_GROUP = "flink-traffic-processor-v2"

# Vehicle COCO class IDs
VEHICLE_CLASSES = {
    1: "motorcycle", # YOLO often confuses motorcycles with bicycles at night
    2: "car",
    3: "motorcycle",
    5: "bus",
    7: "truck",
}

# Per-class confidence thresholds — lower for motorcycles since they're small
# and hard to detect in low-res traffic cam images
CONFIDENCE_THRESHOLDS = {
    "motorcycle": 0.15,
    "car":        0.25,
    "bus":        0.25,
    "truck":      0.25,
}
MODEL_CONFIDENCE = 0.15  # Global SAHI threshold (lowest per-class value)

# Heuristic thresholds
THRESHOLD_MODERATE = 10
THRESHOLD_HEAVY = 20
THRESHOLD_SEVERE = 30
OVERLAP_HEAVY = 0.2
OVERLAP_SEVERE = 0.3

# Batch settings
BATCH_SIZE = 10
BATCH_TIMEOUT = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("FlinkProcessor")


#YOLO Processing Map Function
class VehicleDetectionFunction(MapFunction):

    #Loading YOLOv26x model with SAHI
    def open(self, runtime_context: RuntimeContext):
        print("Loading YOLOv26x model with SAHI...", flush=True)
        self.detection_model = AutoDetectionModel.from_pretrained(
            model_type="ultralytics",
            model_path="yolo26x.pt",
            confidence_threshold=MODEL_CONFIDENCE,
            device="cuda:0",
        )
        print("YOLOv26x + SAHI loaded successfully", flush=True)

    def map(self, value: str) -> str:
        try:
            data = json.loads(value)
            camera_id = data["camera_id"]
            timestamp = data["timestamp"]
            location = data.get("location", {})
            image_b64 = data["image_b64"]

            #Decode Base64 -> image
            image_bytes = base64.b64decode(image_b64)
            nparr = np.frombuffer(image_bytes, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            if image is None:
                print(f"Camera {camera_id}: failed to decode image", flush=True)
                return json.dumps({"error": "decode_failed", "camera_id": camera_id})

            #Run SAHI sliced inference
            result = get_sliced_prediction(
                image,
                self.detection_model,
                slice_height=320,
                slice_width=320,
                overlap_height_ratio=0.2,
                overlap_width_ratio=0.2,
                verbose=0,
            )

            #Count vehicles by class
            counts = {name: 0 for name in set(VEHICLE_CLASSES.values())}
            bboxes = []

            for pred in result.object_prediction_list:
                cls_id = pred.category.id
                if cls_id in VEHICLE_CLASSES:
                    label = VEHICLE_CLASSES[cls_id]
                    conf = pred.score.value
                    # Apply per-class confidence threshold
                    if conf < CONFIDENCE_THRESHOLDS.get(label, 0.25):
                        continue
                    counts[label] += 1
                    bbox = pred.bbox.to_xyxy()
                    bboxes.append(bbox)

            total_vehicles = sum(counts.values())

            #Compute overlap score
            overlap_score = self._compute_overlap_score(bboxes)

            #Apply heuristic classification
            status = self._classify_traffic(total_vehicles, overlap_score)

            #Generate annotated image with bounding boxes
            annotated_img = image.copy()
            for pred in result.object_prediction_list:
                cls_id = pred.category.id
                if cls_id in VEHICLE_CLASSES:
                    label = VEHICLE_CLASSES[cls_id]
                    conf = pred.score.value
                    #Same per-class filter as counting
                    if conf < CONFIDENCE_THRESHOLDS.get(label, 0.25):
                        continue
                    bbox = pred.bbox.to_xyxy()
                    x1, y1, x2, y2 = [int(c) for c in bbox]
                    color = (0, 255, 255)
                    cv2.rectangle(annotated_img, (x1, y1), (x2, y2), color, 2)
                    cv2.putText(annotated_img, f"{label} {conf:.2f}", (x1, y1 - 5),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

            _, buffer = cv2.imencode('.jpg', annotated_img)
            annotated_b64 = base64.b64encode(buffer).decode('utf-8')

            #Build alert payload
            alert = {
                "camera_id": camera_id,
                "timestamp": timestamp,
                "location": location,
                "vehicle_count": total_vehicles,
                "car_count": counts.get("car", 0),
                "motorcycle_count": counts.get("motorcycle", 0),
                "bus_count": counts.get("bus", 0),
                "truck_count": counts.get("truck", 0),
                "overlap_score": round(overlap_score, 3),
                "status": status,
                "processed_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "image_b64": annotated_b64
            }

            print(
                f"Camera {camera_id}: {total_vehicles} vehicles "
                f"(car={counts['car']} moto={counts['motorcycle']} "
                f"bus={counts['bus']} truck={counts['truck']}) "
                f"overlap={overlap_score:.3f} -> {status}",
                flush=True,
            )

            return json.dumps(alert)

        except Exception as e:
            print(f"Processing error: {e}", flush=True)
            import traceback
            traceback.print_exc()
            return json.dumps({"error": str(e)})

    #Compute overlap score
    def _compute_overlap_score(self, bboxes):
        """Compute average IoU overlap between all bounding box pairs."""
        if len(bboxes) < 2:
            return 0.0

        overlap_count = 0
        pair_count = 0

        for i in range(len(bboxes)):
            for j in range(i + 1, len(bboxes)):
                pair_count += 1
                iou = self._compute_iou(bboxes[i], bboxes[j])
                if iou > 0.05:
                    overlap_count += 1

        return overlap_count / pair_count if pair_count > 0 else 0.0

    #Compute iou (Intersection over Union) of two bounding boxes
    @staticmethod
    def _compute_iou(box1, box2):
        """Compute IoU for two bounding boxes [x1, y1, x2, y2]."""
        x1 = max(box1[0], box2[0])
        y1 = max(box1[1], box2[1])
        x2 = min(box1[2], box2[2])
        y2 = min(box1[3], box2[3])

        intersection = max(0, x2 - x1) * max(0, y2 - y1)
        area1 = (box1[2] - box1[0]) * (box1[3] - box1[1])
        area2 = (box2[2] - box2[0]) * (box2[3] - box2[1])
        union = area1 + area2 - intersection

        return intersection / union if union > 0 else 0.0

    #Classify traffic based on vehicle count and overlap score
    @staticmethod
    def _classify_traffic(total_vehicles, overlap_score):
        """Apply heuristic rules to classify traffic status."""
        if total_vehicles > THRESHOLD_SEVERE or (
            total_vehicles > 20 and overlap_score > OVERLAP_SEVERE
        ):
            return "Severe Congestion"
        elif total_vehicles > THRESHOLD_HEAVY or (
            total_vehicles > 15 and overlap_score > OVERLAP_HEAVY
        ):
            return "Heavy Traffic"
        elif total_vehicles > THRESHOLD_MODERATE:
            return "Moderate Traffic"
        else:
            return "Normal Flow"


#Kafka helpers
def consume_kafka_batch():
    #Creates Kafka consumer and subscribes to the input topic
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "session.timeout.ms": 6000,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([INPUT_TOPIC])
    log.info(f"Waiting for messages on '{INPUT_TOPIC}'...")

    messages = []
    deadline = time.time() + BATCH_TIMEOUT

    #Polls Kafka for a batch of messages
    while len(messages) < BATCH_SIZE and time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.error(f"Kafka error: {msg.error()}")
            continue

        #Only accept valid JSON messages with required fields
        try:
            raw = msg.value().decode("utf-8")
            data = json.loads(raw)
            if "camera_id" in data and "image_b64" in data:
                messages.append(raw)
            else:
                log.warning(f"Skipping message without required fields")
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.warning(f"Skipping invalid message: {e}")

    consumer.close()
    return messages

#Publishes processed alerts to the output topic
def publish_alerts(alert_jsons):
    #Creates Kafka producer and publishes processed alerts
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "client.id": "flink-alert-producer",
    })

    count = 0
    for alert_json in alert_jsons:
        try:
            alert = json.loads(alert_json)
            if "error" in alert:
                continue
            producer.produce(
                topic=OUTPUT_TOPIC,
                key=alert.get("camera_id", "unknown"),
                value=alert_json,
            )
            count += 1
        except Exception as e:
            log.error(f"Failed to publish alert: {e}")

    producer.flush(timeout=10)
    return count


#Main loop
def main():
    #Logs pipeline information
    log.info("=" * 60)
    log.info("  PyFlink Traffic Stream Processor")
    log.info(f"  Input:  {INPUT_TOPIC} @ {KAFKA_BROKER}")
    log.info(f"  Output: {OUTPUT_TOPIC} @ {KAFKA_BROKER}")
    log.info(f"  Batch:  up to {BATCH_SIZE} msgs, {BATCH_TIMEOUT}s timeout")
    log.info("=" * 60)

    #Consume a single batch of messages from Kafka
    messages = consume_kafka_batch()

    #If no messages received, exit
    if not messages:
        log.info("No messages received.")
        return

    log.info(f"Received {len(messages)} valid messages")

    #Step 2: Process through PyFlink
    log.info("Starting Flink pipeline...")
    
    from pyflink.common import Configuration
    config = Configuration()

    config.set_string("heartbeat.timeout", "300000") 
    config.set_string("heartbeat.interval", "60000")
    config.set_string("akka.ask.timeout", "300 s")
    config.set_string("web.timeout", "300000")
    
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(1)

    ds = env.from_collection(messages, type_info=Types.STRING())
    alerts = ds.map(VehicleDetectionFunction(), output_type=Types.STRING())

    #Collect results
    results = []
    with alerts.execute_and_collect() as result_iter:
        for alert_json in result_iter:
            results.append(alert_json)

    #Publish alerts to Kafka
    published = publish_alerts(results)
    log.info(f"Processing complete: {len(results)} processed, {published} published to '{OUTPUT_TOPIC}'")

    #Print summary
    for r in results:
        try:
            alert = json.loads(r)
            if "error" not in alert:
                log.info(
                    f"  Camera {alert['camera_id']}: "
                    f"{alert['vehicle_count']} vehicles -> {alert['status']}"
                )
        except json.JSONDecodeError:
            pass


if __name__ == "__main__":
    #If launched with --single-batch, process one batch and exit.
    #Otherwise, loop continuously by re-spawning as a clean subprocess
    #to avoid PyFlink's gRPC multiplexer memory leak.
    if "--single-batch" in sys.argv:
        main()
    else:
        LOOP_DELAY = 2 
        log.info("Starting continuous processor loop (Ctrl+C to stop)...")
        while True:
            try:
                subprocess.run(
                    [sys.executable, __file__, "--single-batch"],
                    check=False,
                )
            except KeyboardInterrupt:
                log.info("Processor stopped.")
                break
            time.sleep(LOOP_DELAY)
