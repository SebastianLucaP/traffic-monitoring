import asyncio
import io
import json
import logging
import threading
from flask import Flask, render_template, request, send_file
from flask_socketio import SocketIO
from confluent_kafka import Consumer, KafkaError
import edge_tts

#TTS voice — Microsoft Neural voice (high-quality, natural sounding)
TTS_VOICE = "en-US-GuyNeural"

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

#Configuration
KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC = "traffic-alerts"
CONSUMER_GROUP = "dashboard-consumer"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("Dashboard")

#Store the latest alert per camera
latest_alerts = {}

def kafka_consumer_thread():
    #Background thread to consume alerts from Kafka and push to WebSockets
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info(f"Dashboard connected to Kafka topic '{KAFKA_TOPIC}'")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error(f"Kafka error: {msg.error()}")
            continue

        try:
            alert_json = msg.value().decode("utf-8")
            alert = json.loads(alert_json)
            
            #Update cache
            camera_id = alert.get("camera_id")
            if camera_id:
                latest_alerts[camera_id] = alert
            
            #Push to all connected browsers
            socketio.emit('new_alert', alert)
            log.info(f"Pushed alert for camera {camera_id} to frontend")
        except Exception as e:
            log.error(f"Error processing alert: {e}")

@app.route("/")
def index():
    #Pass initial state on page load
    return render_template("index.html", initial_alerts=latest_alerts)

@app.route("/api/tts", methods=["POST"])
def tts():
    #Generate TTS audio using Microsoft Neural voices via edge-tts
    data = request.get_json(silent=True) or {}
    text = data.get("text", "")
    if not text:
        return "No text provided", 400

    try:
        #edge-tts is async, run it in a one-shot event loop
        audio_buffer = io.BytesIO()

        async def generate():
            communicate = edge_tts.Communicate(text, TTS_VOICE, rate="+10%")
            async for chunk in communicate.stream():
                if chunk["type"] == "audio":
                    audio_buffer.write(chunk["data"])

        asyncio.run(generate())
        audio_buffer.seek(0)
        return send_file(audio_buffer, mimetype="audio/mpeg")
    except Exception as e:
        log.error(f"TTS generation failed: {e}")
        return str(e), 500

if __name__ == "__main__":
    #Start Kafka consumer thread
    thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    thread.start()
    
    log.info("Starting Dashboard server on port 5000...")
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)
