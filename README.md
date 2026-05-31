# City Traffic Monitoring & Alert System

A real-time traffic monitoring and alert system that pulls live camera feeds, processes them using computer vision, and displays the results on an interactive web dashboard with voice alerts.

## Architecture
1. **Producer (`producer.py`)**: Polls traffic camera images from the Singapore LTA API (`api.data.gov.sg`) and publishes them to a Kafka topic.
2. **Processor (`flink_processor.py`)**: A PyFlink stream processing application that consumes camera images from Kafka. It uses YOLO (with SAHI for sliced inference) to detect and count vehicles (cars, motorcycles, buses, trucks) and calculates congestion severity based on vehicle counts and bounding box overlaps.
3. **Dashboard (`dashboard/app.py`)**: A Flask + Socket.IO web application that consumes processed alerts from Kafka and pushes them to the browser in real-time. It features a Leaflet map, a severity-sorted alert feed, live camera image overlays, and Text-to-Speech (TTS) voice alerts.

## Requirements
- Python 3.x
- Docker & Docker Compose (for Kafka)
- Node.js (optional, for frontend tooling if added later)

## Ramp-up Instructions

### 1. Set up the Environment
Create a virtual environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Start Kafka
The project uses Kafka for message brokering. Start the Kafka broker using Docker Compose:
```bash
docker-compose up -d
```

### 3. Run the Services
You need to run the three main components in separate terminal windows (ensure your virtual environment is activated in each).

**Start the Camera Producer:**
```bash
python ./producer.py
```

**Start the Flink Processor:**
```bash
python ./flink_processor.py
```

**Start the Dashboard:**
```bash
python ./dashboard/app.py
```

### 4. View the Dashboard
Open your web browser and navigate to:
```
http://127.0.0.1:5000
```
You can toggle "Enable Voice Alerts" in the sidebar to hear TTS announcements for heavy traffic and severe congestion.
