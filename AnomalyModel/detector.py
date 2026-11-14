import cv2
import json
import base64
import math
import os
from collections import defaultdict
from ultralytics import YOLO
from kafka import KafkaProducer, KafkaConsumer

model = YOLO('yolov8s.pt')

DANGEROUS_CLASSES = {43: "KNIFE", 34: "BASEBALL_BAT", 76: "SCISSORS"}
track_history = defaultdict(list)
HISTORY_LENGTH = 5
CROWD_THRESHOLD = 3

# Producent Kafka (wysyła alerty)
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    KAFKA_AVAILABLE = True
    print("Połączono z Kafką (Producent).")
except Exception as e:
    KAFKA_AVAILABLE = False
    print("Błąd producenta Kafka:", e)


def detect_fall(bbox, frame_height, is_video_tracked=False):
    x1, y1, x2, y2 = bbox
    width = x2 - x1
    height = y2 - y1

    if is_video_tracked:
        if y2 > frame_height * 0.96 or height < frame_height * 0.12:
            return False
        return width > (height * 1.45)
    return width > (height * 1.2)


def process_frame(frame, camera_id="CAM_01", is_video=True):
    frame_h, frame_w = frame.shape[:2]
    results = model.track(frame, persist=True, tracker="bytetrack.yaml", verbose=False) if is_video else model(frame,
                                                                                                               verbose=False)

    alerts = []
    running_people_count = 0

    for result in results:
        boxes = result.boxes
        if len(boxes) == 0:
            continue

        track_ids = boxes.id.int().cpu().tolist() if boxes.id is not None else [None] * len(boxes)

        for box, track_id in zip(boxes, track_ids):
            cls_id = int(box.cls[0])
            coords = box.xyxy[0].tolist()
            confidence = float(box.conf[0])

            if confidence < 0.35:
                continue

            if cls_id == 0:  # Osoba
                is_running = False
                is_video_tracked = False

                if track_id is not None:
                    x1, y1, x2, y2 = coords
                    cx, cy = (x1 + x2) / 2, (y1 + y2) / 2
                    person_height = y2 - y1

                    track = track_history[track_id]
                    track.append((cx, cy))
                    if len(track) > HISTORY_LENGTH:
                        track.pop(0)
                    if len(track) >= 2:
                        is_video_tracked = True
                    if len(track) == HISTORY_LENGTH:
                        distance = math.hypot(cx - track[0][0], cy - track[0][1])
                        if distance > max(10.0, person_height * 0.12):
                            is_running = True
                            running_people_count += 1

                if not is_running and detect_fall(coords, frame_h, is_video_tracked):
                    alerts.append({"type": "FALL_DETECTED", "confidence": confidence, "coords": coords})

            elif cls_id in DANGEROUS_CLASSES:
                alerts.append({"type": "DANGEROUS_ITEM", "item": DANGEROUS_CLASSES[cls_id], "confidence": confidence,
                               "coords": coords})

    if running_people_count >= CROWD_THRESHOLD:
        alerts.append({"type": "CROWD_RUNNING", "running_count": running_people_count, "confidence": 1.0, "coords": []})

    # OZNACZANIE FILMÓW (Rysowanie ramek na klatce wideo)
    for alert in alerts:
        if alert.get("coords"):
            x1, y1, x2, y2 = map(int, alert["coords"])
            label = f"{alert['type']} ({alert['confidence']:.2f})"
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 3)
            cv2.putText(frame, label, (x1, max(20, y1 - 10)), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)

    # Wysłanie powiadomienia do Kafki
    if alerts and KAFKA_AVAILABLE:
        _, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 70])
        payload = {"camera_id": camera_id, "alerts": alerts, "frame_data": base64.b64encode(buffer).decode('utf-8')}
        producer.send('security_events', payload)
        producer.flush()

    return alerts, frame


def process_video_file(file_path, session_id):
    print(f"Rozpoczęto analizę wideo: {file_path}")
    cap = cv2.VideoCapture(file_path)
    if not cap.isOpened():
        return

    frame_width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(cap.get(cv2.CAP_PROP_FPS)) or 30

    # Ścieżka dla oznakowanego pliku wideo
    out_path = file_path.replace(".mp4", "_alert.mp4")
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(out_path, fourcc, fps, (frame_width, frame_height))

    event_detected = False

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        alerts, annotated_frame = process_frame(frame, camera_id=session_id, is_video=True)
        if alerts:
            event_detected = True

        out.write(annotated_frame)

    cap.release()
    out.release()

    # Jeśli film nie zawierał zagrożeń, usuwamy wygenerowany plik z ramkami, żeby oszczędzić dysk
    if not event_detected:
        os.remove(out_path)
    else:
        print(f"Zapisano oznaczony film ze zdarzeniem: {out_path}")


# Nasłuchiwanie na nowe pliki od Spring Boot
def start_kafka_consumer():
    if not KAFKA_AVAILABLE:
        print("Brak połączenia z Kafką. Analiza zatrzymana.")
        return

    consumer = KafkaConsumer(
        'video_uploads',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    print("Nasłuchiwanie na nowe filmy na topiku 'video_uploads'...")

    for message in consumer:
        data = message.value
        file_path = data.get('file_path')
        session_id = data.get('session_id')

        if file_path and os.path.exists(file_path):
            process_video_file(file_path, session_id)


if __name__ == "__main__":
    start_kafka_consumer()