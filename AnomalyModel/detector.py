import cv2
import json
import base64
import math
from collections import defaultdict
from ultralytics import YOLO
from kafka import KafkaProducer

model = YOLO('yolov8s.pt')

DANGEROUS_CLASSES = {43: "KNIFE", 34: "BASEBALL_BAT", 76: "SCISSORS"}

track_history = defaultdict(list)
HISTORY_LENGTH = 5
CROWD_THRESHOLD = 3

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    KAFKA_AVAILABLE = True
except Exception as e:
    KAFKA_AVAILABLE = False


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

    if is_video:
        results = model.track(frame, persist=True, tracker="bytetrack.yaml", verbose=False)
    else:
        results = model(frame, verbose=False)

    alerts = []
    running_people_count = 0

    for result in results:
        boxes = result.boxes
        if len(boxes) == 0:
            continue

        if boxes.id is not None:
            track_ids = boxes.id.int().cpu().tolist()
        else:
            track_ids = [None] * len(boxes)

        for box, track_id in zip(boxes, track_ids):
            cls_id = int(box.cls[0])
            coords = box.xyxy[0].tolist()
            confidence = float(box.conf[0])

            if confidence < 0.35:
                continue

            if cls_id == 0:
                is_running = False
                is_video_tracked = False

                if track_id is not None:
                    x1, y1, x2, y2 = coords
                    cx = (x1 + x2) / 2
                    cy = (y1 + y2) / 2
                    person_height = y2 - y1

                    track = track_history[track_id]
                    track.append((cx, cy))

                    if len(track) > HISTORY_LENGTH:
                        track.pop(0)

                    if len(track) >= 2:
                        is_video_tracked = True

                    if len(track) == HISTORY_LENGTH:
                        start_cx, start_cy = track[0]
                        distance = math.hypot(cx - start_cx, cy - start_cy)

                        dynamic_speed_threshold = max(10.0, person_height * 0.12)

                        if distance > dynamic_speed_threshold:
                            is_running = True
                            running_people_count += 1

                if not is_running and detect_fall(coords, frame_h, is_video_tracked):
                    alerts.append({"type": "FALL_DETECTED", "confidence": confidence, "coords": coords})

            elif cls_id in DANGEROUS_CLASSES:
                item_name = DANGEROUS_CLASSES[cls_id]
                alerts.append({
                    "type": "DANGEROUS_ITEM_DETECTED",
                    "item": item_name,
                    "confidence": confidence,
                    "coords": coords
                })

    if running_people_count >= CROWD_THRESHOLD:
        alerts.append({
            "type": "CROWD_RUNNING",
            "running_count": running_people_count,
            "confidence": 1.0,
            "coords": []
        })

    if alerts:
        _, buffer = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 70])
        frame_base64 = base64.b64encode(buffer).decode('utf-8')

        payload = {
            "camera_id": camera_id,
            "alerts": alerts,
            "frame_data": frame_base64
        }

        if KAFKA_AVAILABLE:
            producer.send('security_events', payload)
            producer.flush()

    return alerts