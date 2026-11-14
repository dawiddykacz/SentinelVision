import cv2
import json
import base64
import math
import os
from collections import defaultdict
from ultralytics import YOLO
from kafka import KafkaProducer, KafkaConsumer

# Wczytujemy DWA modele
model_obj = YOLO('yolov8s.pt')  # Standardowy YOLO do broni i śledzenia tłumu
model_pose = YOLO('yolov8s-pose.pt')  # YOLO-Pose do analizy szkieletu (Tylko upadki)

KAFKA_URL = 'kafka:9092'

DANGEROUS_CLASSES = {43: "KNIFE", 34: "BASEBALL_BAT", 76: "SCISSORS"}
track_history = defaultdict(list)
HISTORY_LENGTH = 5
CROWD_THRESHOLD = 3

# Producent Kafka (wysyła alerty)
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_URL],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    KAFKA_AVAILABLE = True
    print("Połączono z Kafką (Producent).")
except Exception as e:
    KAFKA_AVAILABLE = False
    print("Błąd producenta Kafka:", e)


def detect_fall_pose(keypoints):
    # 1. Filtrujemy tylko prawidłowe punkty (model zwraca [0.0, 0.0] jeśli czegoś nie widzi)
    valid_pts = [pt for pt in keypoints if pt[0] > 0 and pt[1] > 0]
    if len(valid_pts) < 5:
        return False

    # 2. Obliczamy wirtualną ramkę dla SAMEGO szkieletu
    xs = [pt[0] for pt in valid_pts]
    ys = [pt[1] for pt in valid_pts]
    width = max(xs) - min(xs)
    height = max(ys) - min(ys)

    if height == 0:
        return False

    # Proporcja sylwetki
    ratio = width / height

    # 3. Pobieramy kordynaty głowy i bioder
    head_pts = [pt for pt in [keypoints[0], keypoints[5], keypoints[6]] if pt[1] > 0]
    hip_pts = [pt for pt in [keypoints[11], keypoints[12]] if pt[1] > 0]

    if head_pts and hip_pts:
        head_y = sum([pt[1] for pt in head_pts]) / len(head_pts)
        hip_y = sum([pt[1] for pt in hip_pts]) / len(hip_pts)
        head_x = sum([pt[0] for pt in head_pts]) / len(head_pts)
        hip_x = sum([pt[0] for pt in hip_pts]) / len(hip_pts)

        dy = abs(head_y - hip_y)
        dx = abs(head_x - hip_x)

        # REGUŁA 1: Klasyczny upadek w bok (odległość X jest większa niż Y, czyli tułów leży poziomo)
        if dy < dx:
            return True

        # REGUŁA 2: Odwrotna grawitacja (głowa jest fizycznie niżej niż miednica na ekranie)
        if head_y > (hip_y + 10):
            return True

    # REGUŁA 3: Rozwiązanie na upadek w stronę kamery (Z-axis)
    # Ciało z rozrzuconymi nogami i rękami zajmuje proporcję bliską kwadratowi.
    # Stojący człowiek ma zazwyczaj ratio poniżej 0.4.
    if ratio > 0.75:
        return True

    return False


def process_frame(frame, camera_id="CAM_01", is_video=True):
    frame_h, frame_w = frame.shape[:2]

    # 1. Zwykły model -> Przedmioty i bieganie (ze śledzeniem b-boxów)
    results_obj = model_obj.track(frame, persist=True, tracker="bytetrack.yaml",
                                  verbose=False) if is_video else model_obj(frame, verbose=False)

    # 2. Model Pose -> Wyłącznie upadki
    results_pose = model_pose(frame, verbose=False)

    alerts = []
    running_people_count = 0

    # --- A. Analiza zagrożeń z przedmiotami i bieganiem ---
    for result in results_obj:
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

            if cls_id == 0:  # Tylko obliczanie biegania
                if track_id is not None:
                    x1, y1, x2, y2 = coords
                    cx, cy = (x1 + x2) / 2, (y1 + y2) / 2
                    person_height = y2 - y1

                    track = track_history[track_id]
                    track.append((cx, cy))
                    if len(track) > HISTORY_LENGTH:
                        track.pop(0)
                    if len(track) == HISTORY_LENGTH:
                        distance = math.hypot(cx - track[0][0], cy - track[0][1])
                        if distance > max(10.0, person_height * 0.12):
                            running_people_count += 1

            elif cls_id in DANGEROUS_CLASSES:
                alerts.append({"type": "DANGEROUS_ITEM", "item": DANGEROUS_CLASSES[cls_id], "confidence": confidence,
                               "coords": coords})

    if running_people_count >= CROWD_THRESHOLD:
        alerts.append({"type": "CROWD_RUNNING", "running_count": running_people_count, "confidence": 1.0, "coords": []})

    # --- B. Analiza upadków z YOLO-Pose ---
    for result in results_pose:
        if result.keypoints is None:
            continue

        boxes_pose = result.boxes.xyxy.cpu().tolist()
        confs_pose = result.boxes.conf.cpu().tolist()
        keypoints_list = result.keypoints.xy.cpu().tolist()

        for box, conf, kpts in zip(boxes_pose, confs_pose, keypoints_list):
            if conf < 0.45:  # Tolerancja zaufania modelu do człowieka
                continue

            if detect_fall_pose(kpts):
                alerts.append({"type": "FALL_DETECTED", "confidence": conf, "coords": box})

    # --- OZNACZANIE FILMÓW (Rysowanie ramek na klatce wideo) ---
    for alert in alerts:
        if alert.get("coords"):
            x1, y1, x2, y2 = map(int, alert["coords"])
            label = f"{alert['type']} ({alert['confidence']:.2f})"
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 0, 255), 3)
            cv2.putText(frame, label, (x1, max(20, y1 - 10)), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 0, 255), 2)

    # --- Wysłanie powiadomienia do Kafki ---
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
        bootstrap_servers=[KAFKA_URL],
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