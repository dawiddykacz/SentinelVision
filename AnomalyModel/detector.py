import cv2
import json
import base64
from ultralytics import YOLO
from kafka import KafkaProducer

model = YOLO('yolov8s.pt')


DANGEROUS_CLASSES = {43: "KNIFE", 34: "BASEBALL_BAT", 76: "SCISSORS"}

# Inicjalizacja producenta Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    KAFKA_AVAILABLE = True
except Exception as e:
    KAFKA_AVAILABLE = False

def detect_fall(bbox):
    x1, y1, x2, y2 = bbox
    return (x2 - x1) > ((y2 - y1) * 1.2)

def process_frame(frame, camera_id="CAM_01"):
    results = model(frame, verbose=False)
    alerts = []
    
    for result in results:
        for box in result.boxes:
            cls_id = int(box.cls[0])
            coords = box.xyxy[0].tolist()
            confidence = float(box.conf[0])

            #print(f" [DEBUG] Sieć widzi klasę nr {cls_id} z pewnością {confidence:.2f}")
            if confidence < 0.4:
                continue

            if cls_id == 0 and detect_fall(coords):
                alerts.append({"type": "FALL_DETECTED", "confidence": confidence, "coords": coords})
            
            # 2. Wykrywanie niebezpiecznego przedmiotu (broń/nóż itp.)
            elif cls_id in DANGEROUS_CLASSES:
                item_name = DANGEROUS_CLASSES[cls_id]
                alerts.append({
                    "type": "DANGEROUS_ITEM_DETECTED", 
                    "item": item_name, 
                    "confidence": confidence, 
                    "coords": coords
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
