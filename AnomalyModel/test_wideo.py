import cv2
from detector import process_frame, KAFKA_AVAILABLE

print(f"Status połączenia z Kafką: {'OK' if KAFKA_AVAILABLE else 'MOCK'}\n")


VIDEO_PATH = "videos/running_1.mp4"

cap = cv2.VideoCapture(VIDEO_PATH)

if not cap.isOpened():
    print(f"Błąd: Nie można otworzyć pliku wideo {VIDEO_PATH}")
    exit()

print(f"Rozpoczęto analizę wideo: {VIDEO_PATH}")
frame_count = 0

while cap.isOpened():
    ret, frame = cap.read()
    if not ret:
        print("Koniec wideo.")
        break

    frame_count += 1

    # Dla przyspieszenia testów, jeśli wideo jest duże, można je zmniejszyć:
    # frame = cv2.resize(frame, (640, 480))

    alerts = process_frame(frame, camera_id="CAM_VIDEO_01")

    if alerts:
        for a in alerts:
            if a['type'] == 'CROWD_RUNNING':
                print(
                    f"[KLATKA {frame_count}] [!!!] ALERT: BIEGNĄCY TŁUM! Wykryto {a['running_count']} biegnących osób.")
            elif a['type'] == 'FALL_DETECTED':
                print(f"[KLATKA {frame_count}] [!] ALERT: Wykryto upadek.")
            elif a['type'] == 'DANGEROUS_ITEM_DETECTED':
                print(f"[KLATKA {frame_count}] [!] ALERT: Broń/Przedmiot ({a['item']}).")

cap.release()