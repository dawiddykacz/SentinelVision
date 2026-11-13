import cv2
import os
import urllib.request
import ssl
from detector import process_frame, KAFKA_AVAILABLE

print(f"Status połączenia z Kafką (tryb Mock jeśli brak serwera): {'OK' if KAFKA_AVAILABLE else 'MOCK'}\n")

image_dir = 'images'
os.makedirs(image_dir, exist_ok=True)

urls = {
    "1_normalny_ruch.jpg": "https://ultralytics.com/images/bus.jpg",
    "2_upadek_osoby.jpg": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSaBQimi_7M7Xk-xajRUqKTTjzyPyLPqOoUXIB1KdGNP7BqrlWkYNtE7DI&s=10",
    "3_kij_baseballowy.jpg": "https://us.123rf.com/450wm/remains/remains1908/remains190802338/128531882-niebezpieczny-m%C4%99%C5%BCczyzna-z-kijem-bejsbolowym-gotowym-do-walki.jpg?ver=6",
    "4_noz_kuchenny.jpg": "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQ0IARL10g3RHrX4jzJRW8C5a3G9sAmrHPJpZ0qsJY9ZUYQaRENKE84Tjdj&s=10"
}

# Pobieranie obrazów przy pierwszym uruchomieniu, jeśli folder jest pusty
if not os.listdir(image_dir):
    print("Pobieranie obrazów testowych do folderu 'images'...")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    for filename, url in urls.items():
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, context=ctx) as response, open(os.path.join(image_dir, filename),
                                                                            'wb') as out_file:
                out_file.write(response.read())
            print(f" [V] Pobrano: {filename}")
        except Exception as e:
            print(f" [X] Błąd pobierania {filename}: {e}")
    print("\n")

# Uruchomienie analizy na pobranych obrazach
for file in os.listdir(image_dir):
    if file.endswith(('.jpg', '.png', '.jpeg')):
        path = os.path.join(image_dir, file)
        frame = cv2.imread(path)

        if frame is not None:
            print(f"--- Kamera analizuje klatkę: {file} ---")
            # ZMIANA TUTAJ:
            alerts = process_frame(frame, camera_id=f"CAM_TEST", is_video=False)

            if alerts:
                for a in alerts:
                    item = a.get('item', '')
                    print(f" [!] {a['type']} {item} | Pewność: {a['confidence']:.2f}")
                print(" -> Wysłano paczkę JSON z klatką obrazu do Kafki.\n")
            else:
                print(" [OK] Brak zagrożeń.\n")