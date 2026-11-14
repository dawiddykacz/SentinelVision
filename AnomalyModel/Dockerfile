FROM python:3.10-slim
WORKDIR /app

RUN apt-get update && apt-get install -y libgl1 libglib2.0-0 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN python -c "from ultralytics import YOLO; YOLO('yolov8s.pt'); YOLO('yolov8s-pose.pt')"

CMD ["python", "detector.py"]