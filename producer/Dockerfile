FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY producer.py ./
COPY wait_for_kafka.py ./
CMD ["python", "wait_for_kafka.py"]
