import socket
import time
import subprocess

HOST = 'kafka'
PORT = 9092

print('Waiting for Kafka to be available...')
while True:
    try:
        with socket.create_connection((HOST, PORT), timeout=2):
            print('Kafka is up, starting producer...')
            break
    except Exception:
        time.sleep(1)

# start the producer
subprocess.run(['python', 'producer.py'])
