import json
import time
import random
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

p = Producer({'bootstrap.servers': 'kafka:9092'})

def gen_event():
    return {
        'user_id': random.randint(1, 1000),
        'username': fake.user_name(),
        'action': random.choice(['click', 'view', 'purchase']),
        'value': random.random() * 100,
        'timestamp': int(time.time() * 1000)
    }

if __name__ == '__main__':
    topic = 'events'
    while True:
        ev = gen_event()
        p.produce(topic, json.dumps(ev).encode('utf-8'), callback=delivery_report)
        p.poll(0)
        time.sleep(1)
