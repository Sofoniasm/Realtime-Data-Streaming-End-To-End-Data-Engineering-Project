from datetime import datetime, timedelta
import json
import requests
from confluent_kafka import Producer

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2025, 9, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

producer = Producer({'bootstrap.servers': 'broker:29092'})

def fetch_and_produce():
    try:
        resp = requests.get('https://randomuser.me/api/', timeout=5)
        resp.raise_for_status()
    except Exception as e:
        print('API fetch failed', e)
        return
    payload = resp.json()
    user = payload.get('results', [None])[0]
    if not user:
        return

    # Minimal normalization
    data = {
        'first_name': (user.get('name') or {}).get('first'),
        'last_name': (user.get('name') or {}).get('last'),
        'email': user.get('email')
    }

    producer.produce('events', json.dumps(data).encode('utf-8'))
    producer.flush()

with DAG('api_to_kafka', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    task = PythonOperator(task_id='fetch_and_produce', python_callable=fetch_and_produce)
