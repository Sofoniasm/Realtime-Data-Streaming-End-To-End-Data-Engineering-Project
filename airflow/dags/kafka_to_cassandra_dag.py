from datetime import datetime, timedelta
import requests

# Airflow imports are optional so this file can be run standalone for testing.
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except Exception:
    AIRFLOW_AVAILABLE = False

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2025, 9, 3, 10, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def streaming_job():
    """Fetch a random user from the demo API and (placeholder) send to Kafka."""
    try:
        res = requests.get("https://randomuser.me/api/", timeout=5)
        res.raise_for_status()
    except requests.RequestException as exc:
        print(f"API request failed: {exc}")
        return

    data = res.json()

    # Extract the first result (the API returns a list under 'results')
    try:
        user = data['results'][0]
    except (KeyError, IndexError):
        print('Unexpected API response format:')
        print(data)
        return

    # Friendly fields to display
    name = user.get('name', {})
    title = name.get('title', '')
    first = name.get('first', '')
    last = name.get('last', '')
    full_name = ' '.join(p for p in [title, first, last] if p).strip()

    email = user.get('email')
    phone = user.get('phone')
    cell = user.get('cell')
    location = user.get('location', {})
    city = location.get('city')
    state = location.get('state')
    country = location.get('country')
    picture = user.get('picture', {}).get('large')

    print('--- RandomUser summary ---')
    if full_name:
        print('Name: ', full_name)
    if email:
        print('Email:', email)
    if phone:
        print('Phone:', phone)
    if cell:
        print('Cell: ', cell)
    if city or state or country:
        loc_parts = [p for p in [city, state, country] if p]
        print('Location:', ', '.join(loc_parts))
    if picture:
        print('Picture:', picture)

    # Also pretty-print the full user JSON for debugging (truncated if large)
    import json as _json
    print('\nFull user JSON:')
    print(_json.dumps(user, indent=2)[:2000])

# The DAG and task creation are kept but only created when Airflow is available
if AIRFLOW_AVAILABLE:
    with DAG(
        'kafka_to_cassandra',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False,
    ) as dag:
        streaming_task = PythonOperator(
            task_id='stream_data_from_api_to_kafka',
            python_callable=streaming_job,
        )


if __name__ == '__main__':
    # quick local test: fetch and print API data
    streaming_job()


