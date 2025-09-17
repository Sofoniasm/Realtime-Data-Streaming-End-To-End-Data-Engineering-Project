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


def get_data():
    """Fetch a single user object from RandomUser API and return the raw user dict.

    Returns None on error.
    """
    try:
        resp = requests.get("https://randomuser.me/api/", timeout=5)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"API request failed: {exc}")
        return None

    payload = resp.json()
    try:
        return payload['results'][0]
    except (KeyError, IndexError, TypeError):
        print('Unexpected API response format:')
        print(payload)
        return None


def format_data(res):
    """Normalize the RandomUser result into a flat dictionary for downstream use."""
    if not isinstance(res, dict):
        return None

    loc = res.get('location', {}) or {}
    street = loc.get('street', {}) or {}

    data = {}
    data['first_name'] = (res.get('name') or {}).get('first')
    data['last_name'] = (res.get('name') or {}).get('last')
    data['gender'] = res.get('gender')
    # Address: number + street name, city, state, country
    addr_parts = []
    num = street.get('number')
    name = street.get('name')
    if num and name:
        addr_parts.append(f"{num} {name}")
    elif name:
        addr_parts.append(name)
    city = loc.get('city')
    state = loc.get('state')
    country = loc.get('country')
    for p in (city, state, country):
        if p:
            addr_parts.append(p)
    data['address'] = ', '.join(addr_parts) if addr_parts else None
    data['postcode'] = loc.get('postcode')
    data['email'] = res.get('email')
    data['username'] = (res.get('login') or {}).get('username')
    data['dob'] = (res.get('dob') or {}).get('date')
    data['registered_date'] = (res.get('registered') or {}).get('date')
    data['phone'] = res.get('phone')
    data['picture'] = (res.get('picture') or {}).get('medium')

    return data


def stream_data():
    """High-level function: fetch + format + print the formatted payload."""
    raw = get_data()
    if raw is None:
        print('No data fetched.')
        return

    formatted = format_data(raw)
    if formatted is None:
        print('Failed to format data.')
        return

    # Pretty-print the normalized data
    import json as _json
    print(_json.dumps(formatted, indent=2))

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
            python_callable=stream_data,
        )


if __name__ == '__main__':
    # quick local test: fetch and print API data
    stream_data()


