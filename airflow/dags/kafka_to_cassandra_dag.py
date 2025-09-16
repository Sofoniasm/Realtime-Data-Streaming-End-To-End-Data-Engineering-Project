from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'kafka_to_cassandra',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@once',
    catchup=False,
    default_args=default_args,
) as dag:
    produce = BashOperator(
        task_id='start_producer',
        bash_command='docker compose run --rm producer'
    )

    submit_spark = BashOperator(
        task_id='submit_spark_job',
        bash_command='/opt/scripts/spark_submit.sh'
    )

    produce >> submit_spark
