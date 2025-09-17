FROM apache/airflow:2.8.1
COPY requirements.txt /requirements.txt
# Install python packages as the airflow user (follow Airflow image guidance)
USER airflow
RUN pip install --no-cache-dir --user -r /requirements.txt
USER root
RUN chown -R airflow: /opt/airflow
USER airflow
