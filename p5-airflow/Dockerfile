FROM apache/airflow:2.2.0-python3.9

ARG AIRFLOW_HOME=/opt/airflow

COPY requirements.txt .
COPY config/webserver_config.py ${AIRFLOW_HOME}/webserver_config.py

RUN pip install -r requirements.txt
