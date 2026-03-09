FROM apache/airflow:2.9.2-python3.11

USER airflow

WORKDIR /opt/airflow

# Install project dependencies for DAG code (Gemini client, boto3, etc.)
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

