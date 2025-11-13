FROM apache/airflow:2.9.1-python3.12

USER root
RUN apt-get update && apt-get install -y build-essential

USER airflow

# Install Python dependencies via requirements.txt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

