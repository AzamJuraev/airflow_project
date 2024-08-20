FROM apache/airflow:2.10.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpython3-dev \
    libssl-dev \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ADD requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
