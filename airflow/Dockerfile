FROM apache/airflow:2.10.3-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        rsync \
        openssh-client \
        curl \
        ca-certificates \
    && curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc \
    && chmod +x /usr/local/bin/mc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY airflow/requirements.txt /requirements.txt

RUN if [ -s /requirements.txt ]; then \
      pip install --no-cache-dir -r /requirements.txt; \
    fi
